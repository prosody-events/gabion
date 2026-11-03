//! How `gabiond` exits, run as the built binary: a listener that cannot
//! start or a bad configuration stops it at once with status 1 and one line
//! that says why, and SIGTERM drains it to status 0.

use std::io::{BufRead, BufReader, Read};
use std::net::{SocketAddr, TcpListener, UdpSocket};
use std::process::{Command, Stdio};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::time::{Duration, Instant};

/// Longer than any healthy start or drain takes; a gabiond still running
/// then has ignored its failure or its signal.
const DEADLINE: Duration = Duration::from_secs(20);

/// Run gabiond with `env` until it exits, sending it SIGTERM once a log line
/// contains `term_after`, and return its status code, its log (stdout) and
/// its stderr.
fn run_gabiond(env: &[(&str, String)], term_after: Option<&str>) -> (Option<i32>, String, String) {
    let mut child = Command::new(env!("CARGO_BIN_EXE_gabiond"))
        .env_clear()
        .env("NO_COLOR", "1")
        .env("RUST_LOG", "info")
        .envs(env.iter().map(|(key, value)| (key, value)))
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("gabiond starts");
    let stdout = child.stdout.take().expect("piped stdout");
    let (line_tx, line_rx) = mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            line_tx.send(line).ok();
        }
    });
    // Stdout closes, and the channel with it, when gabiond exits.
    let deadline = Instant::now() + DEADLINE;
    let mut log = String::new();
    loop {
        match line_rx.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
            Ok(line) => {
                if term_after.is_some_and(|marker| line.contains(marker)) {
                    Command::new("kill")
                        .args(["-TERM", &child.id().to_string()])
                        .status()
                        .expect("kill runs");
                }
                log.push_str(&line);
                log.push('\n');
            }
            Err(RecvTimeoutError::Disconnected) => break,
            Err(RecvTimeoutError::Timeout) => {
                child.kill().ok();
                panic!(
                    "gabiond was still running {DEADLINE:?} after it should have exited, \
                     log:\n{log}"
                );
            }
        }
    }
    let status = child.wait().expect("gabiond exits");
    let mut stderr = String::new();
    child
        .stderr
        .take()
        .expect("piped stderr")
        .read_to_string(&mut stderr)
        .expect("stderr reads");
    (status.code(), log, stderr)
}

/// A static peer to gossip to, so gabiond never asks Kubernetes.
fn peer() -> (UdpSocket, String) {
    let socket = UdpSocket::bind("127.0.0.1:0").expect("peer binds");
    let addr = socket.local_addr().expect("peer address").to_string();
    (socket, addr)
}

/// gabiond exited 1 with exactly one error line, coded `code`, no
/// clean-shutdown claim and nothing on stderr; returns that line.
fn failure_line((status, log, stderr): (Option<i32>, String, String), code: &str) -> String {
    assert_eq!(status, Some(1), "log:\n{log}");
    let errors: Vec<&str> = log
        .lines()
        .filter(|line| line.contains(" ERROR "))
        .collect();
    assert_eq!(errors.len(), 1, "exactly one error line, log:\n{log}");
    assert!(
        errors[0].contains(&format!("code=\"{code}\"")),
        "the error line is coded {code}: {}",
        errors[0],
    );
    assert!(!log.contains("Gabion shut down cleanly."), "log:\n{log}");
    assert!(stderr.is_empty(), "stderr: {stderr}");
    errors[0].to_owned()
}

/// The line the gRPC server logs once it serves.
const GRPC_SERVING: &str = "Accepting rate-limit decisions from Envoy.";
/// The line the admin server logs once it serves.
const ADMIN_SERVING: &str = "Admin HTTP endpoint is up.";

/// gabiond exited 1 with one `listener_failed` line naming `config_key` and
/// `addr` and giving the OS cause once, and logged none of `never_serving`:
/// it binds every address before claiming to serve any.
fn assert_listener_failed(
    exit: (Option<i32>, String, String),
    config_key: &str,
    addr: SocketAddr,
    never_serving: &[&str],
) {
    for serving in never_serving {
        assert!(
            !exit.1.contains(serving),
            "claimed `{serving}`, log:\n{}",
            exit.1
        );
    }
    let line = failure_line(exit, "listener_failed");
    assert!(
        line.contains(&format!("config_key=\"{config_key}\""))
            && line.contains(&format!("addr={addr}")),
        "the line names the setting and the address: {line}",
    );
    assert_eq!(
        line.matches("os error").count(),
        1,
        "the OS cause appears once: {line}"
    );
}

#[test]
fn listener_failure_exits_with_status_one() {
    let taken = TcpListener::bind("127.0.0.1:0").expect("port binds");
    let addr = taken.local_addr().expect("taken address");
    let (_peer, peer) = peer();
    let exit = run_gabiond(
        &[
            ("GABION_ENVOY_BIND", addr.to_string()),
            ("GABION_GOSSIP_BIND", "127.0.0.1:0".into()),
            ("GABION_GOSSIP_BOOTSTRAP_PEERS", peer),
        ],
        None,
    );
    assert_listener_failed(exit, "envoy_bind", addr, &[GRPC_SERVING]);
}

#[test]
fn admin_listener_failure_exits_with_status_one() {
    let taken = TcpListener::bind("127.0.0.1:0").expect("port binds");
    let addr = taken.local_addr().expect("taken address");
    let (_peer, peer) = peer();
    let exit = run_gabiond(
        &[
            ("GABION_ADMIN_BIND", addr.to_string()),
            ("GABION_ENVOY_BIND", "127.0.0.1:0".into()),
            ("GABION_GOSSIP_BIND", "127.0.0.1:0".into()),
            ("GABION_GOSSIP_BOOTSTRAP_PEERS", peer),
        ],
        None,
    );
    assert_listener_failed(exit, "admin_bind", addr, &[ADMIN_SERVING, GRPC_SERVING]);
}

#[test]
fn gossip_bind_failure_exits_with_status_one() {
    let taken = UdpSocket::bind("127.0.0.1:0").expect("port binds");
    let addr = taken.local_addr().expect("taken address");
    let (_peer, peer) = peer();
    let exit = run_gabiond(
        &[
            ("GABION_GOSSIP_BIND", addr.to_string()),
            ("GABION_GOSSIP_BOOTSTRAP_PEERS", peer),
        ],
        None,
    );
    assert_listener_failed(exit, "gossip.bind", addr, &[]);
}

#[test]
fn bad_configuration_exits_with_status_one() {
    let exit = run_gabiond(&[("GABION_GOSSIP_BIND", "not-an-address".into())], None);
    let line = failure_line(exit, "gabiond_exit");
    assert!(
        line.contains("stage=\"load its configuration\""),
        "the line names the failed stage: {line}",
    );
}

#[test]
fn sigterm_drains_and_exits_with_status_zero() {
    let (_peer, peer) = peer();
    let (status, log, stderr) = run_gabiond(
        &[
            ("GABION_ENVOY_BIND", "127.0.0.1:0".into()),
            ("GABION_ADMIN_BIND", "127.0.0.1:0".into()),
            ("GABION_GOSSIP_BIND", "127.0.0.1:0".into()),
            ("GABION_GOSSIP_BOOTSTRAP_PEERS", peer),
        ],
        // Logged after the signal handlers are installed, so the signal is
        // never lost.
        Some("Listening for gossip from other gabion nodes."),
    );
    assert_eq!(status, Some(0), "log:\n{log}");
    assert_eq!(
        log.matches("Gabion shut down cleanly.").count(),
        1,
        "one clean-shutdown line, log:\n{log}"
    );
    assert!(!log.contains(" ERROR "), "no error line, log:\n{log}");
    assert!(stderr.is_empty(), "stderr: {stderr}");
}
