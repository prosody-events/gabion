//! How `gabiond` exits, run as the built binary: a listener that cannot
//! start stops it at once with status 1 and one line that says why.

use std::io::Read;
use std::net::{SocketAddr, TcpListener, UdpSocket};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;

/// Longer than any healthy start takes; a gabiond still running then has
/// ignored its failed listener.
const DEADLINE: Duration = Duration::from_secs(20);

/// Run gabiond with `env` until it exits, and return its status code, its
/// log (stdout) and its stderr.
fn run_gabiond(env: &[(&str, String)]) -> (Option<i32>, String, String) {
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
    let mut stdout = child.stdout.take().expect("piped stdout");
    let (log_tx, log_rx) = mpsc::channel();
    std::thread::spawn(move || {
        let mut log = String::new();
        let read = stdout.read_to_string(&mut log);
        log_tx.send(read.map(|_| log)).ok();
    });
    // Stdout closes when gabiond exits.
    let Ok(log) = log_rx.recv_timeout(DEADLINE) else {
        child.kill().ok();
        panic!("gabiond was still running {DEADLINE:?} after its listener failed to start");
    };
    let status = child.wait().expect("gabiond exits");
    let mut stderr = String::new();
    (child.stderr.take().expect("piped stderr"))
        .read_to_string(&mut stderr)
        .expect("stderr reads");
    (status.code(), log.expect("stdout reads"), stderr)
}

/// A static peer to gossip to, so gabiond never asks Kubernetes.
fn peer() -> (UdpSocket, String) {
    let socket = UdpSocket::bind("127.0.0.1:0").expect("peer binds");
    let addr = socket.local_addr().expect("peer address").to_string();
    (socket, addr)
}

/// gabiond exited 1 with one `listener_failed` line naming `config_key` and
/// `addr`, no other error line, no clean-shutdown claim and nothing on stderr.
fn assert_listener_failed(
    (code, log, stderr): (Option<i32>, String, String),
    config_key: &str,
    addr: SocketAddr,
) {
    assert_eq!(code, Some(1), "log:\n{log}");
    let failed: Vec<&str> = log
        .lines()
        .filter(|line| line.contains("code=\"listener_failed\""))
        .collect();
    assert_eq!(failed.len(), 1, "one listener_failed line, log:\n{log}");
    assert!(
        failed[0].contains(&format!("config_key=\"{config_key}\""))
            && failed[0].contains(&format!("addr={addr}")),
        "the line names the setting and the address: {}",
        failed[0],
    );
    assert_eq!(
        log.lines().filter(|line| line.contains(" ERROR ")).count(),
        1,
        "the failure is the only error line, log:\n{log}"
    );
    assert!(!log.contains("Gabion shut down cleanly."), "log:\n{log}");
    assert!(stderr.is_empty(), "stderr: {stderr}");
}

#[test]
fn listener_failure_exits_with_status_one() {
    let taken = TcpListener::bind("127.0.0.1:0").expect("port binds");
    let addr = taken.local_addr().expect("taken address");
    let (_peer, peer) = peer();
    let exit = run_gabiond(&[
        ("GABION_ENVOY_BIND", addr.to_string()),
        ("GABION_GOSSIP_BIND", "127.0.0.1:0".into()),
        ("GABION_GOSSIP_BOOTSTRAP_PEERS", peer),
    ]);
    assert_listener_failed(exit, "envoy_bind", addr);
}

#[test]
fn admin_listener_failure_exits_with_status_one() {
    let taken = TcpListener::bind("127.0.0.1:0").expect("port binds");
    let addr = taken.local_addr().expect("taken address");
    let (_peer, peer) = peer();
    let exit = run_gabiond(&[
        ("GABION_ADMIN_BIND", addr.to_string()),
        ("GABION_GOSSIP_BIND", "127.0.0.1:0".into()),
        ("GABION_GOSSIP_BOOTSTRAP_PEERS", peer),
    ]);
    assert_listener_failed(exit, "admin_bind", addr);
}

#[test]
fn gossip_bind_failure_exits_with_status_one() {
    let taken = UdpSocket::bind("127.0.0.1:0").expect("port binds");
    let addr = taken.local_addr().expect("taken address");
    let (_peer, peer) = peer();
    let exit = run_gabiond(&[
        ("GABION_GOSSIP_BIND", addr.to_string()),
        ("GABION_GOSSIP_BOOTSTRAP_PEERS", peer),
    ]);
    assert_listener_failed(exit, "gossip.bind", addr);
}
