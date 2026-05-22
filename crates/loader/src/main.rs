use std::env;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use envoy_types::pb::envoy::extensions::common::ratelimit::v3::{
    RateLimitDescriptor, rate_limit_descriptor,
};
use envoy_types::pb::envoy::service::ratelimit::v3::{
    RateLimitRequest, rate_limit_response, rate_limit_service_client::RateLimitServiceClient,
};
use futures::future::join_all;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{Instant, sleep, sleep_until, timeout};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Backend {
    Http,
    Grpc,
}

#[derive(Debug)]
struct Config {
    backend: Backend,
    target_url: String,
    grpc_addr: String,
    domain: String,
    tenants: usize,
    budget_per_tenant: u64,
    rps_per_tenant: u64,
    duration_s: u64,
    warmup_s: u64,
    window_ms: u64,
    align_window: bool,
    descriptor_key: String,
    request_timeout_ms: u64,
}

#[derive(Clone, Debug)]
struct HttpTarget {
    host: String,
    port: u16,
    path_prefix: String,
    path_suffix: String,
    authority: String,
}

#[derive(Debug)]
struct Counters {
    ok: AtomicU64,
    over: AtomicU64,
    fail: AtomicU64,
}

impl Counters {
    fn new() -> Self {
        Self {
            ok: AtomicU64::new(0),
            over: AtomicU64::new(0),
            fail: AtomicU64::new(0),
        }
    }

    fn add(&self, outcome: Outcome) {
        match outcome {
            Outcome::Ok => &self.ok,
            Outcome::Over => &self.over,
            Outcome::Fail => &self.fail,
        }
        .fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> (u64, u64, u64) {
        (
            self.ok.load(Ordering::Relaxed),
            self.over.load(Ordering::Relaxed),
            self.fail.load(Ordering::Relaxed),
        )
    }
}

#[derive(Clone, Copy, Debug)]
enum Outcome {
    Ok,
    Over,
    Fail,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cfg = Config::from_env()?;
    let windows = cfg.duration_s.saturating_mul(1_000).div_ceil(cfg.window_ms);
    if windows == 0 {
        bail!("DURATION_S must cover at least one window");
    }

    if cfg.warmup_s > 0 {
        sleep(Duration::from_secs(cfg.warmup_s)).await;
    }
    if cfg.align_window {
        sleep_until_next_window(cfg.window_ms).await?;
    }

    let started_wall_ms = wall_millis()?;
    let started = Instant::now();
    let duration = Duration::from_millis(windows * cfg.window_ms);
    let finished = started + duration;
    let counters: Arc<Vec<Counters>> = Arc::new((0..windows).map(|_| Counters::new()).collect());
    let cfg = Arc::new(cfg);

    let tasks = (0..cfg.tenants)
        .map(|tenant| {
            let cfg = Arc::clone(&cfg);
            let counters = Arc::clone(&counters);
            tokio::spawn(async move { run_tenant(cfg, tenant, started, finished, counters).await })
        })
        .collect::<Vec<_>>();

    let mut task_failures = 0_u64;
    for result in join_all(tasks).await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                task_failures += 1;
                eprintln!("tenant task failed: {err:#}");
            }
            Err(err) => {
                task_failures += 1;
                eprintln!("tenant task panicked: {err:#}");
            }
        }
    }

    print_summary(&cfg, started_wall_ms, windows, &counters, task_failures);
    Ok(())
}

async fn run_tenant(
    cfg: Arc<Config>,
    tenant: usize,
    started: Instant,
    finished: Instant,
    counters: Arc<Vec<Counters>>,
) -> Result<()> {
    let interval = Duration::from_nanos(1_000_000_000_u64 / cfg.rps_per_tenant.max(1));
    let mut next = started;
    let mut grpc = if cfg.backend == Backend::Grpc {
        Some(RateLimitServiceClient::connect(format!("http://{}", cfg.grpc_addr)).await?)
    } else {
        None
    };
    let http = if cfg.backend == Backend::Http {
        Some(parse_http_target(&cfg.target_url)?)
    } else {
        None
    };

    while next < finished {
        sleep_until(next).await;
        let elapsed = next.saturating_duration_since(started);
        let window = (elapsed.as_millis() / u128::from(cfg.window_ms)) as usize;
        if let Some(counter) = counters.get(window) {
            let request_timeout = Duration::from_millis(cfg.request_timeout_ms);
            let outcome = match cfg.backend {
                Backend::Http => {
                    timeout(request_timeout, call_http(http.as_ref().unwrap(), tenant))
                        .await
                        .unwrap_or(Outcome::Fail)
                }
                Backend::Grpc => timeout(
                    request_timeout,
                    call_grpc(
                        grpc.as_mut().unwrap(),
                        &cfg.domain,
                        &cfg.descriptor_key,
                        tenant,
                    ),
                )
                .await
                .unwrap_or(Outcome::Fail),
            };
            counter.add(outcome);
        }
        next += interval;
    }
    Ok(())
}

async fn call_http(target: &HttpTarget, tenant: usize) -> Outcome {
    call_http_inner(target, tenant)
        .await
        .unwrap_or(Outcome::Fail)
}

async fn call_http_inner(target: &HttpTarget, tenant: usize) -> Result<Outcome> {
    let path = format!("{}{}{}", target.path_prefix, tenant, target.path_suffix);
    let request = format!(
        "GET {path} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
        target.authority
    );
    let mut stream = TcpStream::connect((target.host.as_str(), target.port)).await?;
    stream.write_all(request.as_bytes()).await?;
    let mut buf = [0_u8; 128];
    let n = stream.read(&mut buf).await?;
    let status = std::str::from_utf8(&buf[..n]).unwrap_or_default();
    Ok(
        if status.starts_with("HTTP/1.1 429") || status.starts_with("HTTP/1.0 429") {
            Outcome::Over
        } else if status.starts_with("HTTP/1.1 2") || status.starts_with("HTTP/1.0 2") {
            Outcome::Ok
        } else {
            Outcome::Fail
        },
    )
}

async fn call_grpc(
    client: &mut RateLimitServiceClient<tonic::transport::Channel>,
    domain: &str,
    descriptor_key: &str,
    tenant: usize,
) -> Outcome {
    let request = RateLimitRequest {
        domain: domain.to_string(),
        descriptors: vec![RateLimitDescriptor {
            entries: vec![rate_limit_descriptor::Entry {
                key: descriptor_key.to_string(),
                value: tenant.to_string(),
            }],
            limit: None,
            hits_addend: None,
        }],
        hits_addend: 1,
    };
    let Ok(response) = client.should_rate_limit(request).await else {
        return Outcome::Fail;
    };
    match rate_limit_response::Code::try_from(response.into_inner().overall_code) {
        Ok(rate_limit_response::Code::Ok) => Outcome::Ok,
        Ok(rate_limit_response::Code::OverLimit) => Outcome::Over,
        _ => Outcome::Fail,
    }
}

fn parse_http_target(url: &str) -> Result<HttpTarget> {
    let rest = url
        .strip_prefix("http://")
        .context("TARGET_URL must start with http://")?;
    let (authority, path) = rest.split_once('/').unwrap_or((rest, ""));
    let (host, port) = match authority.rsplit_once(':') {
        Some((host, port)) => (host.to_string(), port.parse::<u16>()?),
        None => (authority.to_string(), 80),
    };
    let path = format!("/{path}");
    let marker = "{tenant}";
    let (path_prefix, path_suffix) = if let Some((prefix, suffix)) = path.split_once(marker) {
        (prefix.to_string(), suffix.to_string())
    } else if let Some((prefix, suffix)) = path.split_once("TENANT") {
        (prefix.to_string(), suffix.to_string())
    } else if let Some((prefix, suffix)) = path.split_once("tenant=") {
        (format!("{prefix}tenant="), suffix.to_string())
    } else {
        (path, String::new())
    };
    Ok(HttpTarget {
        host,
        port,
        path_prefix,
        path_suffix,
        authority: authority.to_string(),
    })
}

async fn sleep_until_next_window(window_ms: u64) -> Result<()> {
    let now = wall_millis()?;
    let next = ((now / window_ms) + 1) * window_ms;
    sleep(Duration::from_millis(next.saturating_sub(now))).await;
    Ok(())
}

fn wall_millis() -> Result<u64> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_millis()
        .try_into()?)
}

fn print_summary(
    cfg: &Config,
    started_wall_ms: u64,
    windows: u64,
    counters: &[Counters],
    task_failures: u64,
) {
    let mut total_ok = 0_u64;
    let mut total_over = 0_u64;
    let mut total_fail = task_failures;
    println!();
    println!("=== gabion rust loader summary ===");
    println!("  backend                    {:?}", cfg.backend);
    println!("  tenants                    {}", cfg.tenants);
    println!("  budget per tenant/window   {}", cfg.budget_per_tenant);
    println!("  rps per tenant             {}", cfg.rps_per_tenant);
    println!("  window ms                  {}", cfg.window_ms);
    println!("  windows                    {}", windows);
    println!("  started wall ms            {}", started_wall_ms);
    println!();
    println!("  per-window:");
    for (idx, counter) in counters.iter().enumerate() {
        let (ok, over, fail) = counter.snapshot();
        total_ok += ok;
        total_over += over;
        total_fail += fail;
        println!(
            "    window={idx} ok={ok} over={over} fail={fail} expected_ok={}",
            cfg.tenants as u64 * cfg.budget_per_tenant
        );
    }
    println!();
    println!(
        "  actual attempts            {}",
        total_ok + total_over + total_fail
    );
    println!(
        "  expected allowed           {}",
        cfg.tenants as u64 * cfg.budget_per_tenant * windows
    );
    println!("  actual allowed             {total_ok}");
    println!("  actual rejected            {total_over}");
    println!("  failed calls               {total_fail}");
    println!("---LOADER-SUMMARY-END---");
}

impl Config {
    fn from_env() -> Result<Self> {
        let backend = match env_value("BACKEND", "http").as_str() {
            "http" | "nginx" => Backend::Http,
            "grpc" | "gabiond" => Backend::Grpc,
            other => bail!("unsupported BACKEND={other}; expected http or grpc"),
        };
        Ok(Self {
            backend,
            target_url: env_value("TARGET_URL", "http://gabion-nginx:8080/tenant/index.html"),
            grpc_addr: env_value("GRPC_ADDR", "gabiond:8081"),
            domain: env_value("DOMAIN", "nginx"),
            tenants: parse_env("TENANTS", 20)?,
            budget_per_tenant: parse_env("BUDGET_PER_TENANT", 20)?,
            rps_per_tenant: parse_env("RPS_PER_TENANT", 100)?,
            duration_s: parse_env("DURATION_S", 10)?,
            warmup_s: parse_env("WARMUP_S", 0)?,
            window_ms: parse_env("WINDOW_MS", 1_000)?,
            align_window: parse_bool_env("ALIGN_WINDOW", true)?,
            descriptor_key: env_value("DESCRIPTOR_KEY", "tenant"),
            request_timeout_ms: parse_env("REQUEST_TIMEOUT_MS", 2_000)?,
        })
        .and_then(|cfg| {
            if cfg.tenants == 0 {
                bail!("TENANTS must be greater than zero");
            }
            if cfg.rps_per_tenant == 0 {
                bail!("RPS_PER_TENANT must be greater than zero");
            }
            if cfg.window_ms == 0 {
                bail!("WINDOW_MS must be greater than zero");
            }
            Ok(cfg)
        })
    }
}

fn env_value(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

fn parse_env<T>(key: &str, default: T) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    match env::var(key) {
        Ok(value) => Ok(value.parse()?),
        Err(_) => Ok(default),
    }
}

fn parse_bool_env(key: &str, default: bool) -> Result<bool> {
    match env::var(key) {
        Ok(value) => match value.as_str() {
            "1" | "true" | "TRUE" | "yes" | "YES" => Ok(true),
            "0" | "false" | "FALSE" | "no" | "NO" => Ok(false),
            _ => bail!("{key} must be boolean"),
        },
        Err(_) => Ok(default),
    }
}
