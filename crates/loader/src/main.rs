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
use tokio::sync::mpsc;
use tokio::time::{Instant, sleep, sleep_until, timeout};

#[cfg(test)]
mod tests;

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
    http_connections: usize,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Outcome {
    Ok,
    Over,
    Fail,
}

#[derive(Clone, Copy, Debug)]
struct ScheduledRequest {
    tenant: usize,
    window: usize,
    scheduled_at: Instant,
}

#[derive(Debug)]
struct ExpectedLoad {
    attempts: u64,
    allowed: u64,
    allowed_per_window: Vec<u64>,
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

    let task_failures = match cfg.backend {
        Backend::Http => {
            run_http_load(Arc::clone(&cfg), started, finished, Arc::clone(&counters)).await
        }
        Backend::Grpc => {
            run_grpc_load(Arc::clone(&cfg), started, finished, Arc::clone(&counters)).await
        }
    };

    let expected = expected_load(&cfg, windows);
    print_summary(
        &cfg,
        started_wall_ms,
        windows,
        &expected,
        &counters,
        task_failures,
    );
    Ok(())
}

async fn run_http_load(
    cfg: Arc<Config>,
    started: Instant,
    finished: Instant,
    counters: Arc<Vec<Counters>>,
) -> u64 {
    let target = match parse_http_target(&cfg.target_url) {
        Ok(target) => Arc::new(target),
        Err(error) => {
            eprintln!("could not parse TARGET_URL: {error:#}");
            return cfg.tenants as u64;
        }
    };
    let pool_size = cfg.http_connections.min(cfg.tenants).max(1);
    let mut senders = Vec::with_capacity(pool_size);
    let mut workers = Vec::with_capacity(pool_size);
    for _ in 0..pool_size {
        let (tx, rx) = mpsc::channel::<ScheduledRequest>(cfg.tenants.max(1024));
        senders.push(tx);
        workers.push(tokio::spawn(run_http_worker(
            Arc::clone(&cfg),
            Arc::clone(&target),
            Arc::clone(&counters),
            rx,
        )));
    }

    let tasks = (0..cfg.tenants)
        .map(|tenant| {
            let cfg = Arc::clone(&cfg);
            let sender = senders[tenant % pool_size].clone();
            tokio::spawn(
                async move { schedule_tenant(cfg, tenant, started, finished, sender).await },
            )
        })
        .collect::<Vec<_>>();

    let mut task_failures = join_task_results(tasks).await;
    drop(senders);
    for worker in workers {
        if let Err(error) = worker.await {
            task_failures += 1;
            eprintln!("http worker panicked: {error:#}");
        }
    }
    task_failures
}

async fn run_grpc_load(
    cfg: Arc<Config>,
    started: Instant,
    finished: Instant,
    counters: Arc<Vec<Counters>>,
) -> u64 {
    let tasks = (0..cfg.tenants)
        .map(|tenant| {
            let cfg = Arc::clone(&cfg);
            let counters = Arc::clone(&counters);
            tokio::spawn(
                async move { run_grpc_tenant(cfg, tenant, started, finished, counters).await },
            )
        })
        .collect::<Vec<_>>();

    join_task_results(tasks).await
}

async fn join_task_results(tasks: Vec<tokio::task::JoinHandle<Result<()>>>) -> u64 {
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
    task_failures
}

async fn schedule_tenant(
    cfg: Arc<Config>,
    tenant: usize,
    started: Instant,
    finished: Instant,
    sender: mpsc::Sender<ScheduledRequest>,
) -> Result<()> {
    let schedule = TenantSchedule::new(&cfg, tenant, started, finished);
    for request in schedule {
        sleep_until(request.scheduled_at).await;
        if sender.send(request).await.is_err() {
            break;
        }
    }
    Ok(())
}

async fn run_grpc_tenant(
    cfg: Arc<Config>,
    tenant: usize,
    started: Instant,
    finished: Instant,
    counters: Arc<Vec<Counters>>,
) -> Result<()> {
    let mut grpc = RateLimitServiceClient::connect(format!("http://{}", cfg.grpc_addr)).await?;
    let request_timeout = Duration::from_millis(cfg.request_timeout_ms);
    let schedule = TenantSchedule::new(&cfg, tenant, started, finished);
    for request in schedule {
        sleep_until(request.scheduled_at).await;
        if let Some(counter) = counters.get(request.window) {
            let outcome = timeout(
                request_timeout,
                call_grpc(&mut grpc, &cfg.domain, &cfg.descriptor_key, tenant),
            )
            .await
            .unwrap_or(Outcome::Fail);
            counter.add(outcome);
        }
    }
    Ok(())
}

async fn run_http_worker(
    cfg: Arc<Config>,
    target: Arc<HttpTarget>,
    counters: Arc<Vec<Counters>>,
    mut rx: mpsc::Receiver<ScheduledRequest>,
) {
    let mut stream = None;
    let mut read_buf = Vec::with_capacity(4096);
    while let Some(request) = rx.recv().await {
        let Some(counter) = counters.get(request.window) else {
            continue;
        };
        let deadline = request.scheduled_at + Duration::from_millis(cfg.request_timeout_ms);
        let now = Instant::now();
        if now >= deadline {
            counter.add(Outcome::Fail);
            continue;
        }
        let outcome = timeout(
            deadline.saturating_duration_since(now),
            call_http(&target, request.tenant, &mut stream, &mut read_buf),
        )
        .await
        .unwrap_or_else(|_| {
            stream = None;
            Outcome::Fail
        });
        counter.add(outcome);
    }
}

async fn call_http(
    target: &HttpTarget,
    tenant: usize,
    stream: &mut Option<TcpStream>,
    read_buf: &mut Vec<u8>,
) -> Outcome {
    let had_stream = stream.is_some();
    match call_http_inner(target, tenant, stream, read_buf).await {
        Ok(outcome) => outcome,
        Err(_) if had_stream => {
            *stream = None;
            call_http_inner(target, tenant, stream, read_buf)
                .await
                .unwrap_or(Outcome::Fail)
        }
        Err(_) => {
            *stream = None;
            Outcome::Fail
        }
    }
}

async fn call_http_inner(
    target: &HttpTarget,
    tenant: usize,
    stream: &mut Option<TcpStream>,
    read_buf: &mut Vec<u8>,
) -> Result<Outcome> {
    if stream.is_none() {
        *stream = Some(TcpStream::connect((target.host.as_str(), target.port)).await?);
    }
    let path = format!("{}{}{}", target.path_prefix, tenant, target.path_suffix);
    let request = format!(
        "GET {path} HTTP/1.1\r\nHost: {}\r\nConnection: keep-alive\r\n\r\n",
        target.authority
    );
    let conn = stream.as_mut().expect("stream just connected");
    conn.write_all(request.as_bytes()).await?;
    let response = read_http_response(conn, read_buf).await?;
    if !response.reusable {
        *stream = None;
    }
    Ok(response.outcome)
}

#[derive(Clone, Copy, Debug)]
struct HttpResponse {
    outcome: Outcome,
    reusable: bool,
}

async fn read_http_response(stream: &mut TcpStream, buf: &mut Vec<u8>) -> Result<HttpResponse> {
    buf.clear();
    let header_end = loop {
        if let Some(end) = find_header_end(buf) {
            break end;
        }
        let mut chunk = [0_u8; 1024];
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            bail!("connection closed before HTTP response headers");
        }
        buf.extend_from_slice(&chunk[..n]);
        if buf.len() > 16 * 1024 {
            bail!("HTTP response headers exceeded 16KiB");
        }
    };
    let headers = std::str::from_utf8(&buf[..header_end]).unwrap_or_default();
    let first_line = headers.lines().next().unwrap_or_default();
    let outcome =
        if first_line.starts_with("HTTP/1.1 429") || first_line.starts_with("HTTP/1.0 429") {
            Outcome::Over
        } else if first_line.starts_with("HTTP/1.1 2") || first_line.starts_with("HTTP/1.0 2") {
            Outcome::Ok
        } else {
            Outcome::Fail
        };

    let Some(content_len) = content_length(headers) else {
        return Ok(HttpResponse {
            outcome,
            reusable: false,
        });
    };
    let already = buf.len().saturating_sub(header_end);
    let mut remaining = content_len.saturating_sub(already);
    while remaining > 0 {
        let mut chunk = [0_u8; 1024];
        let n = stream.read(&mut chunk[..remaining.min(1024)]).await?;
        if n == 0 {
            bail!("connection closed before HTTP response body completed");
        }
        remaining -= n;
    }
    Ok(HttpResponse {
        outcome,
        reusable: true,
    })
}

fn find_header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n").map(|i| i + 4)
}

fn content_length(headers: &str) -> Option<usize> {
    headers.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        if name.eq_ignore_ascii_case("content-length") {
            value.trim().parse::<usize>().ok()
        } else {
            None
        }
    })
}

struct TenantSchedule {
    tenant: usize,
    tenants: usize,
    rps_per_tenant: u64,
    window_ms: u64,
    started: Instant,
    duration_ns: u128,
    seq: u128,
}

impl TenantSchedule {
    fn new(cfg: &Config, tenant: usize, started: Instant, finished: Instant) -> Self {
        Self {
            tenant,
            tenants: cfg.tenants,
            rps_per_tenant: cfg.rps_per_tenant,
            window_ms: cfg.window_ms,
            started,
            duration_ns: finished.saturating_duration_since(started).as_nanos(),
            seq: 0,
        }
    }

    fn offset_ns(&self) -> u128 {
        tenant_phase_ns(self.tenant, self.tenants, self.rps_per_tenant)
            + self.seq * 1_000_000_000_u128 / u128::from(self.rps_per_tenant)
    }
}

impl Iterator for TenantSchedule {
    type Item = ScheduledRequest;

    fn next(&mut self) -> Option<Self::Item> {
        let offset_ns = self.offset_ns();
        if offset_ns >= self.duration_ns {
            return None;
        }
        self.seq += 1;
        let window = (offset_ns / (u128::from(self.window_ms) * 1_000_000)) as usize;
        let scheduled_at = self.started + duration_from_nanos(offset_ns);
        Some(ScheduledRequest {
            tenant: self.tenant,
            window,
            scheduled_at,
        })
    }
}

fn tenant_phase_ns(tenant: usize, tenants: usize, rps_per_tenant: u64) -> u128 {
    (tenant as u128 * 1_000_000_000_u128)
        / (tenants.max(1) as u128 * u128::from(rps_per_tenant.max(1)))
}

fn duration_from_nanos(ns: u128) -> Duration {
    let ns = ns.min(u128::from(u64::MAX)) as u64;
    Duration::from_nanos(ns)
}

fn expected_allowed_per_window(cfg: &Config, windows: u64) -> Vec<u64> {
    let mut expected = vec![0_u64; windows as usize];
    let started = Instant::now();
    let finished = started + Duration::from_millis(windows.saturating_mul(cfg.window_ms));
    for tenant in 0..cfg.tenants {
        let mut current_window = 0_usize;
        let mut offered = 0_u64;
        for request in TenantSchedule::new(cfg, tenant, started, finished) {
            if request.window != current_window {
                if let Some(slot) = expected.get_mut(current_window) {
                    *slot = slot.saturating_add(offered.min(cfg.budget_per_tenant));
                }
                current_window = request.window;
                offered = 0;
            }
            offered = offered.saturating_add(1);
        }
        if let Some(slot) = expected.get_mut(current_window) {
            *slot = slot.saturating_add(offered.min(cfg.budget_per_tenant));
        }
    }
    expected
}

fn expected_attempts_total(cfg: &Config, windows: u64) -> u64 {
    let mut total = 0_u64;
    let started = Instant::now();
    let finished = started + Duration::from_millis(windows.saturating_mul(cfg.window_ms));
    for tenant in 0..cfg.tenants {
        total = total
            .saturating_add(TenantSchedule::new(cfg, tenant, started, finished).count() as u64);
    }
    total
}

fn expected_allowed_total(cfg: &Config, windows: u64) -> u64 {
    let mut total = 0_u64;
    let started = Instant::now();
    let finished = started + Duration::from_millis(windows.saturating_mul(cfg.window_ms));
    let tenant_budget = cfg.budget_per_tenant.saturating_mul(windows);
    for tenant in 0..cfg.tenants {
        let offered = TenantSchedule::new(cfg, tenant, started, finished).count() as u64;
        total = total.saturating_add(offered.min(tenant_budget));
    }
    total
}

fn expected_load(cfg: &Config, windows: u64) -> ExpectedLoad {
    ExpectedLoad {
        attempts: expected_attempts_total(cfg, windows),
        allowed: expected_allowed_total(cfg, windows),
        allowed_per_window: expected_allowed_per_window(cfg, windows),
    }
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
    expected: &ExpectedLoad,
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
    if cfg.backend == Backend::Http {
        println!("  http connections           {}", cfg.http_connections);
    }
    println!("  window ms                  {}", cfg.window_ms);
    println!("  windows                    {}", windows);
    println!("  started wall ms            {}", started_wall_ms);
    println!();
    println!("  per-window:");
    for (idx, counter) in counters.iter().enumerate() {
        let (ok, over, fail) = counter.snapshot();
        let expected_ok = expected
            .allowed_per_window
            .get(idx)
            .copied()
            .unwrap_or_default();
        total_ok += ok;
        total_over += over;
        total_fail += fail;
        println!("    window={idx} ok={ok} over={over} fail={fail} expected_ok={expected_ok}",);
    }
    println!();
    println!("  expected attempts          {}", expected.attempts);
    println!(
        "  actual attempts            {}",
        total_ok + total_over + total_fail
    );
    println!("  expected allowed           {}", expected.allowed);
    println!("  actual allowed             {total_ok}");
    println!("  actual rejected            {total_over}");
    println!("  failed calls               {total_fail}");
    let elapsed_s = (windows.saturating_mul(cfg.window_ms)) as f64 / 1_000.0;
    let actual_attempts = total_ok
        .saturating_add(total_over)
        .saturating_add(total_fail);
    if elapsed_s > 0.0 {
        println!(
            "  transmit rate              {:.1} r/s",
            actual_attempts as f64 / elapsed_s
        );
        println!(
            "  allowed rate               {:.1} r/s",
            total_ok as f64 / elapsed_s
        );
        println!(
            "  limited rate               {:.1} r/s",
            total_over as f64 / elapsed_s
        );
        println!(
            "  failure rate               {:.1} r/s",
            total_fail as f64 / elapsed_s
        );
    }
    if actual_attempts > 0 {
        println!(
            "  allowed percent            {:.2}",
            100.0 * total_ok as f64 / actual_attempts as f64
        );
        println!(
            "  limited percent            {:.2}",
            100.0 * total_over as f64 / actual_attempts as f64
        );
        println!(
            "  failure percent            {:.2}",
            100.0 * total_fail as f64 / actual_attempts as f64
        );
    }
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
            http_connections: parse_env("HTTP_CONNECTIONS", 20)?,
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
            if cfg.duration_s == 0 {
                bail!("DURATION_S must be greater than zero");
            }
            if cfg.request_timeout_ms == 0 {
                bail!("REQUEST_TIMEOUT_MS must be greater than zero");
            }
            if cfg.http_connections == 0 {
                bail!("HTTP_CONNECTIONS must be greater than zero");
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
