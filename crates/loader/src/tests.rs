use std::time::Duration;

use tokio::time::Instant;

use super::{
    Backend, Config, Outcome, TenantSchedule, content_length, expected_allowed_per_window,
    expected_allowed_total, expected_attempts_total, find_header_end, tenant_phase_ns,
};

fn config(tenants: usize, rps_per_tenant: u64, budget_per_tenant: u64) -> Config {
    Config {
        backend: Backend::Http,
        target_url: "http://gabion-nginx:8080/tenant/index.html?tenant={tenant}".to_string(),
        grpc_addr: "gabiond:8081".to_string(),
        domain: "nginx".to_string(),
        tenants,
        budget_per_tenant,
        rps_per_tenant,
        duration_s: 60,
        warmup_s: 0,
        window_ms: 1_000,
        align_window: true,
        descriptor_key: "tenant".to_string(),
        request_timeout_ms: 2_000,
        http_connections: 20,
    }
}

#[test]
fn expected_allowed_is_capped_by_offered_load() {
    let cfg = config(1_000, 1, 20);
    let expected = expected_allowed_per_window(&cfg, 60);
    assert_eq!(expected.iter().sum::<u64>(), 60_000);
    assert_eq!(expected_attempts_total(&cfg, 60), 60_000);
    assert_eq!(expected_allowed_total(&cfg, 60), 60_000);
    assert!(expected.iter().all(|v| *v == 1_000));
}

#[test]
fn expected_allowed_is_capped_by_budget_under_overload() {
    let cfg = config(100, 100, 20);
    let expected = expected_allowed_per_window(&cfg, 60);
    assert_eq!(expected.iter().sum::<u64>(), 120_000);
    assert_eq!(expected_attempts_total(&cfg, 60), 600_000);
    assert_eq!(expected_allowed_total(&cfg, 60), 120_000);
    assert!(expected.iter().all(|v| *v == 2_000));
}

#[test]
fn schedule_does_not_over_emit_for_non_divisor_rates() {
    let cfg = config(1, 3, 20);
    let started = Instant::now();
    let finished = started + Duration::from_secs(1);
    let requests = TenantSchedule::new(&cfg, 0, started, finished).collect::<Vec<_>>();
    assert_eq!(requests.len(), 3);
    assert!(requests.iter().all(|r| r.window == 0));
}

#[test]
fn tenant_phase_spreads_tenants_across_one_interval() {
    assert_eq!(tenant_phase_ns(0, 1_000, 1), 0);
    assert_eq!(tenant_phase_ns(500, 1_000, 1), 500_000_000);
    assert_eq!(tenant_phase_ns(999, 1_000, 1), 999_000_000);
}

#[test]
fn finds_http_header_end() {
    assert_eq!(find_header_end(b"HTTP/1.1 200 OK\r\n\r\nbody"), Some(19));
    assert_eq!(find_header_end(b"HTTP/1.1 200 OK\r\n"), None);
}

#[test]
fn parses_content_length_case_insensitively() {
    assert_eq!(
        content_length("HTTP/1.1 200 OK\r\nContent-Length: 42\r\n"),
        Some(42)
    );
    assert_eq!(
        content_length("HTTP/1.1 200 OK\r\ncontent-length: 7\r\n"),
        Some(7)
    );
    assert_eq!(
        content_length("HTTP/1.1 200 OK\r\nConnection: close\r\n"),
        None
    );
}

#[test]
fn outcome_keeps_http_status_categories_distinct() {
    assert_ne!(Outcome::Ok, Outcome::Over);
    assert_ne!(Outcome::Over, Outcome::Fail);
}
