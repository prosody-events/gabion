use super::*;

/// A throttled warning logs on the 1st, 2nd, 4th … occurrence. Node 3 logs
/// 1, 2, 4 of its 5 rejects and node 5 its 1st; a u64 field not named
/// `*_total` must not count.
#[test]
fn warnings_capture_counts_power_of_two_lines() {
    let warnings = Warnings::new();
    tracing::subscriber::with_default(warnings.subscriber(), || {
        for (node, rejects) in [(3, 5_u64), (5, 1)] {
            warnings.set_node(node);
            for total in (1..=rejects).filter(|n| n.is_power_of_two()) {
                tracing::warn!(
                    count = 99_u64,
                    rejected_total = total,
                    "Dropped a packet. Raise `x`."
                );
            }
        }
        warnings.set_node(NO_NODE);
        tracing::error!(failed_total = 2_u64, "Something broke.");
        tracing::info!("Ignored.");
    });
    let report = warnings.report();
    assert_eq!((report.warn_events, report.error_events), (4, 1));
    assert_eq!(report.kinds.len(), 2);
    let error = &report.kinds[0];
    assert_eq!(
        (error.level.as_str(), error.nodes, error.max_total),
        ("ERROR", 0, Some(2))
    );
    let dropped = &report.kinds[1];
    assert_eq!(dropped.message, "Dropped a packet. Raise `x`.");
    assert_eq!((dropped.events, dropped.nodes), (4, 2));
    assert_eq!(
        (dropped.max_total, dropped.sum_node_max_total),
        (Some(4), 5)
    );
}
