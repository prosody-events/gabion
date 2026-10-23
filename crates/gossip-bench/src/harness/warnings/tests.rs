use super::*;

#[test]
fn tallies_warnings_per_node_and_keeps_the_largest_total() {
    let warnings = Warnings::new();
    tracing::subscriber::with_default(warnings.subscriber(), || {
        for (node, total) in [(3, 1_u64), (3, 2), (3, 4), (5, 8)] {
            warnings.set_node(node);
            tracing::warn!(
                overflow_total = total,
                capacity = 16_u64,
                "Forwarded gossip dirty ring overflowed. Raise it."
            );
        }
        warnings.set_node(NO_NODE);
        tracing::error!(rejected_total = 2_u64, "Something broke.");
        tracing::info!("Ignored.");
    });
    let report = warnings.report();
    assert_eq!((report.warn_events, report.error_events), (4, 1));
    assert_eq!(report.dirty_overflow_nodes, 2);
    assert_eq!(report.dirty_overflow_lower_bound, 4 + 8);
    assert_eq!(report.kinds.len(), 2);
    let dirty = &report.kinds[1];
    assert_eq!(dirty.level, "WARN");
    assert_eq!(
        dirty.message,
        "Forwarded gossip dirty ring overflowed. Raise it."
    );
    assert_eq!((dirty.events, dirty.nodes), (4, 2));
    assert_eq!((dirty.max_total, dirty.sum_node_max_total), (Some(8), 12));
    let error = &report.kinds[0];
    assert_eq!(
        (error.level, error.nodes, error.max_total),
        ("ERROR", 0, Some(2))
    );
}
