use super::MsgVisitor;
use std::fmt::Write;

#[test]
fn message_renders_before_fields_regardless_of_arrival_order() {
    let mut v = MsgVisitor::default();
    // tracing walks fields in macro-argument order, which may put
    // structured kvs *before* the message. Verify the renderer puts
    // the message first either way.
    v.append_field(format_args!("{}={}", "key", "value"));
    let _ = write!(v.message, "the message");
    assert_eq!(v.render(), "the message key=value");
}

#[test]
fn fields_only_event_renders_cleanly() {
    let mut v = MsgVisitor::default();
    v.append_field(format_args!("a={}", 1));
    v.append_field(format_args!("b={}", 2));
    assert_eq!(v.render(), "a=1 b=2");
}

#[test]
fn message_only_event_renders_cleanly() {
    let mut v = MsgVisitor::default();
    let _ = write!(v.message, "hello");
    assert_eq!(v.render(), "hello");
}
