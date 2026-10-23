//! Validation of the simulator against results known independently of
//! gabion's protocol (see "Validation" in the crate README). Reference nodes
//! drive the harness's own network model and event loop; the byte-accounting
//! check also runs a real gabion cluster.

mod reference;
mod tests;
