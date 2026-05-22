use super::*;
use std::collections::HashMap;

/// Test-only [`VariableLookup`]. Dispatches on the same enum the
/// production lookup does — the inline arms return canned values; the
/// `IndexedVariable` arm reads from a `HashMap` keyed on the variable
/// name (ignoring the synthetic index).
pub struct MockVars {
    pub vars: HashMap<String, Vec<u8>>,
    pub uri: Vec<u8>,
    pub args: Vec<u8>,
    pub remote_addr: Vec<u8>,
    pub request_uri: Vec<u8>,
}

impl MockVars {
    pub fn new() -> Self {
        Self {
            vars: HashMap::new(),
            uri: Vec::new(),
            args: Vec::new(),
            remote_addr: Vec::new(),
            request_uri: Vec::new(),
        }
    }

    /// Set a value for an indexed-variable lookup keyed on the
    /// variable name (the `$`-stripped identifier).
    pub fn set(mut self, name: &str, value: &str) -> Self {
        self.vars
            .insert(name.to_string(), value.as_bytes().to_vec());
        self
    }

    pub fn set_bytes(mut self, name: &str, value: &[u8]) -> Self {
        self.vars.insert(name.to_string(), value.to_vec());
        self
    }
}

impl VariableLookup for MockVars {
    fn lookup(&self, binding: &BindingLookup) -> Option<&[u8]> {
        match binding {
            BindingLookup::Uri => Some(self.uri.as_slice()),
            BindingLookup::RequestUri => Some(self.request_uri.as_slice()),
            BindingLookup::Args => Some(self.args.as_slice()),
            BindingLookup::RemoteAddr => Some(self.remote_addr.as_slice()),
            BindingLookup::Arg(name) => find_query_arg_mock(self.args.as_slice(), name.as_bytes()),
            BindingLookup::IndexedVariable { name, .. } => {
                self.vars.get(name.as_ref()).map(Vec::as_slice)
            }
            BindingLookup::ComplexValue { .. } => None,
        }
    }
}

fn find_query_arg_mock<'a>(args: &'a [u8], name: &[u8]) -> Option<&'a [u8]> {
    let mut rest = args;
    while !rest.is_empty() {
        let next = rest
            .iter()
            .position(|byte| *byte == b'&')
            .unwrap_or(rest.len());
        let pair = &rest[..next];
        if let Some(eq) = pair.iter().position(|byte| *byte == b'=')
            && &pair[..eq] == name
        {
            return Some(&pair[eq + 1..]);
        }
        if next == rest.len() {
            break;
        }
        rest = &rest[next + 1..];
    }
    None
}

use crate::shm::ShmRegion;

#[allow(dead_code)]
pub(crate) fn ctx<'a>(rules: &'a CompiledRules, region: &'a ShmRegion) -> AccessCtx<'a> {
    AccessCtx {
        rules,
        aggregate: region.aggregate(),
        queue: region.queue(),
        stats: region.stats(),
        domain: crate::rules::DEFAULT_DOMAIN,
        cardinality: CardinalitySettings::default(),
    }
}
