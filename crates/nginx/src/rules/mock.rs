//! Test-only constructor for [`CompiledRules`] that needs no nginx.

use super::{CompiledRules, NopBindingCompiler, RuleConfig, RuleConfigError, parse_rule_directive};
use crate::mock::validated;

/// One `gabion_limit_rule` directive's arguments, split on whitespace.
pub(crate) fn directive(text: &str) -> RuleConfig {
    let args: Vec<&str> = text.split_whitespace().collect();
    parse_rule_directive(&args).unwrap_or_else(|error| panic!("{text}: {error}"))
}

/// Compile `configs` with [`crate::mock::validated`]'s test settings and
/// [`NopBindingCompiler`].
pub(crate) fn compile(configs: &[RuleConfig]) -> Result<CompiledRules, RuleConfigError> {
    CompiledRules::compile_with(configs, &validated(|_| {}).1, &mut NopBindingCompiler)
}

/// Compile one rule per directive.
pub(crate) fn rules(directives: &[&str]) -> CompiledRules {
    let configs: Vec<RuleConfig> = directives.iter().map(|text| directive(text)).collect();
    compile(&configs).expect("compile rules")
}
