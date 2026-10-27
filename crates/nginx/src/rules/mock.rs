//! Test-only constructor for [`CompiledRules`] that needs no nginx.

use super::{CardinalitySettings, CompiledRules, NopBindingCompiler, RuleConfig, RuleConfigError};

/// Compile `configs` with default cardinality limits and
/// [`NopBindingCompiler`].
pub(crate) fn compile(configs: &[RuleConfig]) -> Result<CompiledRules, RuleConfigError> {
    CompiledRules::compile_with(
        configs,
        CardinalitySettings::default(),
        &mut NopBindingCompiler,
    )
}
