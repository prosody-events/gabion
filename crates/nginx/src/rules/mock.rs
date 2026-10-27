//! Test-only constructor for [`CompiledRules`] that needs no nginx.

use super::{CompiledRules, NopBindingCompiler, RuleConfig, RuleConfigError};
use crate::mock::limits;

/// Compile `configs` with default cardinality limits and
/// [`NopBindingCompiler`].
pub(crate) fn compile(configs: &[RuleConfig]) -> Result<CompiledRules, RuleConfigError> {
    CompiledRules::compile_with(configs, limits(|_| {}), &mut NopBindingCompiler)
}
