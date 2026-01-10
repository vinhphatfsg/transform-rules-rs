mod error;
mod locator;
mod model;
mod transform;
mod validator;

pub use error::{
    ErrorCode, RuleError, TransformError, TransformErrorKind, ValidationResult, YamlLocation,
};
pub use model::{Expr, ExprOp, ExprRef, InputFormat, InputSpec, Mapping, RuleFile};
pub use transform::transform;
pub use validator::{validate_rule_file, validate_rule_file_with_source};

pub fn parse_rule_file(yaml: &str) -> Result<RuleFile, serde_yaml::Error> {
    serde_yaml::from_str(yaml)
}
