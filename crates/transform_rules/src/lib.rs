mod error;
mod locator;
mod model;
mod path;
mod dto;
mod transform;
mod validator;

pub use error::{
    ErrorCode, RuleError, TransformError, TransformErrorKind, TransformWarning, ValidationResult,
    YamlLocation,
};
pub use dto::{generate_dto, DtoError, DtoLanguage};
pub use model::{Expr, ExprOp, ExprRef, InputFormat, InputSpec, Mapping, RuleFile};
pub use transform::{
    preflight_validate, preflight_validate_with_warnings, transform, transform_stream,
    transform_with_warnings, TransformStream, TransformStreamItem,
};
pub use validator::{validate_rule_file, validate_rule_file_with_source};

pub fn parse_rule_file(yaml: &str) -> Result<RuleFile, serde_yaml::Error> {
    serde_yaml::from_str(yaml)
}
