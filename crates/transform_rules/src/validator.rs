use std::collections::HashSet;

use crate::error::{ErrorCode, RuleError, ValidationResult};
use crate::locator::YamlLocator;
use crate::model::{Expr, ExprOp, ExprRef, InputFormat, Mapping, RuleFile};
use crate::path::{parse_path, PathToken};

pub fn validate_rule_file(rule: &RuleFile) -> ValidationResult {
    validate_rule_file_with_locator(rule, None)
}

pub fn validate_rule_file_with_source(rule: &RuleFile, source: &str) -> ValidationResult {
    let locator = YamlLocator::from_str(source);
    validate_rule_file_with_locator(rule, Some(&locator))
}

fn validate_rule_file_with_locator(rule: &RuleFile, locator: Option<&YamlLocator>) -> ValidationResult {
    let mut ctx = ValidationCtx::new(locator);

    validate_version(rule, &mut ctx);
    validate_input(rule, &mut ctx);
    validate_mappings(rule, &mut ctx);

    ctx.finish()
}

fn validate_version(rule: &RuleFile, ctx: &mut ValidationCtx<'_>) {
    if rule.version != 1 {
        ctx.push(ErrorCode::InvalidVersion, "version must be 1", "version");
    }
}

fn validate_input(rule: &RuleFile, ctx: &mut ValidationCtx<'_>) {
    match rule.input.format {
        InputFormat::Csv => {
            if rule.input.csv.is_none() {
                ctx.push(
                    ErrorCode::MissingCsvSection,
                    "input.csv is required when format=csv",
                    "input.csv",
                );
            }
        }
        InputFormat::Json => {
            if rule.input.json.is_none() {
                ctx.push(
                    ErrorCode::MissingJsonSection,
                    "input.json is required when format=json",
                    "input.json",
                );
            }
        }
    }

    if let Some(csv) = &rule.input.csv {
        if csv.delimiter.chars().count() != 1 {
            ctx.push(
                ErrorCode::InvalidDelimiterLength,
                "csv.delimiter must be a single character",
                "input.csv.delimiter",
            );
        }
        if !csv.has_header && csv.columns.is_none() {
            ctx.push(
                ErrorCode::MissingCsvColumns,
                "csv.columns is required when has_header=false",
                "input.csv.columns",
            );
        }
    }

    if let Some(json) = &rule.input.json {
        if let Some(path) = json.records_path.as_deref() {
            if parse_path(path).is_err() {
                ctx.push(
                    ErrorCode::InvalidPath,
                    "records_path is invalid",
                    "input.json.records_path",
                );
            }
        }
    }
}

fn validate_mappings(rule: &RuleFile, ctx: &mut ValidationCtx<'_>) {
    let mut produced_targets: HashSet<Vec<PathToken>> = HashSet::new();

    for (index, mapping) in rule.mappings.iter().enumerate() {
        let base = format!("mappings[{}]", index);

        if mapping.target.trim().is_empty() {
            ctx.push(
                ErrorCode::MissingTarget,
                "mapping.target is required",
                format!("{}.target", base),
            );
        }

        let target_tokens = match parse_path(&mapping.target) {
            Ok(tokens) => tokens,
            Err(_) => {
                ctx.push(
                    ErrorCode::InvalidPath,
                    "target path is invalid",
                    format!("{}.target", base),
                );
                continue;
            }
        };
        if target_tokens
            .iter()
            .any(|token| matches!(token, PathToken::Index(_)))
        {
            ctx.push(
                ErrorCode::InvalidPath,
                "target path must not include indexes",
                format!("{}.target", base),
            );
            continue;
        }

        if produced_targets.contains(&target_tokens) {
            ctx.push(
                ErrorCode::DuplicateTarget,
                "mapping.target is duplicated",
                format!("{}.target", base),
            );
        }

        let value_count = count_value_fields(mapping);
        if value_count == 0 {
            ctx.push(
                ErrorCode::MissingMappingValue,
                "mapping must define source, value, or expr",
                base.clone(),
            );
        } else if value_count > 1 {
            ctx.push(
                ErrorCode::SourceValueExprExclusive,
                "exactly one of source/value/expr is required",
                base.clone(),
            );
        }

        if let Some(type_name) = &mapping.value_type {
            if !is_valid_type_name(type_name) {
                ctx.push(
                    ErrorCode::InvalidTypeName,
                    "type must be string|int|float|bool",
                    format!("{}.type", base),
                );
            }
        }

        if let Some(source) = &mapping.source {
            validate_source(source, &base, &produced_targets, ctx);
        }

        if let Some(expr) = &mapping.expr {
            let expr_path = format!("{}.expr", base);
            validate_expr(expr, &expr_path, &produced_targets, ctx);
        }

        produced_targets.insert(target_tokens);
    }
}

fn count_value_fields(mapping: &Mapping) -> usize {
    let mut count = 0;
    if mapping.source.is_some() {
        count += 1;
    }
    if mapping.value.is_some() {
        count += 1;
    }
    if mapping.expr.is_some() {
        count += 1;
    }
    count
}

fn validate_source(
    source: &str,
    base_path: &str,
    produced_targets: &HashSet<Vec<PathToken>>,
    ctx: &mut ValidationCtx<'_>,
) {
    let full_path = format!("{}.source", base_path);
    let (namespace, path) = match parse_source(source) {
        Some(parsed) => parsed,
        None => {
            ctx.push(
                ErrorCode::InvalidRefNamespace,
                "ref namespace must be input|context|out",
                full_path,
            );
            return;
        }
    };

    let tokens = match parse_path(path) {
        Ok(tokens) => tokens,
        Err(_) => {
            ctx.push(ErrorCode::InvalidPath, "path is invalid", full_path);
            return;
        }
    };

    if namespace == Namespace::Out && !out_ref_resolves(&tokens, produced_targets) {
        ctx.push(
            ErrorCode::ForwardOutReference,
            "out reference must point to previous mappings",
            full_path,
        );
    }
}

fn validate_expr(
    expr: &Expr,
    base_path: &str,
    produced_targets: &HashSet<Vec<PathToken>>,
    ctx: &mut ValidationCtx<'_>,
) {
    match expr {
        Expr::Ref(expr_ref) => validate_ref(expr_ref, base_path, produced_targets, ctx),
        Expr::Op(expr_op) => validate_op(expr_op, base_path, produced_targets, ctx),
        Expr::Literal(_) => {}
    }
}

fn validate_ref(
    expr_ref: &ExprRef,
    base_path: &str,
    produced_targets: &HashSet<Vec<PathToken>>,
    ctx: &mut ValidationCtx<'_>,
) {
    let (namespace, path) = match parse_ref(&expr_ref.ref_path) {
        Some(parsed) => parsed,
        None => {
            ctx.push(
                ErrorCode::InvalidRefNamespace,
                "ref namespace must be input|context|out",
                base_path,
            );
            return;
        }
    };

    let tokens = match parse_path(path) {
        Ok(tokens) => tokens,
        Err(_) => {
            ctx.push(ErrorCode::InvalidPath, "path is invalid", base_path);
            return;
        }
    };

    if namespace == Namespace::Out && !out_ref_resolves(&tokens, produced_targets) {
        ctx.push(
            ErrorCode::ForwardOutReference,
            "out reference must point to previous mappings",
            base_path,
        );
    }
}

fn out_ref_resolves(tokens: &[PathToken], produced_targets: &HashSet<Vec<PathToken>>) -> bool {
    let key_tokens: Vec<PathToken> = tokens
        .iter()
        .filter_map(|token| match token {
            PathToken::Key(key) => Some(PathToken::Key(key.clone())),
            PathToken::Index(_) => None,
        })
        .collect();
    if key_tokens.is_empty() {
        return false;
    }

    for end in (1..=key_tokens.len()).rev() {
        if produced_targets.contains(&key_tokens[..end].to_vec()) {
            return true;
        }
    }
    false
}

fn validate_op(
    expr_op: &ExprOp,
    base_path: &str,
    produced_targets: &HashSet<Vec<PathToken>>,
    ctx: &mut ValidationCtx<'_>,
) {
    if !is_valid_op(&expr_op.op) {
        ctx.push(
            ErrorCode::UnknownOp,
            "expr.op is not supported",
            format!("{}.op", base_path),
        );
    }

    if expr_op.args.is_empty() {
        ctx.push(
            ErrorCode::InvalidArgs,
            "expr.args must be a non-empty array",
            format!("{}.args", base_path),
        );
    }

    match expr_op.op.as_str() {
        "trim" | "lowercase" | "uppercase" | "to_string" => {
            if expr_op.args.len() != 1 {
                ctx.push(
                    ErrorCode::InvalidArgs,
                    "expr.args must contain exactly one item",
                    format!("{}.args", base_path),
                );
            }
        }
        "lookup" | "lookup_first" => {
            validate_lookup_args(expr_op, base_path, ctx);
        }
        _ => {}
    }

    for (index, arg) in expr_op.args.iter().enumerate() {
        let arg_path = format!("{}.args[{}]", base_path, index);
        validate_expr(arg, &arg_path, produced_targets, ctx);
    }
}

fn parse_ref(value: &str) -> Option<(Namespace, &str)> {
    let mut parts = value.splitn(2, '.');
    let namespace = parts.next()?;
    let path = parts.next()?;
    if path.is_empty() {
        return None;
    }

    let namespace = match namespace {
        "input" => Namespace::Input,
        "context" => Namespace::Context,
        "out" => Namespace::Out,
        _ => return None,
    };

    Some((namespace, path))
}

fn parse_source(value: &str) -> Option<(Namespace, &str)> {
    if let Some((prefix, path)) = value.split_once('.') {
        if path.is_empty() {
            return None;
        }
        let namespace = match prefix {
            "input" => Namespace::Input,
            "context" => Namespace::Context,
            "out" => Namespace::Out,
            _ => return None,
        };
        Some((namespace, path))
    } else {
        if value.is_empty() {
            return None;
        }
        Some((Namespace::Input, value))
    }
}

fn is_valid_type_name(value: &str) -> bool {
    matches!(value, "string" | "int" | "float" | "bool")
}

fn is_valid_op(value: &str) -> bool {
    matches!(
        value,
        "concat"
            | "coalesce"
            | "to_string"
            | "trim"
            | "lowercase"
            | "uppercase"
            | "lookup"
            | "lookup_first"
    )
}

fn validate_lookup_args(expr_op: &ExprOp, base_path: &str, ctx: &mut ValidationCtx<'_>) {
    let len = expr_op.args.len();
    if !(3..=4).contains(&len) {
        ctx.push(
            ErrorCode::InvalidArgs,
            "lookup args must be [collection, key_path, match_value, output_path?]",
            format!("{}.args", base_path),
        );
        return;
    }

    let key_path = literal_string(&expr_op.args[1]);
    if key_path.is_none() || key_path == Some("") {
        ctx.push(
            ErrorCode::InvalidArgs,
            "lookup key_path must be a non-empty string literal",
            format!("{}.args[1]", base_path),
        );
    } else if parse_path(key_path.unwrap()).is_err() {
        ctx.push(
            ErrorCode::InvalidArgs,
            "lookup key_path is invalid",
            format!("{}.args[1]", base_path),
        );
    }

    if len == 4 {
        let output_path = literal_string(&expr_op.args[3]);
        if output_path.is_none() || output_path == Some("") {
            ctx.push(
                ErrorCode::InvalidArgs,
                "lookup output_path must be a non-empty string literal",
                format!("{}.args[3]", base_path),
            );
        } else if parse_path(output_path.unwrap()).is_err() {
            ctx.push(
                ErrorCode::InvalidArgs,
                "lookup output_path is invalid",
                format!("{}.args[3]", base_path),
            );
        }
    }
}

fn literal_string(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Literal(value) => value.as_str(),
        _ => None,
    }
}

struct ValidationCtx<'a> {
    locator: Option<&'a YamlLocator>,
    errors: Vec<RuleError>,
}

impl<'a> ValidationCtx<'a> {
    fn new(locator: Option<&'a YamlLocator>) -> Self {
        Self {
            locator,
            errors: Vec::new(),
        }
    }

    fn push(&mut self, code: ErrorCode, message: &str, path: impl Into<String>) {
        let path = path.into();
        let mut err = RuleError::new(code, message).with_path(path.clone());
        if let Some(locator) = self.locator {
            if let Some(location) = locator.location_for(&path) {
                err = err.with_location(location.line, location.column);
            }
        }
        self.errors.push(err);
    }

    fn finish(self) -> ValidationResult {
        if self.errors.is_empty() {
            Ok(())
        } else {
            Err(self.errors)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Namespace {
    Input,
    Context,
    Out,
}
