use csv::ReaderBuilder;
use serde_json::{Map, Value as JsonValue};

use crate::error::{TransformError, TransformErrorKind, TransformWarning};
use crate::model::{Expr, ExprOp, ExprRef, InputFormat, RuleFile};
use crate::path::{get_path, parse_path, PathToken};

pub fn transform(
    rule: &RuleFile,
    input: &str,
    context: Option<&JsonValue>,
) -> Result<JsonValue, TransformError> {
    transform_with_warnings(rule, input, context).map(|(output, _)| output)
}

pub fn preflight_validate(
    rule: &RuleFile,
    input: &str,
    context: Option<&JsonValue>,
) -> Result<(), TransformError> {
    preflight_validate_with_warnings(rule, input, context).map(|_| ())
}

pub fn transform_with_warnings(
    rule: &RuleFile,
    input: &str,
    context: Option<&JsonValue>,
) -> Result<(JsonValue, Vec<TransformWarning>), TransformError> {
    let records = parse_input_records(rule, input)?;

    let mut output_records = Vec::with_capacity(records.len());
    let mut warnings = Vec::new();
    for record in records {
        let out = apply_mappings(rule, &record, context, &mut warnings)?;
        output_records.push(out);
    }

    Ok((JsonValue::Array(output_records), warnings))
}

pub fn preflight_validate_with_warnings(
    rule: &RuleFile,
    input: &str,
    context: Option<&JsonValue>,
) -> Result<Vec<TransformWarning>, TransformError> {
    let records = parse_input_records(rule, input)?;
    let mut warnings = Vec::new();
    for record in records {
        let _ = apply_mappings(rule, &record, context, &mut warnings)?;
    }
    Ok(warnings)
}

fn parse_input_records(rule: &RuleFile, input: &str) -> Result<Vec<JsonValue>, TransformError> {
    match rule.input.format {
        InputFormat::Csv => parse_csv(rule, input),
        InputFormat::Json => parse_json(rule, input),
    }
}

fn apply_mappings(
    rule: &RuleFile,
    record: &JsonValue,
    context: Option<&JsonValue>,
    warnings: &mut Vec<TransformWarning>,
) -> Result<JsonValue, TransformError> {
    let mut out = JsonValue::Object(Map::new());
    for (index, mapping) in rule.mappings.iter().enumerate() {
        let mapping_path = format!("mappings[{}]", index);
        if !eval_when(mapping, record, context, &out, &mapping_path, warnings) {
            continue;
        }
        let value = eval_mapping(mapping, record, context, &out, &mapping_path)?;
        if let Some(value) = value {
            set_path(&mut out, &mapping.target, value, &mapping_path)?;
        }
    }
    Ok(out)
}

fn parse_json(rule: &RuleFile, input: &str) -> Result<Vec<JsonValue>, TransformError> {
    let value: JsonValue = serde_json::from_str(input).map_err(|err| {
        TransformError::new(
            TransformErrorKind::InvalidInput,
            format!("failed to parse JSON input: {}", err),
        )
    })?;

    let records_value = match rule.input.json.as_ref().and_then(|j| j.records_path.as_deref()) {
        Some(path) => {
            let tokens = parse_path(path).map_err(|err| {
                TransformError::new(TransformErrorKind::InvalidRecordsPath, err.message())
                    .with_path("input.json.records_path")
            })?;
            let found = get_path(&value, &tokens).ok_or_else(|| {
                TransformError::new(
                    TransformErrorKind::InvalidRecordsPath,
                    "records_path does not exist",
                )
                .with_path("input.json.records_path")
            })?;
            found
        }
        None => &value,
    };

    match records_value {
        JsonValue::Array(items) => Ok(items.clone()),
        JsonValue::Object(_) => Ok(vec![records_value.clone()]),
        _ => Err(TransformError::new(
            TransformErrorKind::InvalidInput,
            "records_path must point to an array or object",
        )),
    }
}

fn parse_csv(rule: &RuleFile, input: &str) -> Result<Vec<JsonValue>, TransformError> {
    let csv_spec = rule.input.csv.as_ref().ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::InvalidInput,
            "input.csv is required when format=csv",
        )
    })?;

    let delimiter_chars: Vec<char> = csv_spec.delimiter.chars().collect();
    if delimiter_chars.len() != 1 {
        return Err(TransformError::new(
            TransformErrorKind::InvalidInput,
            "csv.delimiter must be a single character",
        ));
    }
    let delimiter = delimiter_chars[0] as u8;

    let mut reader = ReaderBuilder::new()
        .delimiter(delimiter)
        .has_headers(csv_spec.has_header)
        .from_reader(input.as_bytes());

    let headers: Vec<String> = if csv_spec.has_header {
        let header_record = reader
            .headers()
            .map_err(|err| {
                TransformError::new(
                    TransformErrorKind::InvalidInput,
                    format!("failed to read csv header: {}", err),
                )
            })?
            .clone();
        header_record.iter().map(|s| s.to_string()).collect::<Vec<String>>()
    } else {
        let columns = csv_spec.columns.as_ref().ok_or_else(|| {
            TransformError::new(
                TransformErrorKind::InvalidInput,
                "csv.columns is required when has_header=false",
            )
        })?;
        columns
            .iter()
            .map(|col| col.name.clone())
            .collect::<Vec<String>>()
    };

    let mut records = Vec::new();
    for record in reader.records() {
        let record = record.map_err(|err| {
            TransformError::new(
                TransformErrorKind::InvalidInput,
                format!("failed to read csv record: {}", err),
            )
        })?;
        let obj = record_to_object(&headers, &record);
        records.push(JsonValue::Object(obj));
    }

    Ok(records)
}

fn record_to_object(headers: &[String], record: &csv::StringRecord) -> Map<String, JsonValue> {
    let mut obj = Map::new();
    for (index, name) in headers.iter().enumerate() {
        if let Some(value) = record.get(index) {
            obj.insert(name.clone(), JsonValue::String(value.to_string()));
        }
    }
    obj
}

fn eval_mapping(
    mapping: &crate::model::Mapping,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    mapping_path: &str,
) -> Result<Option<JsonValue>, TransformError> {
    let value = if let Some(source) = &mapping.source {
        resolve_source(source, record, context, out, mapping_path)?
    } else if let Some(literal) = &mapping.value {
        EvalValue::Value(literal.clone())
    } else if let Some(expr) = &mapping.expr {
        eval_expr(expr, record, context, out, &format!("{}.expr", mapping_path))?
    } else {
        return Err(TransformError::new(
            TransformErrorKind::InvalidInput,
            "mapping must define source, value, or expr",
        )
        .with_path(mapping_path));
    };

    let mut value = match value {
        EvalValue::Missing => {
            if let Some(default) = &mapping.default {
                default.clone()
            } else if mapping.required {
                return Err(TransformError::new(
                    TransformErrorKind::MissingRequired,
                    "required value is missing",
                )
                .with_path(mapping_path));
            } else {
                return Ok(None);
            }
        }
        EvalValue::Value(value) => value,
    };

    if value.is_null() {
        if mapping.required {
            return Err(TransformError::new(
                TransformErrorKind::MissingRequired,
                "required value is null",
            )
            .with_path(mapping_path));
        }
        return Ok(Some(value));
    }

    if let Some(type_name) = &mapping.value_type {
        value = cast_value(&value, type_name, &format!("{}.type", mapping_path))?;
    }

    Ok(Some(value))
}

fn eval_when(
    mapping: &crate::model::Mapping,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    mapping_path: &str,
    warnings: &mut Vec<TransformWarning>,
) -> bool {
    let expr = match &mapping.when {
        Some(expr) => expr,
        None => return true,
    };

    match eval_when_result(expr, record, context, out, mapping_path) {
        Ok(flag) => flag,
        Err(err) => {
            warnings.push(err.into());
            false
        }
    }
}

fn eval_when_result(
    expr: &Expr,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    mapping_path: &str,
) -> Result<bool, TransformError> {
    let when_path = format!("{}.when", mapping_path);
    let value = eval_expr(expr, record, context, out, &when_path)?;
    let value = match value {
        EvalValue::Missing => JsonValue::Null,
        EvalValue::Value(value) => value,
    };
    match value {
        JsonValue::Bool(flag) => Ok(flag),
        _ => Err(when_type_error(&when_path)),
    }
}

fn when_type_error(path: &str) -> TransformError {
    TransformError::new(
        TransformErrorKind::ExprError,
        "when must evaluate to boolean",
    )
    .with_path(path)
}

fn resolve_source(
    source: &str,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    mapping_path: &str,
) -> Result<EvalValue, TransformError> {
    let (namespace, path) = parse_source(source)
        .map_err(|err| err.with_path(format!("{}.source", mapping_path)))?;
    let tokens = parse_path_tokens(
        path,
        TransformErrorKind::InvalidRef,
        format!("{}.source", mapping_path),
    )?;
    let target = match namespace {
        Namespace::Input => Some(record),
        Namespace::Context => context,
        Namespace::Out => Some(out),
    };

    match target.and_then(|value| get_path(value, &tokens)) {
        Some(value) => Ok(EvalValue::Value(value.clone())),
        None => Ok(EvalValue::Missing),
    }
}

fn eval_expr(
    expr: &Expr,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
) -> Result<EvalValue, TransformError> {
    match expr {
        Expr::Literal(value) => Ok(EvalValue::Value(value.clone())),
        Expr::Ref(expr_ref) => eval_ref(expr_ref, record, context, out, base_path),
        Expr::Op(expr_op) => eval_op(expr_op, record, context, out, base_path),
    }
}

fn eval_ref(
    expr_ref: &ExprRef,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
) -> Result<EvalValue, TransformError> {
    let (namespace, path) = parse_ref(&expr_ref.ref_path).map_err(|err| err.with_path(base_path))?;
    let tokens =
        parse_path_tokens(path, TransformErrorKind::InvalidRef, base_path.to_string())?;
    let target = match namespace {
        Namespace::Input => Some(record),
        Namespace::Context => context,
        Namespace::Out => Some(out),
    };

    match target.and_then(|value| get_path(value, &tokens)) {
        Some(value) => Ok(EvalValue::Value(value.clone())),
        None => Ok(EvalValue::Missing),
    }
}

fn eval_op(
    expr_op: &ExprOp,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
) -> Result<EvalValue, TransformError> {
    if expr_op.args.is_empty() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must be a non-empty array",
        )
        .with_path(format!("{}.args", base_path)));
    }

    match expr_op.op.as_str() {
        "concat" => {
            let mut parts = Vec::new();
            for (index, arg) in expr_op.args.iter().enumerate() {
                let arg_path = format!("{}.args[{}]", base_path, index);
                let value = eval_expr(arg, record, context, out, &arg_path)?;
                match value {
                    EvalValue::Missing => return Ok(EvalValue::Missing),
                    EvalValue::Value(value) => {
                        if value.is_null() {
                            return Err(TransformError::new(
                                TransformErrorKind::ExprError,
                                "concat does not accept null",
                            )
                            .with_path(arg_path));
                        }
                        let part = value_to_string(&value, &arg_path)?;
                        parts.push(part);
                    }
                }
            }
            Ok(EvalValue::Value(JsonValue::String(parts.join(""))))
        }
        "coalesce" => {
            for (index, arg) in expr_op.args.iter().enumerate() {
                let arg_path = format!("{}.args[{}]", base_path, index);
                let value = eval_expr(arg, record, context, out, &arg_path)?;
                match value {
                    EvalValue::Missing => continue,
                    EvalValue::Value(value) => {
                        if value.is_null() {
                            continue;
                        }
                        return Ok(EvalValue::Value(value));
                    }
                }
            }
            Ok(EvalValue::Missing)
        }
        "to_string" => eval_unary_string_op(
            &expr_op.args,
            record,
            context,
            out,
            base_path,
            |value, path| value_to_string(value, path).map(JsonValue::String),
        ),
        "trim" => eval_unary_string_op(
            &expr_op.args,
            record,
            context,
            out,
            base_path,
            |value, path| {
                let s = value_as_string(value, path)?;
                Ok(JsonValue::String(s.trim().to_string()))
            },
        ),
        "lowercase" => eval_unary_string_op(
            &expr_op.args,
            record,
            context,
            out,
            base_path,
            |value, path| {
                let s = value_as_string(value, path)?;
                Ok(JsonValue::String(s.to_lowercase()))
            },
        ),
        "uppercase" => eval_unary_string_op(
            &expr_op.args,
            record,
            context,
            out,
            base_path,
            |value, path| {
                let s = value_as_string(value, path)?;
                Ok(JsonValue::String(s.to_uppercase()))
            },
        ),
        "lookup" => eval_lookup(&expr_op.args, record, context, out, base_path, false),
        "lookup_first" => eval_lookup(&expr_op.args, record, context, out, base_path, true),
        "and" => eval_bool_and_or(&expr_op.args, record, context, out, base_path, true),
        "or" => eval_bool_and_or(&expr_op.args, record, context, out, base_path, false),
        "not" => eval_bool_not(&expr_op.args, record, context, out, base_path),
        "==" | "!=" | "<" | "<=" | ">" | ">=" | "~=" => {
            eval_compare(expr_op, record, context, out, base_path)
        }
        _ => Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.op is not supported",
        )
        .with_path(format!("{}.op", base_path))),
    }
}

fn eval_unary_string_op<F>(
    args: &[Expr],
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    op: F,
) -> Result<EvalValue, TransformError>
where
    F: FnOnce(&JsonValue, &str) -> Result<JsonValue, TransformError>,
{
    if args.len() != 1 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly one item",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let arg_path = format!("{}.args[0]", base_path);
    let value = eval_expr(&args[0], record, context, out, &arg_path)?;
    match value {
        EvalValue::Missing => Ok(EvalValue::Missing),
        EvalValue::Value(value) => {
            if value.is_null() {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "expr arg must not be null",
                )
                .with_path(arg_path));
            }
            op(&value, &arg_path).map(EvalValue::Value)
        }
    }
}

fn eval_lookup(
    args: &[Expr],
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    first_only: bool,
) -> Result<EvalValue, TransformError> {
    if !(3..=4).contains(&args.len()) {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "lookup args must be [collection, key_path, match_value, output_path?]",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let collection_path = format!("{}.args[0]", base_path);
    let collection = match eval_expr(&args[0], record, context, out, &collection_path)? {
        EvalValue::Missing => return Ok(EvalValue::Missing),
        EvalValue::Value(value) => value,
    };
    let collection_array = match collection {
        JsonValue::Array(items) => items,
        JsonValue::Null => {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "lookup collection must be an array",
            )
            .with_path(collection_path))
        }
        _ => {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "lookup collection must be an array",
            )
            .with_path(collection_path))
        }
    };

    let key_path = literal_string(&args[1]).ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::ExprError,
            "lookup key_path must be a non-empty string literal",
        )
        .with_path(format!("{}.args[1]", base_path))
    })?;
    if key_path.is_empty() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "lookup key_path must be a non-empty string literal",
        )
        .with_path(format!("{}.args[1]", base_path)));
    }
    let key_tokens = parse_path(key_path).map_err(|_| {
        TransformError::new(TransformErrorKind::ExprError, "lookup key_path is invalid")
            .with_path(format!("{}.args[1]", base_path))
    })?;

    let output_tokens = if args.len() == 4 {
        let value = literal_string(&args[3]).ok_or_else(|| {
            TransformError::new(
                TransformErrorKind::ExprError,
                "lookup output_path must be a non-empty string literal",
            )
            .with_path(format!("{}.args[3]", base_path))
        })?;
        if value.is_empty() {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "lookup output_path must be a non-empty string literal",
            )
            .with_path(format!("{}.args[3]", base_path)));
        }
        let tokens = parse_path(value).map_err(|_| {
            TransformError::new(TransformErrorKind::ExprError, "lookup output_path is invalid")
                .with_path(format!("{}.args[3]", base_path))
        })?;
        Some(tokens)
    } else {
        None
    };

    let match_path = format!("{}.args[2]", base_path);
    let match_value = match eval_expr(&args[2], record, context, out, &match_path)? {
        EvalValue::Missing => return Ok(EvalValue::Missing),
        EvalValue::Value(value) => value,
    };
    if match_value.is_null() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "lookup match_value must not be null",
        )
        .with_path(match_path));
    }
    let match_key = value_to_string(&match_value, &match_path)?;

    let mut results = Vec::new();
    for item in &collection_array {
        let key_value = match get_path(item, &key_tokens) {
            Some(value) => value,
            None => continue,
        };
        let item_key = match value_to_string_optional(key_value) {
            Some(value) => value,
            None => continue,
        };
        if item_key != match_key {
            continue;
        }

        let selected = match output_tokens.as_ref() {
            Some(tokens) => get_path(item, tokens),
            None => Some(item),
        };

        if let Some(value) = selected {
            if first_only {
                return Ok(EvalValue::Value(value.clone()));
            }
            results.push(value.clone());
        }
    }

    if results.is_empty() {
        Ok(EvalValue::Missing)
    } else {
        Ok(EvalValue::Value(JsonValue::Array(results)))
    }
}

fn eval_bool_and_or(
    args: &[Expr],
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    is_and: bool,
) -> Result<EvalValue, TransformError> {
    if args.len() < 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain at least two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let mut saw_missing = false;
    for (index, arg) in args.iter().enumerate() {
        let arg_path = format!("{}.args[{}]", base_path, index);
        let value = eval_expr(arg, record, context, out, &arg_path)?;
        match value {
            EvalValue::Missing => {
                saw_missing = true;
                continue;
            }
            EvalValue::Value(value) => {
                let flag = value_as_bool(&value, &arg_path)?;
                if is_and {
                    if !flag {
                        return Ok(EvalValue::Value(JsonValue::Bool(false)));
                    }
                } else if flag {
                    return Ok(EvalValue::Value(JsonValue::Bool(true)));
                }
            }
        }
    }

    if saw_missing {
        Ok(EvalValue::Missing)
    } else {
        Ok(EvalValue::Value(JsonValue::Bool(is_and)))
    }
}

fn eval_bool_not(
    args: &[Expr],
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
) -> Result<EvalValue, TransformError> {
    if args.len() != 1 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly one item",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let arg_path = format!("{}.args[0]", base_path);
    let value = eval_expr(&args[0], record, context, out, &arg_path)?;
    match value {
        EvalValue::Missing => Ok(EvalValue::Missing),
        EvalValue::Value(value) => {
            let flag = value_as_bool(&value, &arg_path)?;
            Ok(EvalValue::Value(JsonValue::Bool(!flag)))
        }
    }
}

fn eval_compare(
    expr_op: &ExprOp,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
) -> Result<EvalValue, TransformError> {
    if expr_op.args.len() != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let left_path = format!("{}.args[0]", base_path);
    let right_path = format!("{}.args[1]", base_path);
    let left = eval_expr_value_or_null(&expr_op.args[0], record, context, out, &left_path)?;
    let right = eval_expr_value_or_null(&expr_op.args[1], record, context, out, &right_path)?;

    let result = match expr_op.op.as_str() {
        "==" => compare_eq(&left, &right, &left_path, &right_path)?,
        "!=" => !compare_eq(&left, &right, &left_path, &right_path)?,
        "<" => compare_numbers(&left, &right, &left_path, &right_path, |l, r| l < r)?,
        "<=" => compare_numbers(&left, &right, &left_path, &right_path, |l, r| l <= r)?,
        ">" => compare_numbers(&left, &right, &left_path, &right_path, |l, r| l > r)?,
        ">=" => compare_numbers(&left, &right, &left_path, &right_path, |l, r| l >= r)?,
        "~=" => match_regex(&left, &right, &left_path, &right_path)?,
        _ => {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "expr.op is not supported",
            )
            .with_path(format!("{}.op", base_path)))
        }
    };

    Ok(EvalValue::Value(JsonValue::Bool(result)))
}

fn eval_expr_value_or_null(
    expr: &Expr,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    path: &str,
) -> Result<JsonValue, TransformError> {
    match eval_expr(expr, record, context, out, path)? {
        EvalValue::Missing => Ok(JsonValue::Null),
        EvalValue::Value(value) => Ok(value),
    }
}

fn compare_eq(
    left: &JsonValue,
    right: &JsonValue,
    left_path: &str,
    right_path: &str,
) -> Result<bool, TransformError> {
    if left.is_null() || right.is_null() {
        return Ok(left.is_null() && right.is_null());
    }

    let left_value = value_to_string(left, left_path)?;
    let right_value = value_to_string(right, right_path)?;
    Ok(left_value == right_value)
}

fn compare_numbers<F>(
    left: &JsonValue,
    right: &JsonValue,
    left_path: &str,
    right_path: &str,
    compare: F,
) -> Result<bool, TransformError>
where
    F: FnOnce(f64, f64) -> bool,
{
    let left_value = value_to_number(left, left_path)?;
    let right_value = value_to_number(right, right_path)?;
    Ok(compare(left_value, right_value))
}

fn match_regex(
    left: &JsonValue,
    right: &JsonValue,
    left_path: &str,
    right_path: &str,
) -> Result<bool, TransformError> {
    let value = value_as_string(left, left_path)?;
    let pattern = value_as_string(right, right_path)?;
    let regex = regex::Regex::new(&pattern).map_err(|_| {
        TransformError::new(TransformErrorKind::ExprError, "regex pattern is invalid")
            .with_path(right_path)
    })?;
    Ok(regex.is_match(&value))
}

fn value_to_string(value: &JsonValue, path: &str) -> Result<String, TransformError> {
    match value {
        JsonValue::String(s) => Ok(s.clone()),
        JsonValue::Number(n) => Ok(number_to_string(n)),
        JsonValue::Bool(b) => Ok(b.to_string()),
        _ => Err(TransformError::new(
            TransformErrorKind::ExprError,
            "value must be string/number/bool",
        )
        .with_path(path)),
    }
}

fn value_as_string(value: &JsonValue, path: &str) -> Result<String, TransformError> {
    match value {
        JsonValue::String(s) => Ok(s.clone()),
        _ => Err(TransformError::new(
            TransformErrorKind::ExprError,
            "value must be a string",
        )
        .with_path(path)),
    }
}

fn value_as_bool(value: &JsonValue, path: &str) -> Result<bool, TransformError> {
    match value {
        JsonValue::Bool(flag) => Ok(*flag),
        _ => Err(expr_type_error("value must be a boolean", path)),
    }
}

fn value_to_number(value: &JsonValue, path: &str) -> Result<f64, TransformError> {
    match value {
        JsonValue::Number(n) => n
            .as_f64()
            .filter(|f| f.is_finite())
            .ok_or_else(|| expr_type_error("comparison operand must be a number", path)),
        JsonValue::String(s) => s
            .parse::<f64>()
            .ok()
            .filter(|f| f.is_finite())
            .ok_or_else(|| expr_type_error("comparison operand must be a number", path)),
        _ => Err(expr_type_error("comparison operand must be a number", path)),
    }
}

fn value_to_string_optional(value: &JsonValue) -> Option<String> {
    match value {
        JsonValue::String(s) => Some(s.clone()),
        JsonValue::Number(n) => Some(number_to_string(n)),
        JsonValue::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

fn expr_type_error(message: &str, path: &str) -> TransformError {
    TransformError::new(TransformErrorKind::ExprError, message).with_path(path)
}

fn number_to_string(number: &serde_json::Number) -> String {
    if let Some(i) = number.as_i64() {
        return i.to_string();
    }
    if let Some(u) = number.as_u64() {
        return u.to_string();
    }
    if let Some(f) = number.as_f64() {
        let mut s = format!("{}", f);
        if s.contains('.') {
            while s.ends_with('0') {
                s.pop();
            }
            if s.ends_with('.') {
                s.pop();
            }
        }
        return s;
    }
    number.to_string()
}

fn cast_value(
    value: &JsonValue,
    type_name: &str,
    path: &str,
) -> Result<JsonValue, TransformError> {
    match type_name {
        "string" => Ok(JsonValue::String(value_to_string(value, path)?)),
        "int" => cast_to_int(value, path),
        "float" => cast_to_float(value, path),
        "bool" => cast_to_bool(value, path),
        _ => Err(TransformError::new(
            TransformErrorKind::TypeCastFailed,
            "type must be string|int|float|bool",
        )
        .with_path(path)),
    }
}

fn cast_to_int(value: &JsonValue, path: &str) -> Result<JsonValue, TransformError> {
    match value {
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(JsonValue::Number(i.into()))
            } else if let Some(f) = n.as_f64() {
                if (f.fract()).abs() < f64::EPSILON {
                    Ok(JsonValue::Number((f as i64).into()))
                } else {
                    Err(type_cast_error("int", path))
                }
            } else {
                Err(type_cast_error("int", path))
            }
        }
        JsonValue::String(s) => s
            .parse::<i64>()
            .map(|i| JsonValue::Number(i.into()))
            .map_err(|_| type_cast_error("int", path)),
        _ => Err(type_cast_error("int", path)),
    }
}

fn cast_to_float(value: &JsonValue, path: &str) -> Result<JsonValue, TransformError> {
    match value {
        JsonValue::Number(n) => n
            .as_f64()
            .ok_or_else(|| type_cast_error("float", path))
            .and_then(|f| {
                serde_json::Number::from_f64(f)
                    .map(JsonValue::Number)
                    .ok_or_else(|| type_cast_error("float", path))
            }),
        JsonValue::String(s) => s
            .parse::<f64>()
            .map_err(|_| type_cast_error("float", path))
            .and_then(|f| {
                serde_json::Number::from_f64(f)
                    .map(JsonValue::Number)
                    .ok_or_else(|| type_cast_error("float", path))
            }),
        _ => Err(type_cast_error("float", path)),
    }
}

fn cast_to_bool(value: &JsonValue, path: &str) -> Result<JsonValue, TransformError> {
    match value {
        JsonValue::Bool(b) => Ok(JsonValue::Bool(*b)),
        JsonValue::String(s) => match s.to_lowercase().as_str() {
            "true" => Ok(JsonValue::Bool(true)),
            "false" => Ok(JsonValue::Bool(false)),
            _ => Err(type_cast_error("bool", path)),
        },
        _ => Err(type_cast_error("bool", path)),
    }
}

fn type_cast_error(type_name: &str, path: &str) -> TransformError {
    TransformError::new(
        TransformErrorKind::TypeCastFailed,
        format!("failed to cast to {}", type_name),
    )
    .with_path(path)
}

fn parse_source(source: &str) -> Result<(Namespace, &str), TransformError> {
    if let Some((prefix, path)) = source.split_once('.') {
        if path.is_empty() {
            return Err(TransformError::new(
                TransformErrorKind::InvalidRef,
                "reference path is empty",
            ));
        }
        let namespace = match prefix {
            "input" => Namespace::Input,
            "context" => Namespace::Context,
            "out" => Namespace::Out,
            _ => {
                return Err(TransformError::new(
                    TransformErrorKind::InvalidRef,
                    "ref namespace must be input|context|out",
                ))
            }
        };
        Ok((namespace, path))
    } else {
        if source.is_empty() {
            return Err(TransformError::new(
                TransformErrorKind::InvalidRef,
                "reference path is empty",
            ));
        }
        Ok((Namespace::Input, source))
    }
}

fn parse_ref(value: &str) -> Result<(Namespace, &str), TransformError> {
    let (prefix, path) = value.split_once('.').ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::InvalidRef,
            "ref must include namespace",
        )
    })?;

    if path.is_empty() {
        return Err(TransformError::new(
            TransformErrorKind::InvalidRef,
            "ref path is empty",
        ));
    }

    let namespace = match prefix {
        "input" => Namespace::Input,
        "context" => Namespace::Context,
        "out" => Namespace::Out,
        _ => {
            return Err(TransformError::new(
                TransformErrorKind::InvalidRef,
                "ref namespace must be input|context|out",
            ))
        }
    };

    Ok((namespace, path))
}

fn parse_path_tokens(
    path: &str,
    kind: TransformErrorKind,
    error_path: impl Into<String>,
) -> Result<Vec<PathToken>, TransformError> {
    parse_path(path).map_err(|err| {
        TransformError::new(kind, err.message()).with_path(error_path.into())
    })
}

fn set_path(
    root: &mut JsonValue,
    path: &str,
    value: JsonValue,
    mapping_path: &str,
) -> Result<(), TransformError> {
    let tokens = parse_path_tokens(
        path,
        TransformErrorKind::InvalidTarget,
        format!("{}.target", mapping_path),
    )?;
    if tokens.is_empty() {
        return Err(TransformError::new(
            TransformErrorKind::InvalidTarget,
            "target path is invalid",
        )
        .with_path(format!("{}.target", mapping_path)));
    }

    let mut current = root;
    for (index, token) in tokens.iter().enumerate() {
        let is_last = index == tokens.len() - 1;
        let key = match token {
            PathToken::Key(key) => key,
            PathToken::Index(_) => {
                return Err(TransformError::new(
                    TransformErrorKind::InvalidTarget,
                    "target path must not include indexes",
                )
                .with_path(format!("{}.target", mapping_path)))
            }
        };

        match current {
            JsonValue::Object(map) => {
                if is_last {
                    map.insert(key.to_string(), value);
                    return Ok(());
                }

                let entry = map.entry(key.to_string()).or_insert_with(|| {
                    JsonValue::Object(Map::new())
                });
                if !entry.is_object() {
                    return Err(TransformError::new(
                        TransformErrorKind::InvalidTarget,
                        "target path conflicts with non-object value",
                    )
                    .with_path(format!("{}.target", mapping_path)));
                }
                current = entry;
            }
            _ => {
                return Err(TransformError::new(
                    TransformErrorKind::InvalidTarget,
                    "target root must be an object",
                )
                .with_path(format!("{}.target", mapping_path)))
            }
        }
    }

    Ok(())
}

fn literal_string(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Literal(value) => value.as_str(),
        _ => None,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Namespace {
    Input,
    Context,
    Out,
}

#[derive(Debug, Clone, PartialEq)]
enum EvalValue {
    Missing,
    Value(JsonValue),
}
