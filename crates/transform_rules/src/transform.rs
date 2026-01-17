use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime};
use chrono::offset::TimeZone;
use csv::ReaderBuilder;
use regex::Regex;
use serde_json::{Map, Value as JsonValue};
use std::cmp::Ordering;
use std::collections::HashSet;
use std::sync::{Mutex, OnceLock};

use crate::cache::LruCache;
use crate::error::{TransformError, TransformErrorKind, TransformWarning};
use crate::model::{Expr, ExprChain, ExprOp, ExprRef, InputFormat, RuleFile};
use crate::path::{get_path, parse_path, PathToken};

const REGEX_CACHE_CAPACITY: usize = 128;

fn regex_cache() -> &'static Mutex<LruCache<String, Regex>> {
    static REGEX_CACHE: OnceLock<Mutex<LruCache<String, Regex>>> = OnceLock::new();
    REGEX_CACHE.get_or_init(|| Mutex::new(LruCache::new(REGEX_CACHE_CAPACITY)))
}

fn cached_regex(pattern: &str, path: &str) -> Result<Regex, TransformError> {
    let key = pattern.to_string();
    if let Some(regex) = {
        let mut cache = regex_cache().lock().unwrap_or_else(|err| err.into_inner());
        cache.get_cloned(&key)
    } {
        return Ok(regex);
    }

    let regex = Regex::new(pattern).map_err(|_| {
        TransformError::new(TransformErrorKind::ExprError, "regex pattern is invalid")
            .with_path(path)
    })?;
    {
        let mut cache = regex_cache().lock().unwrap_or_else(|err| err.into_inner());
        cache.insert(key, regex.clone());
    }
    Ok(regex)
}

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

#[derive(Debug)]
pub struct TransformStreamItem {
    pub output: Option<JsonValue>,
    pub warnings: Vec<TransformWarning>,
}

pub struct TransformStream<'a> {
    rule: &'a RuleFile,
    context: Option<&'a JsonValue>,
    records: InputRecordsIter<'a>,
    done: bool,
}

impl<'a> TransformStream<'a> {
    fn new(
        rule: &'a RuleFile,
        input: &'a str,
        context: Option<&'a JsonValue>,
    ) -> Result<Self, TransformError> {
        let records = input_records_iter(rule, input)?;
        Ok(Self {
            rule,
            context,
            records,
            done: false,
        })
    }
}

impl<'a> Iterator for TransformStream<'a> {
    type Item = Result<TransformStreamItem, TransformError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        loop {
            let record = match self.records.next() {
                None => {
                    self.done = true;
                    return None;
                }
                Some(Ok(record)) => record,
                Some(Err(err)) => {
                    self.done = true;
                    return Some(Err(err));
                }
            };

            let mut warnings = Vec::new();
            if !eval_record_when(self.rule, &record, self.context, &mut warnings) {
                if warnings.is_empty() {
                    continue;
                }
                return Some(Ok(TransformStreamItem {
                    output: None,
                    warnings,
                }));
            }

            match apply_mappings(self.rule, &record, self.context, &mut warnings) {
                Ok(output) => {
                    return Some(Ok(TransformStreamItem {
                        output: Some(output),
                        warnings,
                    }))
                }
                Err(err) => {
                    self.done = true;
                    return Some(Err(err));
                }
            }
        }
    }
}

pub fn transform_stream<'a>(
    rule: &'a RuleFile,
    input: &'a str,
    context: Option<&'a JsonValue>,
) -> Result<TransformStream<'a>, TransformError> {
    TransformStream::new(rule, input, context)
}

pub fn transform_with_warnings(
    rule: &RuleFile,
    input: &str,
    context: Option<&JsonValue>,
) -> Result<(JsonValue, Vec<TransformWarning>), TransformError> {
    let mut warnings = Vec::new();
    let mut output_records = Vec::new();
    let stream = transform_stream(rule, input, context)?;
    for item in stream {
        let item = item?;
        warnings.extend(item.warnings);
        if let Some(output) = item.output {
            output_records.push(output);
        }
    }

    Ok((JsonValue::Array(output_records), warnings))
}

pub fn preflight_validate_with_warnings(
    rule: &RuleFile,
    input: &str,
    context: Option<&JsonValue>,
) -> Result<Vec<TransformWarning>, TransformError> {
    let mut warnings = Vec::new();
    let stream = transform_stream(rule, input, context)?;
    for item in stream {
        let item = item?;
        warnings.extend(item.warnings);
    }
    Ok(warnings)
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

fn input_records_iter<'a>(
    rule: &RuleFile,
    input: &'a str,
) -> Result<InputRecordsIter<'a>, TransformError> {
    match rule.input.format {
        InputFormat::Csv => Ok(InputRecordsIter::Csv(CsvRecordIter::new(rule, input)?)),
        InputFormat::Json => Ok(InputRecordsIter::Json(JsonRecordIter::new(parse_json(
            rule, input,
        )?))),
    }
}

enum InputRecordsIter<'a> {
    Csv(CsvRecordIter<'a>),
    Json(JsonRecordIter),
}

impl<'a> Iterator for InputRecordsIter<'a> {
    type Item = Result<JsonValue, TransformError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            InputRecordsIter::Csv(iter) => iter.next(),
            InputRecordsIter::Json(iter) => iter.next(),
        }
    }
}

struct CsvRecordIter<'a> {
    reader: csv::Reader<&'a [u8]>,
    headers: Vec<String>,
    done: bool,
}

impl<'a> CsvRecordIter<'a> {
    fn new(rule: &RuleFile, input: &'a str) -> Result<Self, TransformError> {
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
            let header_record = reader.headers().map_err(|err| {
                TransformError::new(
                    TransformErrorKind::InvalidInput,
                    format!("failed to read csv header: {}", err),
                )
            })?;
            header_record.iter().map(|s| s.to_string()).collect()
        } else {
            let columns = csv_spec.columns.as_ref().ok_or_else(|| {
                TransformError::new(
                    TransformErrorKind::InvalidInput,
                    "csv.columns is required when has_header=false",
                )
            })?;
            columns.iter().map(|col| col.name.clone()).collect()
        };

        Ok(Self {
            reader,
            headers,
            done: false,
        })
    }
}

impl<'a> Iterator for CsvRecordIter<'a> {
    type Item = Result<JsonValue, TransformError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        let mut record = csv::StringRecord::new();
        match self.reader.read_record(&mut record) {
            Ok(has_data) => {
                if !has_data {
                    self.done = true;
                    return None;
                }
                let obj = record_to_object(&self.headers, &record);
                Some(Ok(JsonValue::Object(obj)))
            }
            Err(err) => {
                self.done = true;
                Some(Err(TransformError::new(
                    TransformErrorKind::InvalidInput,
                    format!("failed to read csv record: {}", err),
                )))
            }
        }
    }
}

struct JsonRecordIter {
    iter: std::vec::IntoIter<JsonValue>,
}

impl JsonRecordIter {
    fn new(records: Vec<JsonValue>) -> Self {
        Self {
            iter: records.into_iter(),
        }
    }
}

impl Iterator for JsonRecordIter {
    type Item = Result<JsonValue, TransformError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(Ok)
    }
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
        eval_expr(
            expr,
            record,
            context,
            out,
            &format!("{}.expr", mapping_path),
            None,
        )?
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

    let when_path = format!("{}.when", mapping_path);
    match eval_bool_expr(expr, record, context, out, &when_path) {
        Ok(flag) => flag,
        Err(err) => {
            warnings.push(err.into());
            false
        }
    }
}

fn eval_record_when(
    rule: &RuleFile,
    record: &JsonValue,
    context: Option<&JsonValue>,
    warnings: &mut Vec<TransformWarning>,
) -> bool {
    let expr = match &rule.record_when {
        Some(expr) => expr,
        None => return true,
    };

    let empty_out = JsonValue::Object(Map::new());
    match eval_bool_expr(expr, record, context, &empty_out, "record_when") {
        Ok(flag) => flag,
        Err(err) => {
            warnings.push(err.into());
            false
        }
    }
}

fn eval_bool_expr(
    expr: &Expr,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    path: &str,
) -> Result<bool, TransformError> {
    let value = eval_expr(expr, record, context, out, path, None)?;
    let value = match value {
        EvalValue::Missing => JsonValue::Null,
        EvalValue::Value(value) => value,
    };
    match value {
        JsonValue::Bool(flag) => Ok(flag),
        _ => Err(when_type_error(path)),
    }
}

fn when_type_error(path: &str) -> TransformError {
    TransformError::new(
        TransformErrorKind::ExprError,
        "when/record_when must evaluate to boolean",
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
        Namespace::Item | Namespace::Acc => {
            return Err(TransformError::new(
                TransformErrorKind::InvalidRef,
                "ref namespace must be input|context|out",
            )
            .with_path(format!("{}.source", mapping_path)))
        }
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
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    match expr {
        Expr::Literal(value) => Ok(EvalValue::Value(value.clone())),
        Expr::Ref(expr_ref) => eval_ref(expr_ref, record, context, out, base_path, locals),
        Expr::Op(expr_op) => eval_op(expr_op, record, context, out, base_path, None, locals),
        Expr::Chain(expr_chain) => eval_chain(expr_chain, record, context, out, base_path, locals),
    }
}

fn eval_chain(
    expr_chain: &ExprChain,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    if expr_chain.chain.is_empty() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.chain must be a non-empty array",
        )
        .with_path(format!("{}.chain", base_path)));
    }

    let first_path = format!("{}.chain[0]", base_path);
    let mut current =
        eval_expr(&expr_chain.chain[0], record, context, out, &first_path, locals)?;

    for (index, step) in expr_chain.chain.iter().enumerate().skip(1) {
        let step_path = format!("{}.chain[{}]", base_path, index);
        let expr_op = match step {
            Expr::Op(expr_op) => expr_op,
            _ => {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "expr.chain items after first must be op",
                )
                .with_path(step_path))
            }
        };

        let injected = current.clone();
        current = eval_op(
            expr_op,
            record,
            context,
            out,
            &step_path,
            Some(&injected),
            locals,
        )?;
    }

    Ok(current)
}

fn eval_ref(
    expr_ref: &ExprRef,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let (namespace, path) = parse_ref(&expr_ref.ref_path).map_err(|err| err.with_path(base_path))?;
    let tokens =
        parse_path_tokens(path, TransformErrorKind::InvalidRef, base_path.to_string())?;
    let target = match namespace {
        Namespace::Input => Some(record),
        Namespace::Context => context,
        Namespace::Out => Some(out),
        Namespace::Item => {
            let item = locals.and_then(|locals| locals.item).ok_or_else(|| {
                TransformError::new(
                    TransformErrorKind::ExprError,
                    "item is only available within array ops",
                )
                .with_path(base_path)
            })?;
            let (root, rest) = match tokens.split_first() {
                Some((PathToken::Key(key), rest)) if key == "value" => (item.value, rest),
                Some((PathToken::Key(key), rest)) if key == "index" => {
                    if !rest.is_empty() {
                        return Ok(EvalValue::Missing);
                    }
                    let value = JsonValue::Number(serde_json::Number::from(item.index as u64));
                    return Ok(EvalValue::Value(value));
                }
                _ => {
                    return Err(TransformError::new(
                        TransformErrorKind::ExprError,
                        "item ref must start with value or index",
                    )
                    .with_path(base_path))
                }
            };
            return match get_path(root, rest) {
                Some(value) => Ok(EvalValue::Value(value.clone())),
                None => Ok(EvalValue::Missing),
            };
        }
        Namespace::Acc => {
            let acc = locals.and_then(|locals| locals.acc).ok_or_else(|| {
                TransformError::new(
                    TransformErrorKind::ExprError,
                    "acc is only available within reduce/fold ops",
                )
                .with_path(base_path)
            })?;
            let (root, rest) = match tokens.split_first() {
                Some((PathToken::Key(key), rest)) if key == "value" => (acc, rest),
                _ => {
                    return Err(TransformError::new(
                        TransformErrorKind::ExprError,
                        "acc ref must start with value",
                    )
                    .with_path(base_path))
                }
            };
            return match get_path(root, rest) {
                Some(value) => Ok(EvalValue::Value(value.clone())),
                None => Ok(EvalValue::Missing),
            };
        }
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
    injected: Option<&EvalValue>,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(&expr_op.args, injected);
    if total_len == 0 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must be a non-empty array",
        )
        .with_path(format!("{}.args", base_path)));
    }

    match expr_op.op.as_str() {
        "concat" => {
            let mut parts = Vec::new();
            for index in 0..total_len {
                let arg_path = format!("{}.args[{}]", base_path, index);
                let value =
                    eval_expr_at_index(index, &expr_op.args, injected, record, context, out, base_path, locals)?;
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
            for index in 0..total_len {
                let value =
                    eval_expr_at_index(index, &expr_op.args, injected, record, context, out, base_path, locals)?;
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
            injected,
            record,
            context,
            out,
            base_path,
            locals,
            |value, path| value_to_string(value, path).map(JsonValue::String),
        ),
        "trim" => eval_unary_string_op(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
            |value, path| {
                let s = value_as_string(value, path)?;
                Ok(JsonValue::String(s.trim().to_string()))
            },
        ),
        "lowercase" => eval_unary_string_op(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
            |value, path| {
                let s = value_as_string(value, path)?;
                Ok(JsonValue::String(s.to_lowercase()))
            },
        ),
        "uppercase" => eval_unary_string_op(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
            |value, path| {
                let s = value_as_string(value, path)?;
                Ok(JsonValue::String(s.to_uppercase()))
            },
        ),
        "replace" => eval_replace(&expr_op.args, injected, record, context, out, base_path, locals),
        "split" => eval_split(&expr_op.args, injected, record, context, out, base_path, locals),
        "pad_start" => eval_pad(&expr_op.args, injected, record, context, out, base_path, true, locals),
        "pad_end" => eval_pad(&expr_op.args, injected, record, context, out, base_path, false, locals),
        "lookup" => eval_lookup(&expr_op.args, injected, record, context, out, base_path, false, locals),
        "lookup_first" => {
            eval_lookup(&expr_op.args, injected, record, context, out, base_path, true, locals)
        }
        "merge" => eval_json_merge(&expr_op.args, injected, record, context, out, base_path, false, locals),
        "deep_merge" => {
            eval_json_merge(&expr_op.args, injected, record, context, out, base_path, true, locals)
        }
        "get" => eval_json_get(&expr_op.args, injected, record, context, out, base_path, locals),
        "pick" => eval_json_pick(&expr_op.args, injected, record, context, out, base_path, locals),
        "omit" => eval_json_omit(&expr_op.args, injected, record, context, out, base_path, locals),
        "keys" => eval_json_keys(&expr_op.args, injected, record, context, out, base_path, locals),
        "values" => eval_json_values(&expr_op.args, injected, record, context, out, base_path, locals),
        "entries" => eval_json_entries(&expr_op.args, injected, record, context, out, base_path, locals),
        "object_flatten" => {
            eval_json_object_flatten(&expr_op.args, injected, record, context, out, base_path, locals)
        }
        "object_unflatten" => {
            eval_json_object_unflatten(&expr_op.args, injected, record, context, out, base_path, locals)
        }
        "map" => eval_array_map(&expr_op.args, injected, record, context, out, base_path, locals),
        "filter" => eval_array_filter(&expr_op.args, injected, record, context, out, base_path, locals),
        "flat_map" => eval_array_flat_map(&expr_op.args, injected, record, context, out, base_path, locals),
        "flatten" => eval_array_flatten(&expr_op.args, injected, record, context, out, base_path, locals),
        "take" => eval_array_take(&expr_op.args, injected, record, context, out, base_path, locals),
        "drop" => eval_array_drop(&expr_op.args, injected, record, context, out, base_path, locals),
        "slice" => eval_array_slice(&expr_op.args, injected, record, context, out, base_path, locals),
        "chunk" => eval_array_chunk(&expr_op.args, injected, record, context, out, base_path, locals),
        "zip" => eval_array_zip(&expr_op.args, injected, record, context, out, base_path, locals),
        "zip_with" => eval_array_zip_with(&expr_op.args, injected, record, context, out, base_path, locals),
        "unzip" => eval_array_unzip(&expr_op.args, injected, record, context, out, base_path, locals),
        "group_by" => eval_array_group_by(&expr_op.args, injected, record, context, out, base_path, locals),
        "key_by" => eval_array_key_by(&expr_op.args, injected, record, context, out, base_path, locals),
        "partition" => eval_array_partition(&expr_op.args, injected, record, context, out, base_path, locals),
        "unique" => eval_array_unique(&expr_op.args, injected, record, context, out, base_path, locals),
        "distinct_by" => eval_array_distinct_by(&expr_op.args, injected, record, context, out, base_path, locals),
        "sort_by" => eval_array_sort_by(&expr_op.args, injected, record, context, out, base_path, locals),
        "find" => eval_array_find(&expr_op.args, injected, record, context, out, base_path, locals),
        "find_index" => eval_array_find_index(&expr_op.args, injected, record, context, out, base_path, locals),
        "index_of" => eval_array_index_of(&expr_op.args, injected, record, context, out, base_path, locals),
        "contains" => eval_array_contains(&expr_op.args, injected, record, context, out, base_path, locals),
        "sum" => eval_array_sum(&expr_op.args, injected, record, context, out, base_path, locals),
        "avg" => eval_array_avg(&expr_op.args, injected, record, context, out, base_path, locals),
        "min" => eval_array_min(&expr_op.args, injected, record, context, out, base_path, locals),
        "max" => eval_array_max(&expr_op.args, injected, record, context, out, base_path, locals),
        "reduce" => eval_array_reduce(&expr_op.args, injected, record, context, out, base_path, locals),
        "fold" => eval_array_fold(&expr_op.args, injected, record, context, out, base_path, locals),
        "+" | "-" | "*" | "/" => eval_numeric_op(expr_op, injected, record, context, out, base_path, locals),
        "round" => eval_round(&expr_op.args, injected, record, context, out, base_path, locals),
        "to_base" => eval_to_base(&expr_op.args, injected, record, context, out, base_path, locals),
        "date_format" => eval_date_format(&expr_op.args, injected, record, context, out, base_path, locals),
        "to_unixtime" => {
            eval_to_unixtime(&expr_op.args, injected, record, context, out, base_path, locals)
        }
        "and" => eval_bool_and_or(&expr_op.args, injected, record, context, out, base_path, true, locals),
        "or" => eval_bool_and_or(&expr_op.args, injected, record, context, out, base_path, false, locals),
        "not" => eval_bool_not(&expr_op.args, injected, record, context, out, base_path, locals),
        "==" | "!=" | "<" | "<=" | ">" | ">=" | "~=" => {
            eval_compare(expr_op, injected, record, context, out, base_path, locals)
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
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
    op: F,
) -> Result<EvalValue, TransformError>
where
    F: FnOnce(&JsonValue, &str) -> Result<JsonValue, TransformError>,
{
    let total_len = args_len(args, injected);
    if total_len != 1 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly one item",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let arg_path = format!("{}.args[0]", base_path);
    let value =
        eval_expr_at_index(0, args, injected, record, context, out, base_path, locals)?;
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

fn args_len(args: &[Expr], injected: Option<&EvalValue>) -> usize {
    args.len() + usize::from(injected.is_some())
}

fn arg_expr_at<'a>(
    index: usize,
    args: &'a [Expr],
    injected: Option<&EvalValue>,
) -> Option<&'a Expr> {
    if injected.is_some() {
        if index == 0 {
            None
        } else {
            args.get(index - 1)
        }
    } else {
        args.get(index)
    }
}

fn eval_expr_at_index(
    index: usize,
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    if let Some(injected) = injected {
        if index == 0 {
            return Ok(injected.clone());
        }
        let arg = args.get(index - 1).ok_or_else(|| {
            TransformError::new(
                TransformErrorKind::ExprError,
                "expr.args index is out of bounds",
            )
            .with_path(format!("{}.args[{}]", base_path, index))
        })?;
        let arg_path = format!("{}.args[{}]", base_path, index);
        return eval_expr(arg, record, context, out, &arg_path, locals);
    }

    let arg = args.get(index).ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args index is out of bounds",
        )
        .with_path(format!("{}.args[{}]", base_path, index))
    })?;
    let arg_path = format!("{}.args[{}]", base_path, index);
    eval_expr(arg, record, context, out, &arg_path, locals)
}

fn eval_arg_value_at(
    index: usize,
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<Option<JsonValue>, TransformError> {
    match eval_expr_at_index(index, args, injected, record, context, out, base_path, locals)? {
        EvalValue::Missing => Ok(None),
        EvalValue::Value(value) => Ok(Some(value)),
    }
}

fn eval_arg_string_at(
    index: usize,
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<Option<String>, TransformError> {
    let value = match eval_arg_value_at(index, args, injected, record, context, out, base_path, locals)? {
        None => return Ok(None),
        Some(value) => value,
    };
    let arg_path = format!("{}.args[{}]", base_path, index);
    if value.is_null() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr arg must not be null",
        )
        .with_path(arg_path));
    }
    value_as_string(&value, &arg_path).map(Some)
}

fn eval_expr_value_or_null_at(
    index: usize,
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<JsonValue, TransformError> {
    match eval_expr_at_index(index, args, injected, record, context, out, base_path, locals)? {
        EvalValue::Missing => Ok(JsonValue::Null),
        EvalValue::Value(value) => Ok(value),
    }
}

#[derive(Clone, Copy)]
enum ReplaceMode {
    LiteralFirst,
    LiteralAll,
    RegexFirst,
    RegexAll,
}

fn parse_replace_mode(value: &str, path: &str) -> Result<ReplaceMode, TransformError> {
    match value {
        "all" => Ok(ReplaceMode::LiteralAll),
        "regex" => Ok(ReplaceMode::RegexFirst),
        "regex_all" => Ok(ReplaceMode::RegexAll),
        _ => Err(TransformError::new(
            TransformErrorKind::ExprError,
            "replace mode must be all|regex|regex_all",
        )
        .with_path(path)),
    }
}

fn eval_replace(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if !(3..=4).contains(&total_len) {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain three or four items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let value = match eval_arg_string_at(0, args, injected, record, context, out, base_path, locals)? {
        None => return Ok(EvalValue::Missing),
        Some(value) => value,
    };
    let pattern = match eval_arg_string_at(1, args, injected, record, context, out, base_path, locals)? {
        None => return Ok(EvalValue::Missing),
        Some(value) => value,
    };
    let replacement = match eval_arg_string_at(2, args, injected, record, context, out, base_path, locals)?
    {
        None => return Ok(EvalValue::Missing),
        Some(value) => value,
    };
    let pattern_path = format!("{}.args[1]", base_path);

    let mode = if total_len == 4 {
        let mode_path = format!("{}.args[3]", base_path);
        let mode_value = match eval_arg_string_at(3, args, injected, record, context, out, base_path, locals)?
        {
            None => return Ok(EvalValue::Missing),
            Some(value) => value,
        };
        parse_replace_mode(&mode_value, &mode_path)?
    } else {
        ReplaceMode::LiteralFirst
    };

    let replaced = match mode {
        ReplaceMode::LiteralFirst => value.replacen(&pattern, &replacement, 1),
        ReplaceMode::LiteralAll => value.replace(&pattern, &replacement),
        ReplaceMode::RegexFirst => {
            let regex = cached_regex(&pattern, &pattern_path)?;
            regex.replace(&value, replacement.as_str()).to_string()
        }
        ReplaceMode::RegexAll => {
            let regex = cached_regex(&pattern, &pattern_path)?;
            regex.replace_all(&value, replacement.as_str()).to_string()
        }
    };

    Ok(EvalValue::Value(JsonValue::String(replaced)))
}

fn eval_split(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let value = match eval_arg_string_at(0, args, injected, record, context, out, base_path, locals)? {
        None => return Ok(EvalValue::Missing),
        Some(value) => value,
    };
    let delimiter = match eval_arg_string_at(1, args, injected, record, context, out, base_path, locals)? {
        None => return Ok(EvalValue::Missing),
        Some(value) => value,
    };
    let delimiter_path = format!("{}.args[1]", base_path);

    if delimiter.is_empty() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "split delimiter must not be empty",
        )
        .with_path(delimiter_path));
    }

    let parts = value
        .split(&delimiter)
        .map(|part| JsonValue::String(part.to_string()))
        .collect::<Vec<_>>();

    Ok(EvalValue::Value(JsonValue::Array(parts)))
}

fn eval_pad(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    pad_start: bool,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if !(2..=3).contains(&total_len) {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain two or three items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let value = match eval_arg_string_at(0, args, injected, record, context, out, base_path, locals)? {
        None => return Ok(EvalValue::Missing),
        Some(value) => value,
    };

    let length_value = match eval_arg_value_at(1, args, injected, record, context, out, base_path, locals)?
    {
        None => return Ok(EvalValue::Missing),
        Some(value) => value,
    };
    let length_path = format!("{}.args[1]", base_path);
    if length_value.is_null() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr arg must not be null",
        )
        .with_path(length_path));
    }
    let length = value_to_i64(
        &length_value,
        &length_path,
        "pad length must be a non-negative integer",
    )?;
    if length < 0 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "pad length must be a non-negative integer",
        )
        .with_path(length_path));
    }

    let pad_string = if total_len == 3 {
        match eval_arg_string_at(2, args, injected, record, context, out, base_path, locals)? {
            None => return Ok(EvalValue::Missing),
            Some(value) => value,
        }
    } else {
        " ".to_string()
    };

    let target_len = usize::try_from(length).map_err(|_| {
        TransformError::new(
            TransformErrorKind::ExprError,
            "pad length must be a non-negative integer",
        )
        .with_path(length_path)
    })?;

    let padded = pad_string_value(&value, target_len, &pad_string, pad_start);
    Ok(EvalValue::Value(JsonValue::String(padded)))
}

fn pad_string_value(value: &str, target_len: usize, pad: &str, pad_start: bool) -> String {
    let value_len = value.chars().count();
    if value_len >= target_len || pad.is_empty() {
        return value.to_string();
    }

    let needed = target_len - value_len;
    let pad_len = pad.chars().count();
    let repeats = (needed + pad_len - 1) / pad_len;
    let pad_buf = pad.repeat(repeats);
    let pad_slice = pad_buf.chars().take(needed).collect::<String>();

    if pad_start {
        format!("{}{}", pad_slice, value)
    } else {
        format!("{}{}", value, pad_slice)
    }
}

fn eval_numeric_op(
    expr_op: &ExprOp,
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let op = expr_op.op.as_str();
    let args = &expr_op.args;
    let total_len = args_len(args, injected);

    let requires_exact_two = matches!(op, "-" | "/");
    if requires_exact_two && total_len != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly two items",
        )
        .with_path(format!("{}.args", base_path)));
    }
    if !requires_exact_two && total_len < 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain at least two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let mut result: f64 = 0.0;
    for index in 0..total_len {
        let arg_path = format!("{}.args[{}]", base_path, index);
        let value = match eval_arg_value_at(index, args, injected, record, context, out, base_path, locals)?
        {
            None => return Ok(EvalValue::Missing),
            Some(value) => value,
        };
        if value.is_null() {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "expr arg must not be null",
            )
            .with_path(arg_path));
        }
        let number = value_to_number(&value, &arg_path, "operand must be a number")?;
        if index == 0 {
            result = number;
        } else {
            result = match op {
                "+" => result + number,
                "-" => result - number,
                "*" => result * number,
                "/" => result / number,
                _ => result,
            };
        }
    }

    Ok(EvalValue::Value(json_number_from_f64(result, base_path)?))
}

fn eval_round(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if !(1..=2).contains(&total_len) {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain one or two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let value = match eval_arg_value_at(0, args, injected, record, context, out, base_path, locals)? {
        None => return Ok(EvalValue::Missing),
        Some(value) => value,
    };
    let value_path = format!("{}.args[0]", base_path);
    if value.is_null() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr arg must not be null",
        )
        .with_path(value_path));
    }
    let number = value_to_number(&value, &value_path, "operand must be a number")?;

    let scale = if total_len == 2 {
        let scale_path = format!("{}.args[1]", base_path);
        let scale_value =
            match eval_arg_value_at(1, args, injected, record, context, out, base_path, locals)? {
            None => return Ok(EvalValue::Missing),
            Some(value) => value,
        };
        if scale_value.is_null() {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "expr arg must not be null",
            )
            .with_path(scale_path));
        }
        let scale = value_to_i64(
            &scale_value,
            &scale_path,
            "scale must be a non-negative integer",
        )?;
        if scale < 0 {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "scale must be a non-negative integer",
            )
            .with_path(scale_path));
        }
        if scale > 308 {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "scale is too large",
            )
            .with_path(scale_path));
        }
        scale as i32
    } else {
        0
    };

    let rounded = if scale == 0 {
        number.round()
    } else {
        let factor = 10f64.powi(scale);
        (number * factor).round() / factor
    };

    Ok(EvalValue::Value(json_number_from_f64(rounded, base_path)?))
}

fn eval_to_base(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let value = match eval_arg_value_at(0, args, injected, record, context, out, base_path, locals)? {
        None => return Ok(EvalValue::Missing),
        Some(value) => value,
    };
    let base_value = match eval_arg_value_at(1, args, injected, record, context, out, base_path, locals)?
    {
        None => return Ok(EvalValue::Missing),
        Some(value) => value,
    };
    let value_path = format!("{}.args[0]", base_path);
    let base_path_arg = format!("{}.args[1]", base_path);
    if value.is_null() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr arg must not be null",
        )
        .with_path(value_path));
    }
    if base_value.is_null() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr arg must not be null",
        )
        .with_path(base_path_arg));
    }

    let number = value_to_i64(&value, &value_path, "value must be an integer")?;
    let base = value_to_i64(&base_value, &base_path_arg, "base must be an integer")?;
    if !(2..=36).contains(&base) {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "base must be between 2 and 36",
        )
        .with_path(base_path_arg));
    }

    let formatted = to_radix_string(number, base as u32, &value_path)?;
    Ok(EvalValue::Value(JsonValue::String(formatted)))
}

fn eval_date_format(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if !(2..=4).contains(&total_len) {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain two to four items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let value = match eval_arg_string_at(0, args, injected, record, context, out, base_path, locals)? {
        None => return Ok(EvalValue::Missing),
        Some(value) => value,
    };
    let output_format =
        match eval_arg_string_at(1, args, injected, record, context, out, base_path, locals)?
    {
        None => return Ok(EvalValue::Missing),
        Some(value) => value,
    };
    let value_path = format!("{}.args[0]", base_path);
    let mut input_formats: Option<Vec<String>> = None;
    let mut timezone: Option<FixedOffset> = None;

    if total_len >= 3 {
        let input_path = format!("{}.args[2]", base_path);
        let input_value =
            match eval_arg_value_at(2, args, injected, record, context, out, base_path, locals)? {
            None => return Ok(EvalValue::Missing),
            Some(value) => value,
        };
        if input_value.is_null() {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "expr arg must not be null",
            )
            .with_path(input_path));
        }

        if let Some(value) = input_value.as_str() {
            if looks_like_timezone(value) {
                timezone = Some(parse_timezone(value, &input_path)?);
            } else {
                input_formats = Some(parse_format_list(&input_value, &input_path)?);
            }
        } else {
            input_formats = Some(parse_format_list(&input_value, &input_path)?);
        }
    }

    if total_len == 4 {
        let tz_path = format!("{}.args[3]", base_path);
        let tz_value =
            match eval_arg_string_at(3, args, injected, record, context, out, base_path, locals)? {
            None => return Ok(EvalValue::Missing),
            Some(value) => value,
        };
        timezone = Some(parse_timezone(&tz_value, &tz_path)?);
    }

    let dt = parse_datetime(&value, input_formats.as_deref(), timezone, &value_path)?;
    let dt = match timezone {
        Some(offset) => dt.with_timezone(&offset),
        None => dt,
    };
    let formatted = dt.format(&output_format).to_string();
    Ok(EvalValue::Value(JsonValue::String(formatted)))
}

fn eval_to_unixtime(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if !(1..=3).contains(&total_len) {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain one to three items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let value = match eval_arg_string_at(0, args, injected, record, context, out, base_path, locals)? {
        None => return Ok(EvalValue::Missing),
        Some(value) => value,
    };
    let value_path = format!("{}.args[0]", base_path);

    let mut unit = "s".to_string();
    let mut timezone: Option<FixedOffset> = None;

    if total_len >= 2 {
        let arg_path = format!("{}.args[1]", base_path);
        let arg_value =
            match eval_arg_string_at(1, args, injected, record, context, out, base_path, locals)? {
            None => return Ok(EvalValue::Missing),
            Some(value) => value,
        };
        if total_len == 3 {
            if arg_value != "s" && arg_value != "ms" {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "unit must be s or ms",
                )
                .with_path(arg_path));
            }
            unit = arg_value;
        } else if arg_value == "s" || arg_value == "ms" {
            unit = arg_value;
        } else if looks_like_timezone(&arg_value) {
            timezone = Some(parse_timezone(&arg_value, &arg_path)?);
        } else {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "unit must be s or ms",
            )
            .with_path(arg_path));
        }
    }

    if total_len == 3 {
        let tz_path = format!("{}.args[2]", base_path);
        let tz_value =
            match eval_arg_string_at(2, args, injected, record, context, out, base_path, locals)? {
            None => return Ok(EvalValue::Missing),
            Some(value) => value,
        };
        timezone = Some(parse_timezone(&tz_value, &tz_path)?);
    }

    let dt = parse_datetime(&value, None, timezone, &value_path)?;
    let dt = match timezone {
        Some(offset) => dt.with_timezone(&offset),
        None => dt,
    };
    let timestamp = if unit == "ms" {
        dt.timestamp_millis()
    } else {
        dt.timestamp()
    };

    Ok(EvalValue::Value(JsonValue::Number(timestamp.into())))
}

fn eval_lookup(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    first_only: bool,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if !(3..=4).contains(&total_len) {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "lookup args must be [collection, key_path, match_value, output_path?]",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let collection_path = format!("{}.args[0]", base_path);
    let collection =
        match eval_expr_at_index(0, args, injected, record, context, out, base_path, locals)? {
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

    let key_expr = arg_expr_at(1, args, injected).ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::ExprError,
            "lookup key_path must be a non-empty string literal",
        )
        .with_path(format!("{}.args[1]", base_path))
    })?;
    let key_path = literal_string(key_expr).ok_or_else(|| {
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

    let output_tokens = if total_len == 4 {
        let output_expr = arg_expr_at(3, args, injected).ok_or_else(|| {
            TransformError::new(
                TransformErrorKind::ExprError,
                "lookup output_path must be a non-empty string literal",
            )
            .with_path(format!("{}.args[3]", base_path))
        })?;
        let value = literal_string(output_expr).ok_or_else(|| {
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
    let match_value =
        match eval_expr_at_index(2, args, injected, record, context, out, base_path, locals)? {
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

fn locals_with_item<'a>(
    locals: Option<&EvalLocals<'a>>,
    item: EvalItem<'a>,
) -> EvalLocals<'a> {
    EvalLocals {
        item: Some(item),
        acc: locals.and_then(|locals| locals.acc),
    }
}

fn eval_array_arg(
    index: usize,
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<Vec<JsonValue>, TransformError> {
    let arg_path = format!("{}.args[{}]", base_path, index);
    match eval_expr_at_index(index, args, injected, record, context, out, base_path, locals)? {
        EvalValue::Missing => Ok(Vec::new()),
        EvalValue::Value(value) => {
            if value.is_null() {
                Ok(Vec::new())
            } else if let JsonValue::Array(items) = value {
                Ok(items)
            } else {
                Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "expr arg must be an array",
                )
                .with_path(arg_path))
            }
        }
    }
}

fn eval_expr_or_null(
    expr: &Expr,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<JsonValue, TransformError> {
    match eval_expr(expr, record, context, out, base_path, locals)? {
        EvalValue::Missing => Ok(JsonValue::Null),
        EvalValue::Value(value) => Ok(value),
    }
}

fn eval_predicate_expr(
    expr: &Expr,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<bool, TransformError> {
    match eval_expr(expr, record, context, out, base_path, locals)? {
        EvalValue::Missing => Ok(false),
        EvalValue::Value(value) => {
            if value.is_null() {
                return Ok(false);
            }
            let flag = value_as_bool(&value, base_path)?;
            Ok(flag)
        }
    }
}

fn eval_key_expr_string(
    expr: &Expr,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<String, TransformError> {
    let value = match eval_expr(expr, record, context, out, base_path, locals)? {
        EvalValue::Missing => {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "expr arg must not be missing",
            )
            .with_path(base_path))
        }
        EvalValue::Value(value) => value,
    };
    if value.is_null() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr arg must not be null",
        )
        .with_path(base_path));
    }
    value_to_string(&value, base_path)
}

fn ensure_eq_compatible(value: &JsonValue, path: &str) -> Result<(), TransformError> {
    if value.is_null() {
        return Ok(());
    }
    if value_to_string_optional(value).is_some() {
        return Ok(());
    }
    Err(expr_type_error("value must be string/number/bool or null", path))
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SortKeyKind {
    Number,
    String,
    Bool,
}

#[derive(Clone)]
enum SortKey {
    Number(f64),
    String(String),
    Bool(bool),
}

impl SortKey {
    fn kind(&self) -> SortKeyKind {
        match self {
            SortKey::Number(_) => SortKeyKind::Number,
            SortKey::String(_) => SortKeyKind::String,
            SortKey::Bool(_) => SortKeyKind::Bool,
        }
    }
}

fn compare_sort_keys(left: &SortKey, right: &SortKey) -> Ordering {
    match (left, right) {
        (SortKey::Number(l), SortKey::Number(r)) => l.partial_cmp(r).unwrap_or(Ordering::Equal),
        (SortKey::String(l), SortKey::String(r)) => l.cmp(r),
        (SortKey::Bool(l), SortKey::Bool(r)) => l.cmp(r),
        _ => Ordering::Equal,
    }
}

fn eval_sort_key(
    expr: &Expr,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<SortKey, TransformError> {
    let value = match eval_expr(expr, record, context, out, base_path, locals)? {
        EvalValue::Missing => {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "expr arg must not be missing",
            )
            .with_path(base_path))
        }
        EvalValue::Value(value) => value,
    };
    if value.is_null() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr arg must not be null",
        )
        .with_path(base_path));
    }

    match value {
        JsonValue::Number(number) => {
            let value = number
                .as_f64()
                .filter(|value| value.is_finite())
                .ok_or_else(|| expr_type_error("sort_by key must be a finite number", base_path))?;
            Ok(SortKey::Number(value))
        }
        JsonValue::String(value) => Ok(SortKey::String(value)),
        JsonValue::Bool(value) => Ok(SortKey::Bool(value)),
        _ => Err(expr_type_error(
            "sort_by key must be string/number/bool",
            base_path,
        )),
    }
}

fn eval_array_map(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    let expr = arg_expr_at(1, args, injected).ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args index is out of bounds",
        )
        .with_path(format!("{}.args[1]", base_path))
    })?;
    let expr_index = if injected.is_some() { 0 } else { 1 };
    let expr_path = format!("{}.args[{}]", base_path, expr_index);

    let mut results = Vec::with_capacity(array.len());
    for (index, item) in array.iter().enumerate() {
        let item_locals = locals_with_item(locals, EvalItem { value: item, index });
        let value = eval_expr_or_null(expr, record, context, out, &expr_path, Some(&item_locals))?;
        results.push(value);
    }

    Ok(EvalValue::Value(JsonValue::Array(results)))
}

fn eval_array_filter(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    let expr = arg_expr_at(1, args, injected).ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args index is out of bounds",
        )
        .with_path(format!("{}.args[1]", base_path))
    })?;
    let expr_index = if injected.is_some() { 0 } else { 1 };
    let expr_path = format!("{}.args[{}]", base_path, expr_index);

    let mut results = Vec::new();
    for (index, item) in array.iter().enumerate() {
        let item_locals = locals_with_item(locals, EvalItem { value: item, index });
        if eval_predicate_expr(expr, record, context, out, &expr_path, Some(&item_locals))? {
            results.push(item.clone());
        }
    }

    Ok(EvalValue::Value(JsonValue::Array(results)))
}

fn eval_array_flat_map(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    let expr = arg_expr_at(1, args, injected).ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args index is out of bounds",
        )
        .with_path(format!("{}.args[1]", base_path))
    })?;
    let expr_index = if injected.is_some() { 0 } else { 1 };
    let expr_path = format!("{}.args[{}]", base_path, expr_index);

    let mut results = Vec::new();
    for (index, item) in array.iter().enumerate() {
        let item_locals = locals_with_item(locals, EvalItem { value: item, index });
        let value = eval_expr_or_null(expr, record, context, out, &expr_path, Some(&item_locals))?;
        match value {
            JsonValue::Array(items) => results.extend(items),
            value => results.push(value),
        }
    }

    Ok(EvalValue::Value(JsonValue::Array(results)))
}

fn flatten_value(value: &JsonValue, depth: usize, out: &mut Vec<JsonValue>) {
    if depth == 0 {
        out.push(value.clone());
        return;
    }

    if let JsonValue::Array(items) = value {
        for item in items {
            flatten_value(item, depth - 1, out);
        }
    } else {
        out.push(value.clone());
    }
}

fn eval_array_flatten(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if !(1..=2).contains(&total_len) {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain one or two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    let depth = if total_len == 2 {
        let depth_path = format!("{}.args[1]", base_path);
        let depth_value =
            match eval_arg_value_at(1, args, injected, record, context, out, base_path, locals)? {
                None => return Ok(EvalValue::Missing),
                Some(value) => value,
            };
        if depth_value.is_null() {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "expr arg must not be null",
            )
            .with_path(depth_path));
        }
        let depth =
            value_to_i64(&depth_value, &depth_path, "depth must be a non-negative integer")?;
        if depth < 0 {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "depth must be a non-negative integer",
            )
            .with_path(depth_path));
        }
        usize::try_from(depth).map_err(|_| {
            TransformError::new(
                TransformErrorKind::ExprError,
                "depth is too large",
            )
            .with_path(depth_path)
        })?
    } else {
        1
    };

    let mut results = Vec::new();
    for item in &array {
        flatten_value(item, depth, &mut results);
    }

    Ok(EvalValue::Value(JsonValue::Array(results)))
}

fn eval_array_take(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    let count_path = format!("{}.args[1]", base_path);
    let count_value =
        match eval_arg_value_at(1, args, injected, record, context, out, base_path, locals)? {
            None => return Ok(EvalValue::Missing),
            Some(value) => value,
        };
    if count_value.is_null() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr arg must not be null",
        )
        .with_path(count_path));
    }
    let count = value_to_i64(&count_value, &count_path, "count must be an integer")?;

    let len = array.len() as i64;
    let results = if count >= 0 {
        let take_count = count.min(len).max(0) as usize;
        array[..take_count].to_vec()
    } else {
        let abs_count = if count == i64::MIN {
            (i64::MAX as u64) + 1
        } else {
            (-count) as u64
        };
        let take_count = abs_count.min(array.len() as u64) as usize;
        let start = array.len().saturating_sub(take_count);
        array[start..].to_vec()
    };

    Ok(EvalValue::Value(JsonValue::Array(results)))
}

fn eval_array_drop(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    let count_path = format!("{}.args[1]", base_path);
    let count_value =
        match eval_arg_value_at(1, args, injected, record, context, out, base_path, locals)? {
            None => return Ok(EvalValue::Missing),
            Some(value) => value,
        };
    if count_value.is_null() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr arg must not be null",
        )
        .with_path(count_path));
    }
    let count = value_to_i64(&count_value, &count_path, "count must be an integer")?;

    let len = array.len() as i64;
    let results = if count >= 0 {
        let drop_count = count.min(len).max(0) as usize;
        array[drop_count..].to_vec()
    } else {
        let abs_count = if count == i64::MIN {
            (i64::MAX as u64) + 1
        } else {
            (-count) as u64
        };
        let drop_count = abs_count.min(array.len() as u64) as usize;
        let end = array.len().saturating_sub(drop_count);
        array[..end].to_vec()
    };

    Ok(EvalValue::Value(JsonValue::Array(results)))
}

fn eval_array_slice(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if !(2..=3).contains(&total_len) {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain two or three items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    let len = array.len() as i64;

    let start_path = format!("{}.args[1]", base_path);
    let start_value =
        match eval_arg_value_at(1, args, injected, record, context, out, base_path, locals)? {
            None => return Ok(EvalValue::Missing),
            Some(value) => value,
        };
    if start_value.is_null() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr arg must not be null",
        )
        .with_path(start_path));
    }
    let start = value_to_i64(&start_value, &start_path, "start must be an integer")?;

    let end = if total_len == 3 {
        let end_path = format!("{}.args[2]", base_path);
        let end_value =
            match eval_arg_value_at(2, args, injected, record, context, out, base_path, locals)? {
                None => return Ok(EvalValue::Missing),
                Some(value) => value,
            };
        if end_value.is_null() {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "expr arg must not be null",
            )
            .with_path(end_path));
        }
        value_to_i64(&end_value, &end_path, "end must be an integer")?
    } else {
        len
    };

    let mut start_index = if start < 0 { len + start } else { start };
    let mut end_index = if end < 0 { len + end } else { end };
    start_index = start_index.clamp(0, len);
    end_index = end_index.clamp(0, len);

    let results = if end_index <= start_index {
        Vec::new()
    } else {
        array[start_index as usize..end_index as usize].to_vec()
    };

    Ok(EvalValue::Value(JsonValue::Array(results)))
}

fn eval_array_chunk(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    let size_path = format!("{}.args[1]", base_path);
    let size_value =
        match eval_arg_value_at(1, args, injected, record, context, out, base_path, locals)? {
            None => return Ok(EvalValue::Missing),
            Some(value) => value,
        };
    if size_value.is_null() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr arg must not be null",
        )
        .with_path(size_path));
    }
    let size = value_to_i64(&size_value, &size_path, "size must be a positive integer")?;
    if size <= 0 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "size must be a positive integer",
        )
        .with_path(size_path));
    }
    let size = usize::try_from(size).map_err(|_| {
        TransformError::new(
            TransformErrorKind::ExprError,
            "size is too large",
        )
        .with_path(size_path)
    })?;

    let mut chunks = Vec::new();
    let mut index = 0;
    while index < array.len() {
        let end = (index + size).min(array.len());
        chunks.push(JsonValue::Array(array[index..end].to_vec()));
        index = end;
    }

    Ok(EvalValue::Value(JsonValue::Array(chunks)))
}

fn eval_array_zip(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len < 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain at least two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let mut arrays = Vec::new();
    for index in 0..total_len {
        arrays.push(eval_array_arg(index, args, injected, record, context, out, base_path, locals)?);
    }

    let min_len = arrays.iter().map(|items| items.len()).min().unwrap_or(0);
    let mut results = Vec::with_capacity(min_len);
    for idx in 0..min_len {
        let mut row = Vec::with_capacity(arrays.len());
        for array in &arrays {
            row.push(array[idx].clone());
        }
        results.push(JsonValue::Array(row));
    }

    Ok(EvalValue::Value(JsonValue::Array(results)))
}

fn eval_array_zip_with(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len < 3 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain at least three items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let expr_index = total_len - 1;
    let expr = arg_expr_at(expr_index, args, injected).ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args index is out of bounds",
        )
        .with_path(format!("{}.args[{}]", base_path, expr_index))
    })?;
    let expr_arg_index = if injected.is_some() {
        expr_index - 1
    } else {
        expr_index
    };
    let expr_path = format!("{}.args[{}]", base_path, expr_arg_index);

    let mut arrays = Vec::new();
    for index in 0..expr_index {
        arrays.push(eval_array_arg(index, args, injected, record, context, out, base_path, locals)?);
    }

    let min_len = arrays.iter().map(|items| items.len()).min().unwrap_or(0);
    let mut results = Vec::with_capacity(min_len);
    for idx in 0..min_len {
        let mut row = Vec::with_capacity(arrays.len());
        for array in &arrays {
            row.push(array[idx].clone());
        }
        let row_value = JsonValue::Array(row);
        let item_locals = locals_with_item(
            locals,
            EvalItem {
                value: &row_value,
                index: idx,
            },
        );
        let value = eval_expr_or_null(expr, record, context, out, &expr_path, Some(&item_locals))?;
        results.push(value);
    }

    Ok(EvalValue::Value(JsonValue::Array(results)))
}

fn eval_array_unzip(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 1 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly one item",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    if array.is_empty() {
        return Ok(EvalValue::Value(JsonValue::Array(Vec::new())));
    }

    let mut columns: Vec<Vec<JsonValue>> = Vec::new();
    let mut expected_len: Option<usize> = None;
    for item in &array {
        let items = match item {
            JsonValue::Array(items) => items,
            _ => {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "unzip items must be arrays",
                )
                .with_path(format!("{}.args[0]", base_path)))
            }
        };
        if let Some(expected) = expected_len {
            if items.len() != expected {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "unzip items must have the same length",
                )
                .with_path(format!("{}.args[0]", base_path)));
            }
        } else {
            expected_len = Some(items.len());
            columns = vec![Vec::with_capacity(array.len()); items.len()];
        }
        for (index, value) in items.iter().enumerate() {
            if let Some(column) = columns.get_mut(index) {
                column.push(value.clone());
            }
        }
    }

    let output = columns
        .into_iter()
        .map(JsonValue::Array)
        .collect::<Vec<_>>();
    Ok(EvalValue::Value(JsonValue::Array(output)))
}

fn eval_array_group_by(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    let expr = arg_expr_at(1, args, injected).ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args index is out of bounds",
        )
        .with_path(format!("{}.args[1]", base_path))
    })?;
    let expr_index = if injected.is_some() { 0 } else { 1 };
    let expr_path = format!("{}.args[{}]", base_path, expr_index);

    let mut results = Map::new();
    for (index, item) in array.iter().enumerate() {
        let item_locals = locals_with_item(locals, EvalItem { value: item, index });
        let key = eval_key_expr_string(expr, record, context, out, &expr_path, Some(&item_locals))?;
        let entry = results
            .entry(key)
            .or_insert_with(|| JsonValue::Array(Vec::new()));
        if let JsonValue::Array(items) = entry {
            items.push(item.clone());
        }
    }

    Ok(EvalValue::Value(JsonValue::Object(results)))
}

fn eval_array_key_by(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    let expr = arg_expr_at(1, args, injected).ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args index is out of bounds",
        )
        .with_path(format!("{}.args[1]", base_path))
    })?;
    let expr_index = if injected.is_some() { 0 } else { 1 };
    let expr_path = format!("{}.args[{}]", base_path, expr_index);

    let mut results = Map::new();
    for (index, item) in array.iter().enumerate() {
        let item_locals = locals_with_item(locals, EvalItem { value: item, index });
        let key = eval_key_expr_string(expr, record, context, out, &expr_path, Some(&item_locals))?;
        results.insert(key, item.clone());
    }

    Ok(EvalValue::Value(JsonValue::Object(results)))
}

fn eval_array_partition(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    let expr = arg_expr_at(1, args, injected).ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args index is out of bounds",
        )
        .with_path(format!("{}.args[1]", base_path))
    })?;
    let expr_index = if injected.is_some() { 0 } else { 1 };
    let expr_path = format!("{}.args[{}]", base_path, expr_index);

    let mut matched = Vec::new();
    let mut unmatched = Vec::new();
    for (index, item) in array.iter().enumerate() {
        let item_locals = locals_with_item(locals, EvalItem { value: item, index });
        if eval_predicate_expr(expr, record, context, out, &expr_path, Some(&item_locals))? {
            matched.push(item.clone());
        } else {
            unmatched.push(item.clone());
        }
    }

    Ok(EvalValue::Value(JsonValue::Array(vec![
        JsonValue::Array(matched),
        JsonValue::Array(unmatched),
    ])))
}

fn eval_array_unique(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 1 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly one item",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    let item_path = format!("{}.args[0]", base_path);

    let mut results: Vec<JsonValue> = Vec::new();
    for item in array {
        ensure_eq_compatible(&item, &item_path)?;
        let mut exists = false;
        for existing in &results {
            if compare_eq(&item, existing, &item_path, &item_path)? {
                exists = true;
                break;
            }
        }
        if !exists {
            results.push(item);
        }
    }

    Ok(EvalValue::Value(JsonValue::Array(results)))
}

fn eval_array_distinct_by(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    let expr = arg_expr_at(1, args, injected).ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args index is out of bounds",
        )
        .with_path(format!("{}.args[1]", base_path))
    })?;
    let expr_index = if injected.is_some() { 0 } else { 1 };
    let expr_path = format!("{}.args[{}]", base_path, expr_index);

    let mut results = Vec::new();
    let mut seen = HashSet::new();
    for (index, item) in array.iter().enumerate() {
        let item_locals = locals_with_item(locals, EvalItem { value: item, index });
        let key = eval_key_expr_string(expr, record, context, out, &expr_path, Some(&item_locals))?;
        if seen.insert(key) {
            results.push(item.clone());
        }
    }

    Ok(EvalValue::Value(JsonValue::Array(results)))
}

fn eval_array_sort_by(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if !(2..=3).contains(&total_len) {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain two or three items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    if array.is_empty() {
        return Ok(EvalValue::Value(JsonValue::Array(Vec::new())));
    }

    let expr = arg_expr_at(1, args, injected).ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args index is out of bounds",
        )
        .with_path(format!("{}.args[1]", base_path))
    })?;
    let expr_index = if injected.is_some() { 0 } else { 1 };
    let expr_path = format!("{}.args[{}]", base_path, expr_index);

    let order = if total_len == 3 {
        let order_path = format!("{}.args[2]", base_path);
        let value =
            match eval_arg_string_at(2, args, injected, record, context, out, base_path, locals)? {
                None => return Ok(EvalValue::Missing),
                Some(value) => value,
            };
        if value != "asc" && value != "desc" {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "order must be asc or desc",
            )
            .with_path(order_path));
        }
        value
    } else {
        "asc".to_string()
    };

    struct SortItem {
        key: SortKey,
        index: usize,
        value: JsonValue,
    }

    let mut items = Vec::with_capacity(array.len());
    let mut key_kind: Option<SortKeyKind> = None;
    for (index, item) in array.iter().enumerate() {
        let item_locals = locals_with_item(locals, EvalItem { value: item, index });
        let key = eval_sort_key(expr, record, context, out, &expr_path, Some(&item_locals))?;
        let kind = key.kind();
        if let Some(existing) = key_kind {
            if existing != kind {
                return Err(expr_type_error(
                    "sort_by keys must be all the same type",
                    &expr_path,
                ));
            }
        } else {
            key_kind = Some(kind);
        }
        items.push(SortItem {
            key,
            index,
            value: item.clone(),
        });
    }

    items.sort_by(|left, right| {
        let mut ordering = compare_sort_keys(&left.key, &right.key);
        if order == "desc" {
            ordering = ordering.reverse();
        }
        if ordering == Ordering::Equal {
            left.index.cmp(&right.index)
        } else {
            ordering
        }
    });

    let results = items
        .into_iter()
        .map(|item| item.value)
        .collect::<Vec<_>>();
    Ok(EvalValue::Value(JsonValue::Array(results)))
}

fn eval_array_find(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    let expr = arg_expr_at(1, args, injected).ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args index is out of bounds",
        )
        .with_path(format!("{}.args[1]", base_path))
    })?;
    let expr_index = if injected.is_some() { 0 } else { 1 };
    let expr_path = format!("{}.args[{}]", base_path, expr_index);

    for (index, item) in array.iter().enumerate() {
        let item_locals = locals_with_item(locals, EvalItem { value: item, index });
        if eval_predicate_expr(expr, record, context, out, &expr_path, Some(&item_locals))? {
            return Ok(EvalValue::Value(item.clone()));
        }
    }

    Ok(EvalValue::Value(JsonValue::Null))
}

fn eval_array_find_index(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    let expr = arg_expr_at(1, args, injected).ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args index is out of bounds",
        )
        .with_path(format!("{}.args[1]", base_path))
    })?;
    let expr_index = if injected.is_some() { 0 } else { 1 };
    let expr_path = format!("{}.args[{}]", base_path, expr_index);

    for (index, item) in array.iter().enumerate() {
        let item_locals = locals_with_item(locals, EvalItem { value: item, index });
        if eval_predicate_expr(expr, record, context, out, &expr_path, Some(&item_locals))? {
            return Ok(EvalValue::Value(JsonValue::Number((index as i64).into())));
        }
    }

    Ok(EvalValue::Value(JsonValue::Number((-1).into())))
}

fn eval_array_index_of(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    let value_path = format!("{}.args[1]", base_path);
    let value =
        eval_expr_value_or_null_at(1, args, injected, record, context, out, base_path, locals)?;

    ensure_eq_compatible(&value, &value_path)?;
    let item_path = format!("{}.args[0]", base_path);
    for (index, item) in array.iter().enumerate() {
        ensure_eq_compatible(item, &item_path)?;
        if compare_eq(item, &value, &item_path, &value_path)? {
            return Ok(EvalValue::Value(JsonValue::Number((index as i64).into())));
        }
    }

    Ok(EvalValue::Value(JsonValue::Number((-1).into())))
}

fn eval_array_contains(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    let value_path = format!("{}.args[1]", base_path);
    let value =
        eval_expr_value_or_null_at(1, args, injected, record, context, out, base_path, locals)?;

    ensure_eq_compatible(&value, &value_path)?;
    let item_path = format!("{}.args[0]", base_path);
    for item in &array {
        ensure_eq_compatible(item, &item_path)?;
        if compare_eq(item, &value, &item_path, &value_path)? {
            return Ok(EvalValue::Value(JsonValue::Bool(true)));
        }
    }

    Ok(EvalValue::Value(JsonValue::Bool(false)))
}

fn eval_array_sum(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 1 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly one item",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    if array.is_empty() {
        return Ok(EvalValue::Value(JsonValue::Null));
    }

    let item_path = format!("{}.args[0]", base_path);
    let mut sum = 0.0;
    for item in &array {
        let value = value_to_number(item, &item_path, "array item must be a number")?;
        sum += value;
    }

    Ok(EvalValue::Value(json_number_from_f64(sum, base_path)?))
}

fn eval_array_avg(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 1 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly one item",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    if array.is_empty() {
        return Ok(EvalValue::Value(JsonValue::Null));
    }

    let item_path = format!("{}.args[0]", base_path);
    let mut sum = 0.0;
    for item in &array {
        let value = value_to_number(item, &item_path, "array item must be a number")?;
        sum += value;
    }
    let avg = sum / array.len() as f64;

    Ok(EvalValue::Value(json_number_from_f64(avg, base_path)?))
}

fn eval_array_min(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 1 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly one item",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    if array.is_empty() {
        return Ok(EvalValue::Value(JsonValue::Null));
    }

    let item_path = format!("{}.args[0]", base_path);
    let mut min_value: Option<f64> = None;
    for item in &array {
        let value = value_to_number(item, &item_path, "array item must be a number")?;
        min_value = Some(match min_value {
            Some(current) => current.min(value),
            None => value,
        });
    }

    Ok(EvalValue::Value(json_number_from_f64(
        min_value.unwrap_or(0.0),
        base_path,
    )?))
}

fn eval_array_max(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 1 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly one item",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    if array.is_empty() {
        return Ok(EvalValue::Value(JsonValue::Null));
    }

    let item_path = format!("{}.args[0]", base_path);
    let mut max_value: Option<f64> = None;
    for item in &array {
        let value = value_to_number(item, &item_path, "array item must be a number")?;
        max_value = Some(match max_value {
            Some(current) => current.max(value),
            None => value,
        });
    }

    Ok(EvalValue::Value(json_number_from_f64(
        max_value.unwrap_or(0.0),
        base_path,
    )?))
}

fn eval_array_reduce(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    if array.is_empty() {
        return Ok(EvalValue::Value(JsonValue::Null));
    }

    let expr = arg_expr_at(1, args, injected).ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args index is out of bounds",
        )
        .with_path(format!("{}.args[1]", base_path))
    })?;
    let expr_index = if injected.is_some() { 0 } else { 1 };
    let expr_path = format!("{}.args[{}]", base_path, expr_index);

    let mut acc = array[0].clone();
    for (index, item) in array.iter().enumerate().skip(1) {
        let item_locals = EvalLocals {
            item: Some(EvalItem { value: item, index }),
            acc: Some(&acc),
        };
        let value = eval_expr_or_null(expr, record, context, out, &expr_path, Some(&item_locals))?;
        acc = value;
    }

    Ok(EvalValue::Value(acc))
}

fn eval_array_fold(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 3 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly three items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    let initial =
        match eval_expr_at_index(1, args, injected, record, context, out, base_path, locals)? {
            EvalValue::Missing => return Ok(EvalValue::Missing),
            EvalValue::Value(value) => value,
        };

    let expr = arg_expr_at(2, args, injected).ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args index is out of bounds",
        )
        .with_path(format!("{}.args[2]", base_path))
    })?;
    let expr_index = if injected.is_some() { 1 } else { 2 };
    let expr_path = format!("{}.args[{}]", base_path, expr_index);

    let mut acc = initial;
    for (index, item) in array.iter().enumerate() {
        let item_locals = EvalLocals {
            item: Some(EvalItem { value: item, index }),
            acc: Some(&acc),
        };
        let value = eval_expr_or_null(expr, record, context, out, &expr_path, Some(&item_locals))?;
        acc = value;
    }

    Ok(EvalValue::Value(acc))
}

fn eval_json_merge(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    deep: bool,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len < 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain at least two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let mut result: Option<Map<String, JsonValue>> = None;
    for index in 0..total_len {
        let arg_path = format!("{}.args[{}]", base_path, index);
        let value = eval_expr_at_index(index, args, injected, record, context, out, base_path, locals)?;
        let value = match value {
            EvalValue::Missing => continue,
            EvalValue::Value(value) => value,
        };
        if value.is_null() {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "expr arg must not be null",
            )
            .with_path(arg_path));
        }
        let obj = match value {
            JsonValue::Object(map) => map,
            _ => {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "expr arg must be object",
                )
                .with_path(arg_path))
            }
        };

        match result {
            Some(ref mut existing) => merge_object(existing, &obj, deep),
            None => result = Some(obj),
        }
    }

    match result {
        Some(map) => Ok(EvalValue::Value(JsonValue::Object(map))),
        None => Ok(EvalValue::Missing),
    }
}

fn eval_json_get(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let base_value =
        eval_expr_at_index(0, args, injected, record, context, out, base_path, locals)?;
    let base_value = match base_value {
        EvalValue::Missing => return Ok(EvalValue::Missing),
        EvalValue::Value(value) => value,
    };
    if base_value.is_null() {
        return Ok(EvalValue::Missing);
    }

    let path_path = format!("{}.args[1]", base_path);
    let path_value =
        eval_expr_at_index(1, args, injected, record, context, out, base_path, locals)?;
    let path_value = match path_value {
        EvalValue::Missing => return Ok(EvalValue::Missing),
        EvalValue::Value(value) => value,
    };
    if path_value.is_null() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr arg must not be null",
        )
        .with_path(path_path));
    }
    let path = value_as_string(&path_value, &path_path)?;
    if path.is_empty() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "path must be a non-empty string",
        )
        .with_path(path_path));
    }
    let tokens = parse_path_tokens(&path, TransformErrorKind::ExprError, &path_path)?;
    match get_path(&base_value, &tokens) {
        Some(value) => Ok(EvalValue::Value(value.clone())),
        None => Ok(EvalValue::Missing),
    }
}

fn eval_json_pick(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let base_path_arg = format!("{}.args[0]", base_path);
    let base_value =
        eval_expr_at_index(0, args, injected, record, context, out, base_path, locals)?;
    let base_value = match base_value {
        EvalValue::Missing => return Ok(EvalValue::Missing),
        EvalValue::Value(value) => value,
    };
    if base_value.is_null() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr arg must not be null",
        )
        .with_path(base_path_arg));
    }
    let base_obj = match base_value {
        JsonValue::Object(map) => map,
        _ => {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "expr arg must be object",
            )
            .with_path(base_path_arg))
        }
    };
    let base_value = JsonValue::Object(base_obj);

    let paths = eval_json_paths_arg(args, injected, record, context, out, base_path, locals, 1, true)?;
    let Some(paths) = paths else {
        return Ok(EvalValue::Missing);
    };

    let mut output = JsonValue::Object(Map::new());
    for tokens in paths {
        if let Some(value) = get_path(&base_value, &tokens) {
            set_path_with_indexes(&mut output, &tokens, value.clone(), base_path)?;
        }
    }

    Ok(EvalValue::Value(output))
}

fn eval_json_omit(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let base_path_arg = format!("{}.args[0]", base_path);
    let base_value =
        eval_expr_at_index(0, args, injected, record, context, out, base_path, locals)?;
    let mut base_value = match base_value {
        EvalValue::Missing => return Ok(EvalValue::Missing),
        EvalValue::Value(value) => value,
    };
    if base_value.is_null() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr arg must not be null",
        )
        .with_path(base_path_arg));
    }
    let base_obj = match base_value {
        JsonValue::Object(map) => map,
        _ => {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "expr arg must be object",
            )
            .with_path(base_path_arg))
        }
    };
    base_value = JsonValue::Object(base_obj);

    let paths = eval_json_paths_arg(args, injected, record, context, out, base_path, locals, 1, false)?;
    let Some(paths) = paths else {
        return Ok(EvalValue::Missing);
    };

    for tokens in paths {
        remove_path(&mut base_value, &tokens);
    }

    Ok(EvalValue::Value(base_value))
}

fn eval_json_keys(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    eval_json_object_unary(args, injected, record, context, out, base_path, locals, |map| {
        Ok(JsonValue::Array(
            map.keys().cloned().map(JsonValue::String).collect(),
        ))
    })
}

fn eval_json_values(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    eval_json_object_unary(args, injected, record, context, out, base_path, locals, |map| {
        Ok(JsonValue::Array(map.values().cloned().collect()))
    })
}

fn eval_json_entries(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    eval_json_object_unary(args, injected, record, context, out, base_path, locals, |map| {
        let mut entries = Vec::with_capacity(map.len());
        for (key, value) in map {
            let mut entry = Map::new();
            entry.insert("key".to_string(), JsonValue::String(key.clone()));
            entry.insert("value".to_string(), value.clone());
            entries.push(JsonValue::Object(entry));
        }
        Ok(JsonValue::Array(entries))
    })
}

fn eval_json_object_flatten(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    eval_json_object_unary(args, injected, record, context, out, base_path, locals, |map| {
        let mut output = Map::new();
        let mut tokens = Vec::new();
        flatten_object(map, &mut tokens, &mut output, base_path)?;
        Ok(JsonValue::Object(output))
    })
}

fn eval_json_object_unflatten(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    eval_json_object_unary(args, injected, record, context, out, base_path, locals, |map| {
        let mut paths = Vec::with_capacity(map.len());
        let mut values = Vec::with_capacity(map.len());
        for (key, value) in map {
            let tokens = parse_path_tokens(
                key,
                TransformErrorKind::ExprError,
                format!("{}.args[0]", base_path),
            )?;
            if tokens.iter().any(|token| matches!(token, PathToken::Index(_))) {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "array indexes are not allowed in path",
                )
                .with_path(format!("{}.args[0]", base_path)));
            }
            if has_path_conflict(&paths, &tokens) {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "path conflicts with another path",
                )
                .with_path(format!("{}.args[0]", base_path)));
            }
            paths.push(tokens);
            values.push(value.clone());
        }

        let mut root = JsonValue::Object(Map::new());
        for (tokens, value) in paths.into_iter().zip(values) {
            set_path_object_only(&mut root, &tokens, value, base_path)?;
        }

        Ok(root)
    })
}

fn eval_json_object_unary<F>(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
    op: F,
) -> Result<EvalValue, TransformError>
where
    F: FnOnce(&Map<String, JsonValue>) -> Result<JsonValue, TransformError>,
{
    let total_len = args_len(args, injected);
    if total_len != 1 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly one item",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let arg_path = format!("{}.args[0]", base_path);
    let value =
        eval_expr_at_index(0, args, injected, record, context, out, base_path, locals)?;
    let value = match value {
        EvalValue::Missing => return Ok(EvalValue::Missing),
        EvalValue::Value(value) => value,
    };
    if value.is_null() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr arg must not be null",
        )
        .with_path(arg_path));
    }
    let map = match value {
        JsonValue::Object(map) => map,
        _ => {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "expr arg must be object",
            )
            .with_path(arg_path))
        }
    };

    op(&map).map(EvalValue::Value)
}

fn eval_json_paths_arg(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
    index: usize,
    allow_terminal_index: bool,
) -> Result<Option<Vec<Vec<PathToken>>>, TransformError> {
    let arg_path = format!("{}.args[{}]", base_path, index);
    let value =
        eval_expr_at_index(index, args, injected, record, context, out, base_path, locals)?;
    let value = match value {
        EvalValue::Missing => return Ok(None),
        EvalValue::Value(value) => value,
    };
    if value.is_null() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr arg must not be null",
        )
        .with_path(arg_path));
    }
    let items: Vec<(String, String)> = match value {
        JsonValue::String(path) => vec![(arg_path.clone(), path)],
        JsonValue::Array(items) => items
            .iter()
            .enumerate()
            .map(|(path_index, item)| {
                let item_path = format!("{}.args[{}][{}]", base_path, index, path_index);
                let path = item.as_str().ok_or_else(|| {
                    TransformError::new(
                        TransformErrorKind::ExprError,
                        "paths must be a string or array of strings",
                    )
                    .with_path(&item_path)
                })?;
                Ok::<(String, String), TransformError>((item_path, path.to_string()))
            })
            .collect::<Result<Vec<_>, TransformError>>()?,
        _ => {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "paths must be a string or array of strings",
            )
            .with_path(arg_path))
        }
    };

    let mut paths = Vec::new();
    for (item_path, path) in items {
        let tokens = parse_path_tokens(&path, TransformErrorKind::ExprError, &item_path)?;
        if !allow_terminal_index && matches!(tokens.last(), Some(PathToken::Index(_))) {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "path must not end with array index",
            )
            .with_path(item_path));
        }
        if has_duplicate_path(&paths, &tokens) {
            continue;
        }
        if has_path_conflict(&paths, &tokens) {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "path conflicts with another path",
            )
            .with_path(item_path));
        }
        paths.push(tokens);
    }

    Ok(Some(paths))
}

fn has_duplicate_path(paths: &[Vec<PathToken>], tokens: &[PathToken]) -> bool {
    paths.iter().any(|existing| existing == tokens)
}

fn has_path_conflict(paths: &[Vec<PathToken>], tokens: &[PathToken]) -> bool {
    paths.iter().any(|existing| {
        is_path_prefix(existing, tokens) || is_path_prefix(tokens, existing)
    })
}

fn is_path_prefix(prefix: &[PathToken], tokens: &[PathToken]) -> bool {
    if prefix.len() > tokens.len() {
        return false;
    }
    prefix.iter().zip(tokens).all(|(left, right)| left == right)
}

fn merge_object(target: &mut Map<String, JsonValue>, incoming: &Map<String, JsonValue>, deep: bool) {
    for (key, value) in incoming {
        if deep {
            if let (Some(JsonValue::Object(target_obj)), JsonValue::Object(incoming_obj)) =
                (target.get_mut(key), value)
            {
                merge_object(target_obj, incoming_obj, true);
                continue;
            }
        }
        target.insert(key.clone(), value.clone());
    }
}

fn flatten_object(
    map: &Map<String, JsonValue>,
    tokens: &mut Vec<PathToken>,
    output: &mut Map<String, JsonValue>,
    base_path: &str,
) -> Result<(), TransformError> {
    for (key, value) in map {
        if key.is_empty() {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "object_flatten does not support empty keys",
            )
            .with_path(format!("{}.args[0]", base_path)));
        }
        if key.contains('[') || key.contains(']') {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "object_flatten does not support keys with '[' or ']'",
            )
            .with_path(format!("{}.args[0]", base_path)));
        }
        tokens.push(PathToken::Key(key.clone()));
        match value {
            JsonValue::Object(child) => {
                if child.is_empty() {
                    let path = format_path_tokens(tokens);
                    output.insert(path, JsonValue::Object(Map::new()));
                } else {
                    flatten_object(child, tokens, output, base_path)?;
                }
            }
            _ => {
                let path = format_path_tokens(tokens);
                output.insert(path, value.clone());
            }
        }
        tokens.pop();
    }
    Ok(())
}

fn format_path_tokens(tokens: &[PathToken]) -> String {
    let mut path = String::new();
    for token in tokens {
        match token {
            PathToken::Key(key) => {
                if needs_bracket_quote(key) {
                    let escaped = key.replace('\\', "\\\\").replace('"', "\\\"");
                    path.push('[');
                    path.push('"');
                    path.push_str(&escaped);
                    path.push('"');
                    path.push(']');
                } else {
                    if !path.is_empty() {
                        path.push('.');
                    }
                    path.push_str(key);
                }
            }
            PathToken::Index(index) => {
                path.push('[');
                path.push_str(&index.to_string());
                path.push(']');
            }
        }
    }
    path
}

fn needs_bracket_quote(key: &str) -> bool {
    key.contains('.')
}

fn set_path_object_only(
    root: &mut JsonValue,
    tokens: &[PathToken],
    value: JsonValue,
    base_path: &str,
) -> Result<(), TransformError> {
    if tokens.is_empty() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "path is empty",
        )
        .with_path(format!("{}.args[0]", base_path)));
    }

    let mut current = root;
    for (index, token) in tokens.iter().enumerate() {
        let key = match token {
            PathToken::Key(key) => key,
            PathToken::Index(_) => {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "array indexes are not allowed in path",
                )
                .with_path(format!("{}.args[0]", base_path)))
            }
        };
        let is_last = index == tokens.len() - 1;

        match current {
            JsonValue::Object(map) => {
                if is_last {
                    if map.contains_key(key) {
                        return Err(TransformError::new(
                            TransformErrorKind::ExprError,
                            "path conflicts with existing value",
                        )
                        .with_path(format!("{}.args[0]", base_path)));
                    }
                    map.insert(key.clone(), value);
                    return Ok(());
                }

                let entry = map.entry(key.clone()).or_insert_with(|| {
                    JsonValue::Object(Map::new())
                });
                if !entry.is_object() {
                    return Err(TransformError::new(
                        TransformErrorKind::ExprError,
                        "path conflicts with non-object value",
                    )
                    .with_path(format!("{}.args[0]", base_path)));
                }
                current = entry;
            }
            _ => {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "path conflicts with non-object value",
                )
                .with_path(format!("{}.args[0]", base_path)))
            }
        }
    }

    Ok(())
}

fn set_path_with_indexes(
    root: &mut JsonValue,
    tokens: &[PathToken],
    value: JsonValue,
    base_path: &str,
) -> Result<(), TransformError> {
    if tokens.is_empty() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "path is empty",
        )
        .with_path(format!("{}.args[1]", base_path)));
    }

    let mut current = root;
    for (index, token) in tokens.iter().enumerate() {
        let is_last = index == tokens.len() - 1;
        match token {
            PathToken::Key(key) => {
                let next_token = tokens.get(index + 1);
                match current {
                    JsonValue::Object(map) => {
                        if is_last {
                            map.insert(key.clone(), value);
                            return Ok(());
                        }
                        let entry = map.entry(key.clone()).or_insert_with(|| {
                            match next_token {
                                Some(PathToken::Index(_)) => JsonValue::Array(Vec::new()),
                                _ => JsonValue::Object(Map::new()),
                            }
                        });
                        let expect_index = matches!(next_token, Some(PathToken::Index(_)));
                        let entry_is_array = matches!(entry, JsonValue::Array(_));
                        let entry_is_object = matches!(entry, JsonValue::Object(_));
                        if !(expect_index && entry_is_array
                            || !expect_index && entry_is_object)
                        {
                            return Err(TransformError::new(
                                TransformErrorKind::ExprError,
                                "path conflicts with non-object value",
                            )
                            .with_path(format!("{}.args[1]", base_path)));
                        }
                        current = entry;
                    }
                    _ => {
                        return Err(TransformError::new(
                            TransformErrorKind::ExprError,
                            "path conflicts with non-object value",
                        )
                        .with_path(format!("{}.args[1]", base_path)))
                    }
                }
            }
            PathToken::Index(path_index) => {
                let next_token = tokens.get(index + 1);
                match current {
                    JsonValue::Array(items) => {
                        if items.len() <= *path_index {
                            items.resize_with(path_index + 1, || JsonValue::Null);
                        }
                        if is_last {
                            items[*path_index] = value;
                            return Ok(());
                        }
                        let entry = &mut items[*path_index];
                        if entry.is_null() {
                            *entry = match next_token {
                                Some(PathToken::Index(_)) => JsonValue::Array(Vec::new()),
                                _ => JsonValue::Object(Map::new()),
                            };
                        }
                        let expect_index = matches!(next_token, Some(PathToken::Index(_)));
                        let entry_is_array = matches!(entry, JsonValue::Array(_));
                        let entry_is_object = matches!(entry, JsonValue::Object(_));
                        if !(expect_index && entry_is_array
                            || !expect_index && entry_is_object)
                        {
                            return Err(TransformError::new(
                                TransformErrorKind::ExprError,
                                "path conflicts with non-object value",
                            )
                            .with_path(format!("{}.args[1]", base_path)));
                        }
                        current = entry;
                    }
                    _ => {
                        return Err(TransformError::new(
                            TransformErrorKind::ExprError,
                            "path conflicts with non-object value",
                        )
                        .with_path(format!("{}.args[1]", base_path)))
                    }
                }
            }
        }
    }

    Ok(())
}

fn remove_path(root: &mut JsonValue, tokens: &[PathToken]) {
    if tokens.is_empty() {
        return;
    }

    let (first, rest) = tokens.split_first().unwrap();
    match first {
        PathToken::Key(key) => {
            if let JsonValue::Object(map) = root {
                if rest.is_empty() {
                    map.remove(key);
                    return;
                }
                if let Some(next) = map.get_mut(key) {
                    remove_path(next, rest);
                }
            }
        }
        PathToken::Index(index) => {
            if let JsonValue::Array(items) = root {
                if let Some(next) = items.get_mut(*index) {
                    remove_path(next, rest);
                }
            }
        }
    }
}

fn eval_bool_and_or(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    is_and: bool,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len < 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain at least two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let mut saw_missing = false;
    for index in 0..total_len {
        let arg_path = format!("{}.args[{}]", base_path, index);
        let value = eval_expr_at_index(index, args, injected, record, context, out, base_path, locals)?;
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
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 1 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly one item",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let arg_path = format!("{}.args[0]", base_path);
    let value = eval_expr_at_index(0, args, injected, record, context, out, base_path, locals)?;
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
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(&expr_op.args, injected);
    if total_len != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let left_path = format!("{}.args[0]", base_path);
    let right_path = format!("{}.args[1]", base_path);
    let left = eval_expr_value_or_null_at(0, &expr_op.args, injected, record, context, out, base_path, locals)?;
    let right = eval_expr_value_or_null_at(1, &expr_op.args, injected, record, context, out, base_path, locals)?;

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
    let left_value = value_to_number(left, left_path, "comparison operand must be a number")?;
    let right_value = value_to_number(right, right_path, "comparison operand must be a number")?;
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
    let regex = cached_regex(&pattern, right_path)?;
    Ok(regex.is_match(&value))
}

const DEFAULT_DATE_FORMATS_WITH_TZ: [&str; 8] = [
    "%Y-%m-%dT%H:%M:%S%:z",
    "%Y-%m-%d %H:%M:%S%:z",
    "%Y-%m-%dT%H:%M:%S%.f%:z",
    "%Y-%m-%d %H:%M:%S%.f%:z",
    "%Y-%m-%dT%H:%M:%S%z",
    "%Y-%m-%d %H:%M:%S%z",
    "%Y/%m/%d %H:%M:%S%:z",
    "%Y/%m/%d %H:%M:%S%z",
];

const DEFAULT_DATE_FORMATS: [&str; 12] = [
    "%Y-%m-%d",
    "%Y/%m/%d",
    "%Y%m%d",
    "%Y-%m-%d %H:%M",
    "%Y/%m/%d %H:%M",
    "%Y-%m-%d %H:%M:%S",
    "%Y/%m/%d %H:%M:%S",
    "%Y-%m-%dT%H:%M",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%S%.f",
    "%Y-%m-%d %H:%M:%S%.f",
    "%Y/%m/%d %H:%M:%S%.f",
];

fn parse_format_list(value: &JsonValue, path: &str) -> Result<Vec<String>, TransformError> {
    match value {
        JsonValue::String(s) => {
            if s.is_empty() {
                Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "input_format must not be empty",
                )
                .with_path(path))
            } else {
                Ok(vec![s.clone()])
            }
        }
        JsonValue::Array(items) => {
            if items.is_empty() {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "input_format must not be empty",
                )
                .with_path(path));
            }
            let mut formats = Vec::with_capacity(items.len());
            for (index, item) in items.iter().enumerate() {
                let item_path = format!("{}[{}]", path, index);
                let value = match item.as_str() {
                    Some(value) => value,
                    None => {
                        return Err(TransformError::new(
                            TransformErrorKind::ExprError,
                            "input_format must be a string or array of strings",
                        )
                        .with_path(item_path))
                    }
                };
                if value.is_empty() {
                    return Err(TransformError::new(
                        TransformErrorKind::ExprError,
                        "input_format must not be empty",
                    )
                    .with_path(item_path));
                }
                formats.push(value.to_string());
            }
            Ok(formats)
        }
        _ => Err(TransformError::new(
            TransformErrorKind::ExprError,
            "input_format must be a string or array of strings",
        )
        .with_path(path)),
    }
}

fn parse_datetime(
    value: &str,
    formats: Option<&[String]>,
    timezone: Option<FixedOffset>,
    path: &str,
) -> Result<DateTime<FixedOffset>, TransformError> {
    if let Some(formats) = formats {
        return parse_datetime_with_formats(value, formats, timezone, path);
    }

    if let Ok(dt) = DateTime::parse_from_rfc3339(value) {
        return Ok(dt);
    }
    if let Ok(dt) = DateTime::parse_from_rfc2822(value) {
        return Ok(dt);
    }

    for format in DEFAULT_DATE_FORMATS_WITH_TZ {
        if let Ok(dt) = DateTime::parse_from_str(value, format) {
            return Ok(dt);
        }
    }

    parse_datetime_with_formats(
        value,
        &DEFAULT_DATE_FORMATS.iter().map(|f| f.to_string()).collect::<Vec<_>>(),
        timezone,
        path,
    )
}

fn parse_datetime_with_formats(
    value: &str,
    formats: &[String],
    timezone: Option<FixedOffset>,
    path: &str,
) -> Result<DateTime<FixedOffset>, TransformError> {
    for format in formats {
        if let Ok(dt) = DateTime::parse_from_str(value, format) {
            return Ok(dt);
        }
        if let Ok(naive) = NaiveDateTime::parse_from_str(value, format) {
            return apply_timezone(naive, timezone, path);
        }
        if let Ok(date) = NaiveDate::parse_from_str(value, format) {
            let naive = date
                .and_hms_opt(0, 0, 0)
                .ok_or_else(|| expr_type_error("date is invalid", path))?;
            return apply_timezone(naive, timezone, path);
        }
    }

    Err(TransformError::new(
        TransformErrorKind::ExprError,
        "date format is invalid",
    )
    .with_path(path))
}

fn apply_timezone(
    naive: NaiveDateTime,
    timezone: Option<FixedOffset>,
    path: &str,
) -> Result<DateTime<FixedOffset>, TransformError> {
    let offset = timezone.unwrap_or_else(|| FixedOffset::east_opt(0).unwrap());
    offset
        .from_local_datetime(&naive)
        .single()
        .ok_or_else(|| expr_type_error("date is invalid", path))
}

fn looks_like_timezone(value: &str) -> bool {
    if value.eq_ignore_ascii_case("utc") || value == "Z" {
        return true;
    }
    matches!(value.chars().next(), Some('+') | Some('-'))
}

fn parse_timezone(value: &str, path: &str) -> Result<FixedOffset, TransformError> {
    if value.eq_ignore_ascii_case("utc") || value == "Z" {
        return FixedOffset::east_opt(0).ok_or_else(|| {
            TransformError::new(
                TransformErrorKind::ExprError,
                "timezone must be UTC or an offset like +09:00",
            )
            .with_path(path)
        });
    }

    let (sign, rest) = match value.chars().next() {
        Some('+') => (1i32, &value[1..]),
        Some('-') => (-1i32, &value[1..]),
        _ => {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "timezone must be UTC or an offset like +09:00",
            )
            .with_path(path))
        }
    };

    let (hours, minutes) = if let Some((h, m)) = rest.split_once(':') {
        let hours = h.parse::<i32>().ok();
        let minutes = m.parse::<i32>().ok();
        match (hours, minutes) {
            (Some(hours), Some(minutes)) => (hours, minutes),
            _ => {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "timezone must be UTC or an offset like +09:00",
                )
                .with_path(path))
            }
        }
    } else {
        match rest.len() {
            2 => {
                let hours = rest.parse::<i32>().ok();
                match hours {
                    Some(hours) => (hours, 0),
                    None => {
                        return Err(TransformError::new(
                            TransformErrorKind::ExprError,
                            "timezone must be UTC or an offset like +09:00",
                        )
                        .with_path(path))
                    }
                }
            }
            4 => {
                let hours = rest[..2].parse::<i32>().ok();
                let minutes = rest[2..].parse::<i32>().ok();
                match (hours, minutes) {
                    (Some(hours), Some(minutes)) => (hours, minutes),
                    _ => {
                        return Err(TransformError::new(
                            TransformErrorKind::ExprError,
                            "timezone must be UTC or an offset like +09:00",
                        )
                        .with_path(path))
                    }
                }
            }
            _ => {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "timezone must be UTC or an offset like +09:00",
                )
                .with_path(path))
            }
        }
    };

    if !(0..=23).contains(&hours) || !(0..=59).contains(&minutes) {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "timezone must be UTC or an offset like +09:00",
        )
        .with_path(path));
    }

    let offset_seconds = sign * (hours * 3600 + minutes * 60);
    FixedOffset::east_opt(offset_seconds).ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::ExprError,
            "timezone must be UTC or an offset like +09:00",
        )
        .with_path(path)
    })
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

fn value_to_number(value: &JsonValue, path: &str, message: &str) -> Result<f64, TransformError> {
    match value {
        JsonValue::Number(n) => n
            .as_f64()
            .filter(|f| f.is_finite())
            .ok_or_else(|| expr_type_error(message, path)),
        JsonValue::String(s) => s
            .parse::<f64>()
            .ok()
            .filter(|f| f.is_finite())
            .ok_or_else(|| expr_type_error(message, path)),
        _ => Err(expr_type_error(message, path)),
    }
}

fn value_to_i64(value: &JsonValue, path: &str, message: &str) -> Result<i64, TransformError> {
    match value {
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(i)
            } else if let Some(u) = n.as_u64() {
                i64::try_from(u).map_err(|_| expr_type_error(message, path))
            } else if let Some(f) = n.as_f64() {
                if f.is_finite() && (f.fract()).abs() < f64::EPSILON {
                    let value = f as i64;
                    if (value as f64 - f).abs() < f64::EPSILON {
                        Ok(value)
                    } else {
                        Err(expr_type_error(message, path))
                    }
                } else {
                    Err(expr_type_error(message, path))
                }
            } else {
                Err(expr_type_error(message, path))
            }
        }
        JsonValue::String(s) => s
            .parse::<i64>()
            .map_err(|_| expr_type_error(message, path)),
        _ => Err(expr_type_error(message, path)),
    }
}

fn json_number_from_f64(value: f64, path: &str) -> Result<JsonValue, TransformError> {
    if !value.is_finite() {
        return Err(expr_type_error("number result is not finite", path));
    }
    if (value.fract()).abs() < f64::EPSILON {
        let as_i64 = value as i64;
        if (as_i64 as f64 - value).abs() < f64::EPSILON {
            return Ok(JsonValue::Number(as_i64.into()));
        }
    }
    serde_json::Number::from_f64(value)
        .map(JsonValue::Number)
        .ok_or_else(|| expr_type_error("number result is not finite", path))
}

fn to_radix_string(value: i64, base: u32, path: &str) -> Result<String, TransformError> {
    let digits = b"0123456789abcdefghijklmnopqrstuvwxyz";
    if base < 2 || base > 36 {
        return Err(expr_type_error("base must be between 2 and 36", path));
    }

    if value == 0 {
        return Ok("0".to_string());
    }

    let is_negative = value < 0;
    let mut n = value.checked_abs().ok_or_else(|| {
        expr_type_error("value is out of range for base conversion", path)
    })? as u64;

    let mut buf = Vec::new();
    while n > 0 {
        let idx = (n % base as u64) as usize;
        buf.push(digits[idx] as char);
        n /= base as u64;
    }
    if is_negative {
        buf.push('-');
    }
    buf.reverse();
    Ok(buf.iter().collect())
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
        "item" => Namespace::Item,
        "acc" => Namespace::Acc,
        _ => {
            return Err(TransformError::new(
                TransformErrorKind::InvalidRef,
                "ref namespace must be input|context|out|item|acc",
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
    Item,
    Acc,
}

#[derive(Clone, Copy)]
struct EvalItem<'a> {
    value: &'a JsonValue,
    index: usize,
}

#[derive(Clone, Copy)]
struct EvalLocals<'a> {
    item: Option<EvalItem<'a>>,
    acc: Option<&'a JsonValue>,
}

#[derive(Debug, Clone, PartialEq)]
enum EvalValue {
    Missing,
    Value(JsonValue),
}
