use std::collections::HashSet;

use crate::error::{ErrorCode, RuleError, ValidationResult};
use crate::locator::YamlLocator;
use crate::model::{Expr, ExprChain, ExprOp, ExprRef, InputFormat, Mapping, RuleFile};
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
    validate_record_when(rule, &mut ctx);
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

fn validate_record_when(rule: &RuleFile, ctx: &mut ValidationCtx<'_>) {
    let expr = match rule.record_when.as_ref() {
        Some(expr) => expr,
        None => return,
    };

    let base_path = "record_when";
    let produced_targets = HashSet::new();
    validate_expr(expr, base_path, &produced_targets, ctx, LocalScope::None);
    validate_when_expr(expr, base_path, ctx);
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
            validate_expr(expr, &expr_path, &produced_targets, ctx, LocalScope::None);
        }

        if let Some(when) = &mapping.when {
            let when_path = format!("{}.when", base);
            validate_expr(when, &when_path, &produced_targets, ctx, LocalScope::None);
            validate_when_expr(when, &when_path, ctx);
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
    scope: LocalScope,
) {
    match expr {
        Expr::Ref(expr_ref) => validate_ref(expr_ref, base_path, produced_targets, ctx, scope),
        Expr::Op(expr_op) => validate_op(expr_op, base_path, produced_targets, ctx, scope),
        Expr::Chain(expr_chain) => {
            validate_chain(expr_chain, base_path, produced_targets, ctx, scope)
        }
        Expr::Literal(_) => {}
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum LocalScope {
    None,
    Item,
    ItemAcc,
}

impl LocalScope {
    fn allows_item(self) -> bool {
        matches!(self, LocalScope::Item | LocalScope::ItemAcc)
    }

    fn allows_acc(self) -> bool {
        matches!(self, LocalScope::ItemAcc)
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum BoolExprKind {
    Bool,
    Maybe,
    NotBool,
}

fn validate_when_expr(expr: &Expr, base_path: &str, ctx: &mut ValidationCtx<'_>) {
    if matches!(bool_expr_kind(expr), BoolExprKind::NotBool) {
        ctx.push(
            ErrorCode::InvalidWhenType,
            "when/record_when must evaluate to boolean",
            base_path,
        );
    }
}

fn bool_expr_kind(expr: &Expr) -> BoolExprKind {
    match expr {
        Expr::Literal(value) => {
            if value.is_boolean() {
                BoolExprKind::Bool
            } else {
                BoolExprKind::NotBool
            }
        }
        Expr::Ref(_) => BoolExprKind::Maybe,
        Expr::Op(expr_op) => match expr_op.op.as_str() {
            "concat"
            | "to_string"
            | "trim"
            | "lowercase"
            | "uppercase"
            | "replace"
            | "split"
            | "pad_start"
            | "pad_end"
            | "lookup"
            | "lookup_first"
            | "merge"
            | "deep_merge"
            | "get"
            | "pick"
            | "omit"
            | "keys"
            | "values"
            | "entries"
            | "object_flatten"
            | "object_unflatten"
            | "map"
            | "filter"
            | "flat_map"
            | "flatten"
            | "take"
            | "drop"
            | "slice"
            | "chunk"
            | "zip"
            | "zip_with"
            | "unzip"
            | "group_by"
            | "key_by"
            | "partition"
            | "unique"
            | "distinct_by"
            | "sort_by"
            | "find_index"
            | "index_of"
            | "sum"
            | "avg"
            | "min"
            | "max"
            | "+"
            | "-"
            | "*"
            | "/"
            | "round"
            | "to_base"
            | "date_format"
            | "to_unixtime" => BoolExprKind::NotBool,
            "and" | "or" | "not" | "contains" => BoolExprKind::Bool,
            "==" | "!=" | "<" | "<=" | ">" | ">=" | "~=" => BoolExprKind::Bool,
            "coalesce" => {
                let mut saw_maybe = false;
                for arg in &expr_op.args {
                    match bool_expr_kind(arg) {
                        BoolExprKind::Bool => {}
                        BoolExprKind::Maybe => saw_maybe = true,
                        BoolExprKind::NotBool => return BoolExprKind::NotBool,
                    }
                }
                if saw_maybe {
                    BoolExprKind::Maybe
                } else {
                    BoolExprKind::Bool
                }
            }
            _ => BoolExprKind::Maybe,
        },
        Expr::Chain(expr_chain) => bool_expr_kind_chain(expr_chain),
    }
}

fn bool_expr_kind_chain(expr_chain: &ExprChain) -> BoolExprKind {
    if expr_chain.chain.is_empty() {
        return BoolExprKind::NotBool;
    }

    let mut current = bool_expr_kind(&expr_chain.chain[0]);
    for step in expr_chain.chain.iter().skip(1) {
        let expr_op = match step {
            Expr::Op(expr_op) => expr_op,
            _ => return BoolExprKind::Maybe,
        };
        current = bool_expr_kind_for_op_with_input(expr_op, current);
    }
    current
}

fn bool_expr_kind_for_op_with_input(expr_op: &ExprOp, injected: BoolExprKind) -> BoolExprKind {
    match expr_op.op.as_str() {
            "concat"
            | "to_string"
            | "trim"
            | "lowercase"
            | "uppercase"
            | "replace"
            | "split"
            | "pad_start"
            | "pad_end"
            | "lookup"
            | "lookup_first"
            | "merge"
            | "deep_merge"
            | "get"
            | "pick"
            | "omit"
            | "keys"
            | "values"
            | "entries"
            | "object_flatten"
            | "object_unflatten"
            | "map"
            | "filter"
            | "flat_map"
            | "flatten"
            | "take"
            | "drop"
            | "slice"
            | "chunk"
            | "zip"
            | "zip_with"
            | "unzip"
            | "group_by"
            | "key_by"
            | "partition"
            | "unique"
            | "distinct_by"
            | "sort_by"
            | "find_index"
            | "index_of"
            | "sum"
            | "avg"
            | "min"
            | "max"
            | "+"
            | "-"
            | "*"
            | "/"
            | "round"
            | "to_base"
            | "date_format"
            | "to_unixtime" => BoolExprKind::NotBool,
            "and" | "or" | "not" | "contains" => BoolExprKind::Bool,
            "==" | "!=" | "<" | "<=" | ">" | ">=" | "~=" => BoolExprKind::Bool,
            "coalesce" => {
            let mut saw_maybe = matches!(injected, BoolExprKind::Maybe);
            if matches!(injected, BoolExprKind::NotBool) {
                return BoolExprKind::NotBool;
            }
            for arg in &expr_op.args {
                match bool_expr_kind(arg) {
                    BoolExprKind::Bool => {}
                    BoolExprKind::Maybe => saw_maybe = true,
                    BoolExprKind::NotBool => return BoolExprKind::NotBool,
                }
            }
            if saw_maybe {
                BoolExprKind::Maybe
            } else {
                BoolExprKind::Bool
            }
        }
        _ => BoolExprKind::Maybe,
    }
}

fn validate_chain(
    expr_chain: &ExprChain,
    base_path: &str,
    produced_targets: &HashSet<Vec<PathToken>>,
    ctx: &mut ValidationCtx<'_>,
    scope: LocalScope,
) {
    if expr_chain.chain.is_empty() {
        ctx.push(
            ErrorCode::InvalidExprShape,
            "expr.chain must be a non-empty array",
            format!("{}.chain", base_path),
        );
        return;
    }

    for (index, item) in expr_chain.chain.iter().enumerate() {
        let item_path = format!("{}.chain[{}]", base_path, index);
        if index == 0 {
            validate_expr(item, &item_path, produced_targets, ctx, scope);
            continue;
        }

        match item {
            Expr::Op(expr_op) => {
                validate_chain_op(expr_op, &item_path, produced_targets, ctx, scope);
            }
            _ => {
                ctx.push(
                    ErrorCode::InvalidExprShape,
                    "expr.chain items after first must be op",
                    item_path,
                );
            }
        }
    }
}

fn validate_chain_op(
    expr_op: &ExprOp,
    base_path: &str,
    produced_targets: &HashSet<Vec<PathToken>>,
    ctx: &mut ValidationCtx<'_>,
    scope: LocalScope,
) {
    if !is_valid_op(&expr_op.op) {
        ctx.push(
            ErrorCode::UnknownOp,
            "expr.op is not supported",
            format!("{}.op", base_path),
        );
    }

    let args_len = expr_op.args.len() + 1;
    match expr_op.op.as_str() {
        "trim" | "lowercase" | "uppercase" | "to_string" | "not" => {
            if args_len != 1 {
                ctx.push(
                    ErrorCode::InvalidArgs,
                    "expr.args must contain exactly one item",
                    format!("{}.args", base_path),
                );
            }
        }
        "replace" => {
            if !(3..=4).contains(&args_len) {
                ctx.push(
                    ErrorCode::InvalidArgs,
                    "expr.args must contain three or four items",
                    format!("{}.args", base_path),
                );
            }
        }
        "split" => {
            if args_len != 2 {
                ctx.push(
                    ErrorCode::InvalidArgs,
                    "expr.args must contain exactly two items",
                    format!("{}.args", base_path),
                );
            }
        }
        "pad_start" | "pad_end" => {
            if !(2..=3).contains(&args_len) {
                ctx.push(
                    ErrorCode::InvalidArgs,
                    "expr.args must contain two or three items",
                    format!("{}.args", base_path),
                );
            }
        }
        "lookup" | "lookup_first" => {
            validate_lookup_args_chain(expr_op, base_path, ctx);
        }
        "merge" | "deep_merge" => {
            if args_len < 2 {
                ctx.push(
                    ErrorCode::InvalidArgs,
                    "expr.args must contain at least two items",
                    format!("{}.args", base_path),
                );
            }
        }
        "get" => {
            if args_len != 2 {
                ctx.push(
                    ErrorCode::InvalidArgs,
                    "expr.args must contain exactly two items",
                    format!("{}.args", base_path),
                );
            } else {
                validate_path_arg(&expr_op.args[0], &format!("{}.args[0]", base_path), ctx);
            }
        }
        "pick" | "omit" => {
            if args_len != 2 {
                ctx.push(
                    ErrorCode::InvalidArgs,
                    "expr.args must contain exactly two items",
                    format!("{}.args", base_path),
                );
            } else {
                let allow_terminal_index = expr_op.op == "pick";
                validate_path_array_arg(
                    &expr_op.args[0],
                    &format!("{}.args[0]", base_path),
                    allow_terminal_index,
                    ctx,
                );
            }
        }
        "keys" | "values" | "entries" | "object_flatten" | "object_unflatten" => {
            if args_len != 1 {
                ctx.push(
                    ErrorCode::InvalidArgs,
                    "expr.args must contain exactly one item",
                    format!("{}.args", base_path),
                );
            }
        }
        "map"
        | "filter"
        | "flat_map"
        | "group_by"
        | "key_by"
        | "partition"
        | "distinct_by"
        | "find"
        | "find_index" => {
            if args_len != 2 {
                ctx.push(
                    ErrorCode::InvalidArgs,
                    "expr.args must contain exactly two items",
                    format!("{}.args", base_path),
                );
            }
        }
        "flatten" => {
            if !(1..=2).contains(&args_len) {
                ctx.push(
                    ErrorCode::InvalidArgs,
                    "expr.args must contain one or two items",
                    format!("{}.args", base_path),
                );
            }
        }
        "take" | "drop" | "chunk" | "index_of" | "contains" | "reduce" => {
            if args_len != 2 {
                ctx.push(
                    ErrorCode::InvalidArgs,
                    "expr.args must contain exactly two items",
                    format!("{}.args", base_path),
                );
            }
        }
        "slice" => {
            if !(2..=3).contains(&args_len) {
                ctx.push(
                    ErrorCode::InvalidArgs,
                    "expr.args must contain two or three items",
                    format!("{}.args", base_path),
                );
            }
        }
        "zip" => {
            if args_len < 2 {
                ctx.push(
                    ErrorCode::InvalidArgs,
                    "expr.args must contain at least two items",
                    format!("{}.args", base_path),
                );
            }
        }
        "zip_with" => {
            if args_len < 3 {
                ctx.push(
                    ErrorCode::InvalidArgs,
                    "expr.args must contain at least three items",
                    format!("{}.args", base_path),
                );
            }
        }
        "unzip" | "unique" | "sum" | "avg" | "min" | "max" => {
            if args_len != 1 {
                ctx.push(
                    ErrorCode::InvalidArgs,
                    "expr.args must contain exactly one item",
                    format!("{}.args", base_path),
                );
            }
        }
        "sort_by" => {
            if !(2..=3).contains(&args_len) {
                ctx.push(
                    ErrorCode::InvalidArgs,
                    "expr.args must contain two or three items",
                    format!("{}.args", base_path),
                );
            }
        }
        "fold" => {
            if args_len != 3 {
                ctx.push(
                    ErrorCode::InvalidArgs,
                    "expr.args must contain exactly three items",
                    format!("{}.args", base_path),
                );
            }
        }
        "+" | "*" | "and" | "or" => {
            if args_len < 2 {
                ctx.push(
                    ErrorCode::InvalidArgs,
                    "expr.args must contain at least two items",
                    format!("{}.args", base_path),
                );
            }
        }
        "-" | "/" | "to_base" | "==" | "!=" | "<" | "<=" | ">" | ">=" | "~=" => {
            if args_len != 2 {
                ctx.push(
                    ErrorCode::InvalidArgs,
                    "expr.args must contain exactly two items",
                    format!("{}.args", base_path),
                );
            }
        }
        "round" => {
            if !(1..=2).contains(&args_len) {
                ctx.push(
                    ErrorCode::InvalidArgs,
                    "expr.args must contain one or two items",
                    format!("{}.args", base_path),
                );
            }
        }
        "date_format" => {
            if !(2..=4).contains(&args_len) {
                ctx.push(
                    ErrorCode::InvalidArgs,
                    "expr.args must contain two to four items",
                    format!("{}.args", base_path),
                );
            }
        }
        "to_unixtime" => {
            if !(1..=3).contains(&args_len) {
                ctx.push(
                    ErrorCode::InvalidArgs,
                    "expr.args must contain one to three items",
                    format!("{}.args", base_path),
                );
            }
        }
        _ => {}
    }

    let expr_scope = element_expr_scope(&expr_op.op, true, expr_op.args.len(), scope);
    for (index, arg) in expr_op.args.iter().enumerate() {
        let arg_path = format!("{}.args[{}]", base_path, index);
        let arg_scope = match expr_scope {
            Some((expr_index, expr_scope)) if expr_index == index => expr_scope,
            _ => scope,
        };
        validate_expr(arg, &arg_path, produced_targets, ctx, arg_scope);
    }
}

fn element_expr_scope(
    op: &str,
    injected: bool,
    args_len: usize,
    parent_scope: LocalScope,
) -> Option<(usize, LocalScope)> {
    let item_scope = if parent_scope.allows_acc() {
        LocalScope::ItemAcc
    } else {
        LocalScope::Item
    };
    match op {
        "map"
        | "filter"
        | "flat_map"
        | "group_by"
        | "key_by"
        | "partition"
        | "distinct_by"
        | "sort_by"
        | "find"
        | "find_index" => {
            let index = if injected { 0 } else { 1 };
            Some((index, item_scope))
        }
        "zip_with" => args_len.checked_sub(1).map(|index| (index, item_scope)),
        "reduce" => {
            let index = if injected { 0 } else { 1 };
            Some((index, LocalScope::ItemAcc))
        }
        "fold" => {
            let index = if injected { 1 } else { 2 };
            Some((index, LocalScope::ItemAcc))
        }
        _ => None,
    }
}

fn validate_lookup_args_chain(expr_op: &ExprOp, base_path: &str, ctx: &mut ValidationCtx<'_>) {
    let len = expr_op.args.len();
    if !(2..=3).contains(&len) {
        ctx.push(
            ErrorCode::InvalidArgs,
            "lookup args must be [key_path, match_value, output_path?] in chain",
            format!("{}.args", base_path),
        );
        return;
    }

    let key_path = literal_string(&expr_op.args[0]);
    if key_path.is_none() || key_path == Some("") {
        ctx.push(
            ErrorCode::InvalidArgs,
            "lookup key_path must be a non-empty string literal",
            format!("{}.args[0]", base_path),
        );
    } else if parse_path(key_path.unwrap()).is_err() {
        ctx.push(
            ErrorCode::InvalidArgs,
            "lookup key_path is invalid",
            format!("{}.args[0]", base_path),
        );
    }

    if len == 3 {
        let output_path = literal_string(&expr_op.args[2]);
        if output_path.is_none() || output_path == Some("") {
            ctx.push(
                ErrorCode::InvalidArgs,
                "lookup output_path must be a non-empty string literal",
                format!("{}.args[2]", base_path),
            );
        } else if parse_path(output_path.unwrap()).is_err() {
            ctx.push(
                ErrorCode::InvalidArgs,
                "lookup output_path is invalid",
                format!("{}.args[2]", base_path),
            );
        }
    }
}

fn validate_ref(
    expr_ref: &ExprRef,
    base_path: &str,
    produced_targets: &HashSet<Vec<PathToken>>,
    ctx: &mut ValidationCtx<'_>,
    scope: LocalScope,
) {
    let (namespace, path) = match parse_ref(&expr_ref.ref_path) {
        Some(parsed) => parsed,
        None => {
            ctx.push(
                ErrorCode::InvalidRefNamespace,
                "ref namespace must be input|context|out|item|acc",
                base_path,
            );
            return;
        }
    };

    match namespace {
        Namespace::Item => {
            if !scope.allows_item() {
                ctx.push(
                    ErrorCode::InvalidRefNamespace,
                    "item refs are only allowed inside array ops",
                    base_path,
                );
                return;
            }
        }
        Namespace::Acc => {
            if !scope.allows_acc() {
                ctx.push(
                    ErrorCode::InvalidRefNamespace,
                    "acc refs are only allowed inside reduce/fold ops",
                    base_path,
                );
                return;
            }
        }
        _ => {}
    }

    let tokens = match parse_path(path) {
        Ok(tokens) => tokens,
        Err(_) => {
            ctx.push(ErrorCode::InvalidPath, "path is invalid", base_path);
            return;
        }
    };

    match namespace {
        Namespace::Out => {
            if !out_ref_resolves(&tokens, produced_targets) {
                ctx.push(
                    ErrorCode::ForwardOutReference,
                    "out reference must point to previous mappings",
                    base_path,
                );
            }
        }
        Namespace::Item => {
            let ok = matches!(tokens.first(), Some(PathToken::Key(key)) if key == "value" || key == "index");
            if !ok {
                ctx.push(
                    ErrorCode::InvalidPath,
                    "item ref must start with value or index",
                    base_path,
                );
            }
        }
        Namespace::Acc => {
            let ok = matches!(tokens.first(), Some(PathToken::Key(key)) if key == "value");
            if !ok {
                ctx.push(
                    ErrorCode::InvalidPath,
                    "acc ref must start with value",
                    base_path,
                );
            }
        }
        _ => {}
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
    scope: LocalScope,
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
        "replace" => {
            if !(3..=4).contains(&expr_op.args.len()) {
                ctx.push(
                    ErrorCode::InvalidArgs,
                    "expr.args must contain three or four items",
                    format!("{}.args", base_path),
                );
            }
        }
        "split" => {
            if expr_op.args.len() != 2 {
                ctx.push(
                    ErrorCode::InvalidArgs,
                    "expr.args must contain exactly two items",
                    format!("{}.args", base_path),
                );
            }
        }
        "pad_start" | "pad_end" => {
            if !(2..=3).contains(&expr_op.args.len()) {
                ctx.push(
                    ErrorCode::InvalidArgs,
                    "expr.args must contain two or three items",
                    format!("{}.args", base_path),
                );
            }
        }
        "lookup" | "lookup_first" => {
            validate_lookup_args(expr_op, base_path, ctx);
        }
        "merge" | "deep_merge" => {
            if expr_op.args.len() < 2 {
                ctx.push(
                    ErrorCode::InvalidArgs,
                    "expr.args must contain at least two items",
                    format!("{}.args", base_path),
                );
            }
        }
        "get" => {
            if expr_op.args.len() != 2 {
                ctx.push(
                    ErrorCode::InvalidArgs,
                    "expr.args must contain exactly two items",
                    format!("{}.args", base_path),
                );
            } else {
                validate_path_arg(&expr_op.args[1], &format!("{}.args[1]", base_path), ctx);
            }
        }
        "pick" | "omit" => {
            if expr_op.args.len() != 2 {
                ctx.push(
                    ErrorCode::InvalidArgs,
                    "expr.args must contain exactly two items",
                    format!("{}.args", base_path),
                );
            } else {
                let allow_terminal_index = expr_op.op == "pick";
                validate_path_array_arg(
                    &expr_op.args[1],
                    &format!("{}.args[1]", base_path),
                    allow_terminal_index,
                    ctx,
                );
            }
        }
        "keys" | "values" | "entries" | "object_flatten" | "object_unflatten" => {
            if expr_op.args.len() != 1 {
                ctx.push(
                    ErrorCode::InvalidArgs,
                    "expr.args must contain exactly one item",
                    format!("{}.args", base_path),
                );
            }
        }
        "map"
        | "filter"
        | "flat_map"
        | "group_by"
        | "key_by"
        | "partition"
        | "distinct_by"
        | "find"
        | "find_index" => {
            if expr_op.args.len() != 2 {
                ctx.push(
                    ErrorCode::InvalidArgs,
                    "expr.args must contain exactly two items",
                    format!("{}.args", base_path),
                );
            }
        }
        "flatten" => {
            if !(1..=2).contains(&expr_op.args.len()) {
                ctx.push(
                    ErrorCode::InvalidArgs,
                    "expr.args must contain one or two items",
                    format!("{}.args", base_path),
                );
            }
        }
        "take" | "drop" | "chunk" | "index_of" | "contains" | "reduce" => {
            if expr_op.args.len() != 2 {
                ctx.push(
                    ErrorCode::InvalidArgs,
                    "expr.args must contain exactly two items",
                    format!("{}.args", base_path),
                );
            }
        }
        "slice" => {
            if !(2..=3).contains(&expr_op.args.len()) {
                ctx.push(
                    ErrorCode::InvalidArgs,
                    "expr.args must contain two or three items",
                    format!("{}.args", base_path),
                );
            }
        }
        "zip" => {
            if expr_op.args.len() < 2 {
                ctx.push(
                    ErrorCode::InvalidArgs,
                    "expr.args must contain at least two items",
                    format!("{}.args", base_path),
                );
            }
        }
        "zip_with" => {
            if expr_op.args.len() < 3 {
                ctx.push(
                    ErrorCode::InvalidArgs,
                    "expr.args must contain at least three items",
                    format!("{}.args", base_path),
                );
            }
        }
        "unzip" | "unique" | "sum" | "avg" | "min" | "max" => {
            if expr_op.args.len() != 1 {
                ctx.push(
                    ErrorCode::InvalidArgs,
                    "expr.args must contain exactly one item",
                    format!("{}.args", base_path),
                );
            }
        }
        "sort_by" => {
            if !(2..=3).contains(&expr_op.args.len()) {
                ctx.push(
                    ErrorCode::InvalidArgs,
                    "expr.args must contain two or three items",
                    format!("{}.args", base_path),
                );
            }
        }
        "fold" => {
            if expr_op.args.len() != 3 {
                ctx.push(
                    ErrorCode::InvalidArgs,
                    "expr.args must contain exactly three items",
                    format!("{}.args", base_path),
                );
            }
        }
        "+" | "*" => {
            if expr_op.args.len() < 2 {
                ctx.push(
                    ErrorCode::InvalidArgs,
                    "expr.args must contain at least two items",
                    format!("{}.args", base_path),
                );
            }
        }
        "-" | "/" | "to_base" => {
            if expr_op.args.len() != 2 {
                ctx.push(
                    ErrorCode::InvalidArgs,
                    "expr.args must contain exactly two items",
                    format!("{}.args", base_path),
                );
            }
        }
        "round" => {
            if !(1..=2).contains(&expr_op.args.len()) {
                ctx.push(
                    ErrorCode::InvalidArgs,
                    "expr.args must contain one or two items",
                    format!("{}.args", base_path),
                );
            }
        }
        "date_format" => {
            if !(2..=4).contains(&expr_op.args.len()) {
                ctx.push(
                    ErrorCode::InvalidArgs,
                    "expr.args must contain two to four items",
                    format!("{}.args", base_path),
                );
            }
        }
        "to_unixtime" => {
            if !(1..=3).contains(&expr_op.args.len()) {
                ctx.push(
                    ErrorCode::InvalidArgs,
                    "expr.args must contain one to three items",
                    format!("{}.args", base_path),
                );
            }
        }
        "and" | "or" => {
            if expr_op.args.len() < 2 {
                ctx.push(
                    ErrorCode::InvalidArgs,
                    "expr.args must contain at least two items",
                    format!("{}.args", base_path),
                );
            }
        }
        "not" => {
            if expr_op.args.len() != 1 {
                ctx.push(
                    ErrorCode::InvalidArgs,
                    "expr.args must contain exactly one item",
                    format!("{}.args", base_path),
                );
            }
        }
        "==" | "!=" | "<" | "<=" | ">" | ">=" | "~=" => {
            if expr_op.args.len() != 2 {
                ctx.push(
                    ErrorCode::InvalidArgs,
                    "expr.args must contain exactly two items",
                    format!("{}.args", base_path),
                );
            }
        }
        _ => {}
    }

    let expr_scope = element_expr_scope(&expr_op.op, false, expr_op.args.len(), scope);
    for (index, arg) in expr_op.args.iter().enumerate() {
        let arg_path = format!("{}.args[{}]", base_path, index);
        let arg_scope = match expr_scope {
            Some((expr_index, expr_scope)) if expr_index == index => expr_scope,
            _ => scope,
        };
        validate_expr(arg, &arg_path, produced_targets, ctx, arg_scope);
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
        "item" => Namespace::Item,
        "acc" => Namespace::Acc,
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
            | "replace"
            | "split"
            | "pad_start"
            | "pad_end"
            | "lookup"
            | "lookup_first"
            | "merge"
            | "deep_merge"
            | "get"
            | "pick"
            | "omit"
            | "keys"
            | "values"
            | "entries"
            | "object_flatten"
            | "object_unflatten"
            | "map"
            | "filter"
            | "flat_map"
            | "flatten"
            | "take"
            | "drop"
            | "slice"
            | "chunk"
            | "zip"
            | "zip_with"
            | "unzip"
            | "group_by"
            | "key_by"
            | "partition"
            | "unique"
            | "distinct_by"
            | "sort_by"
            | "find"
            | "find_index"
            | "index_of"
            | "contains"
            | "sum"
            | "avg"
            | "min"
            | "max"
            | "reduce"
            | "fold"
            | "+"
            | "-"
            | "*"
            | "/"
            | "round"
            | "to_base"
            | "date_format"
            | "to_unixtime"
            | "and"
            | "or"
            | "not"
            | "=="
            | "!="
            | "<"
            | "<="
            | ">"
            | ">="
            | "~="
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

fn validate_path_array_arg(
    expr: &Expr,
    base_path: &str,
    allow_terminal_index: bool,
    ctx: &mut ValidationCtx<'_>,
) {
    let value = match expr {
        Expr::Literal(value) => value,
        _ => return,
    };

    let mut items: Vec<(String, String)> = Vec::new();
    if let Some(path) = value.as_str() {
        items.push((base_path.to_string(), path.to_string()));
    } else if let Some(array) = value.as_array() {
        for (index, item) in array.iter().enumerate() {
            let item_path = format!("{}[{}]", base_path, index);
            let path = match item.as_str() {
                Some(path) => path,
                None => {
                    ctx.push(
                        ErrorCode::InvalidArgs,
                        "paths must be a string or array of strings",
                        item_path,
                    );
                    continue;
                }
            };
            items.push((item_path, path.to_string()));
        }
    } else {
        ctx.push(
            ErrorCode::InvalidArgs,
            "paths must be a string or array of strings",
            base_path,
        );
        return;
    }

    let mut paths: Vec<Vec<PathToken>> = Vec::new();
    for (item_path, path) in items {

        let tokens = match parse_path(&path) {
            Ok(tokens) => tokens,
            Err(_) => {
                ctx.push(
                    ErrorCode::InvalidArgs,
                    "paths must be valid path strings",
                    item_path,
                );
                continue;
            }
        };

        if !allow_terminal_index && matches!(tokens.last(), Some(PathToken::Index(_))) {
            ctx.push(
                ErrorCode::InvalidArgs,
                "path must not end with array index",
                item_path,
            );
            continue;
        }

        if paths.iter().any(|existing| existing == &tokens) {
            continue;
        }
        if has_path_conflict(&paths, &tokens) {
            ctx.push(
                ErrorCode::InvalidArgs,
                "path conflicts with another path",
                item_path,
            );
            continue;
        }
        paths.push(tokens);
    }
}

fn validate_path_arg(expr: &Expr, base_path: &str, ctx: &mut ValidationCtx<'_>) {
    let value = match expr {
        Expr::Literal(value) => value,
        _ => return,
    };

    let path = match value.as_str() {
        Some(path) => path,
        None => {
            ctx.push(ErrorCode::InvalidArgs, "path must be a string", base_path);
            return;
        }
    };

    if path.is_empty() {
        ctx.push(
            ErrorCode::InvalidArgs,
            "path must be a non-empty string",
            base_path,
        );
        return;
    }

    if parse_path(path).is_err() {
        ctx.push(
            ErrorCode::InvalidArgs,
            "path must be a valid path string",
            base_path,
        );
    }
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
    Item,
    Acc,
}
