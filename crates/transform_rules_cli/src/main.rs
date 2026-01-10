use std::fs;
use std::path::PathBuf;

use clap::{Args, Parser, Subcommand, ValueEnum};
use serde_json::json;
use transform_rules::{
    parse_rule_file, transform, validate_rule_file_with_source, InputFormat, RuleError, RuleFile,
    TransformError, TransformErrorKind,
};

#[derive(Parser)]
#[command(name = "transform-rules")]
#[command(version, about = "Transform CSV/JSON data using YAML rules")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Validate(ValidateArgs),
    Transform(TransformArgs),
}

#[derive(Args)]
struct ValidateArgs {
    #[arg(long)]
    rules: PathBuf,
    #[arg(long, default_value = "text")]
    error_format: ErrorFormat,
}

#[derive(Args)]
struct TransformArgs {
    #[arg(long)]
    rules: PathBuf,
    #[arg(long)]
    input: PathBuf,
    #[arg(long)]
    format: Option<FormatOverride>,
    #[arg(long)]
    context: Option<PathBuf>,
    #[arg(long)]
    output: Option<PathBuf>,
    #[arg(long)]
    validate: bool,
    #[arg(long, default_value = "text")]
    error_format: ErrorFormat,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum ErrorFormat {
    Text,
    Json,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum FormatOverride {
    Csv,
    Json,
}

fn main() {
    let cli = Cli::parse();
    let exit_code = match cli.command {
        Commands::Validate(args) => run_validate(args),
        Commands::Transform(args) => run_transform(args),
    };
    std::process::exit(exit_code);
}

fn run_validate(args: ValidateArgs) -> i32 {
    let (rule, yaml) = match load_rule(&args.rules) {
        Ok(value) => value,
        Err(code) => return code,
    };

    match validate_rule_file_with_source(&rule, &yaml) {
        Ok(()) => 0,
        Err(errors) => {
            emit_validation_errors(&errors, args.error_format);
            2
        }
    }
}

fn run_transform(args: TransformArgs) -> i32 {
    let (mut rule, yaml) = match load_rule(&args.rules) {
        Ok(value) => value,
        Err(code) => return code,
    };

    if let Some(format) = args.format {
        rule.input.format = match format {
            FormatOverride::Csv => InputFormat::Csv,
            FormatOverride::Json => InputFormat::Json,
        };
    }

    if args.validate {
        if let Err(errors) = validate_rule_file_with_source(&rule, &yaml) {
            emit_validation_errors(&errors, args.error_format);
            return 2;
        }
    }

    let input = match fs::read_to_string(&args.input) {
        Ok(value) => value,
        Err(err) => {
            eprintln!("failed to read input: {}", err);
            return 1;
        }
    };

    let context_value = match args.context {
        Some(path) => match fs::read_to_string(&path) {
            Ok(data) => match serde_json::from_str(&data) {
                Ok(json) => Some(json),
                Err(err) => {
                    eprintln!("failed to parse context JSON: {}", err);
                    return 1;
                }
            },
            Err(err) => {
                eprintln!("failed to read context: {}", err);
                return 1;
            }
        },
        None => None,
    };

    let output = match transform(&rule, &input, context_value.as_ref()) {
        Ok(value) => value,
        Err(err) => {
            emit_transform_error(&err, args.error_format);
            return 3;
        }
    };

    let output_text = match serde_json::to_string(&output) {
        Ok(text) => text,
        Err(err) => {
            eprintln!("failed to serialize output JSON: {}", err);
            return 1;
        }
    };

    if let Some(path) = args.output {
        if let Err(err) = fs::write(&path, output_text.as_bytes()) {
            eprintln!("failed to write output: {}", err);
            return 1;
        }
    } else {
        println!("{}", output_text);
    }

    0
}

fn load_rule(path: &PathBuf) -> Result<(RuleFile, String), i32> {
    let yaml = match fs::read_to_string(path) {
        Ok(data) => data,
        Err(err) => {
            eprintln!("failed to read rules: {}", err);
            return Err(1);
        }
    };

    let rule = match parse_rule_file(&yaml) {
        Ok(rule) => rule,
        Err(err) => {
            eprintln!("failed to parse rules: {}", err);
            return Err(1);
        }
    };

    Ok((rule, yaml))
}

fn emit_validation_errors(errors: &[RuleError], format: ErrorFormat) {
    match format {
        ErrorFormat::Text => {
            for err in errors {
                emit_validation_text(err);
            }
        }
        ErrorFormat::Json => {
            let values: Vec<_> = errors
                .iter()
                .map(|err| validation_error_json(err))
                .collect();
            eprintln!("{}", serde_json::to_string(&values).unwrap_or_default());
        }
    }
}

fn emit_validation_text(err: &RuleError) {
    let mut parts = Vec::new();
    parts.push(format!("E {}", err.code.as_str()));
    if let Some(path) = &err.path {
        parts.push(format!("path={}", path));
    }
    if let Some(location) = &err.location {
        parts.push(format!("line={}", location.line));
        parts.push(format!("col={}", location.column));
    }
    parts.push(format!("msg=\"{}\"", err.message));
    eprintln!("{}", parts.join(" "));
}

fn validation_error_json(err: &RuleError) -> serde_json::Value {
    let mut value = json!({
        "type": "validation",
        "code": err.code.as_str(),
        "message": err.message,
    });

    if let Some(path) = &err.path {
        value["path"] = json!(path);
    }
    if let Some(location) = &err.location {
        value["line"] = json!(location.line);
        value["column"] = json!(location.column);
    }

    value
}

fn emit_transform_error(err: &TransformError, format: ErrorFormat) {
    match format {
        ErrorFormat::Text => {
            let mut parts = Vec::new();
            parts.push(format!("E {}", transform_kind_to_str(&err.kind)));
            if let Some(path) = &err.path {
                parts.push(format!("path={}", path));
            }
            parts.push(format!("msg=\"{}\"", err.message));
            eprintln!("{}", parts.join(" "));
        }
        ErrorFormat::Json => {
            let mut value = json!({
                "type": "transform",
                "kind": transform_kind_to_str(&err.kind),
                "message": err.message,
            });
            if let Some(path) = &err.path {
                value["path"] = json!(path);
            }
            eprintln!("{}", serde_json::to_string(&vec![value]).unwrap_or_default());
        }
    }
}

fn transform_kind_to_str(kind: &TransformErrorKind) -> &'static str {
    match kind {
        TransformErrorKind::InvalidInput => "InvalidInput",
        TransformErrorKind::InvalidRecordsPath => "InvalidRecordsPath",
        TransformErrorKind::InvalidRef => "InvalidRef",
        TransformErrorKind::InvalidTarget => "InvalidTarget",
        TransformErrorKind::MissingRequired => "MissingRequired",
        TransformErrorKind::TypeCastFailed => "TypeCastFailed",
        TransformErrorKind::ExprError => "ExprError",
    }
}
