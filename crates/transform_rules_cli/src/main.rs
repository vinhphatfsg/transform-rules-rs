use std::fs;
use std::io::{self, Write};
use std::path::PathBuf;

use clap::{Args, Parser, Subcommand, ValueEnum};
use serde_json::json;
use transform_rules::{
    generate_dto, parse_rule_file, preflight_validate_with_warnings, transform_stream,
    transform_with_warnings, validate_rule_file_with_source, DtoLanguage, InputFormat, RuleError,
    RuleFile, TransformError, TransformErrorKind, TransformWarning,
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
    Preflight(PreflightArgs),
    Transform(TransformArgs),
    Generate(GenerateArgs),
}

#[derive(Args)]
struct ValidateArgs {
    #[arg(short = 'r', long)]
    rules: PathBuf,
    #[arg(short = 'e', long, default_value = "text")]
    error_format: ErrorFormat,
}

#[derive(Args)]
struct PreflightArgs {
    #[arg(short = 'r', long)]
    rules: PathBuf,
    #[arg(short = 'i', long)]
    input: PathBuf,
    #[arg(short = 'f', long)]
    format: Option<FormatOverride>,
    #[arg(short = 'c', long)]
    context: Option<PathBuf>,
    #[arg(short = 'e', long, default_value = "text")]
    error_format: ErrorFormat,
}

#[derive(Args)]
struct TransformArgs {
    #[arg(short = 'r', long)]
    rules: PathBuf,
    #[arg(short = 'i', long)]
    input: PathBuf,
    #[arg(short = 'f', long)]
    format: Option<FormatOverride>,
    #[arg(short = 'c', long)]
    context: Option<PathBuf>,
    #[arg(short = 'o', long)]
    output: Option<PathBuf>,
    #[arg(long)]
    ndjson: bool,
    #[arg(short = 'v', long)]
    validate: bool,
    #[arg(short = 'e', long, default_value = "text")]
    error_format: ErrorFormat,
}

#[derive(Args)]
struct GenerateArgs {
    #[arg(short = 'r', long)]
    rules: PathBuf,
    #[arg(short = 'l', long)]
    lang: DtoLanguageArg,
    #[arg(short = 'n', long)]
    name: Option<String>,
    #[arg(short = 'o', long)]
    output: Option<PathBuf>,
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

#[derive(Clone, Copy, Debug, ValueEnum)]
enum DtoLanguageArg {
    Rust,
    #[value(alias = "ts")]
    TypeScript,
    Python,
    Go,
    Java,
    Kotlin,
    Swift,
}

fn main() {
    let cli = Cli::parse();
    let exit_code = match cli.command {
        Commands::Validate(args) => run_validate(args),
        Commands::Preflight(args) => run_preflight(args),
        Commands::Transform(args) => run_transform(args),
        Commands::Generate(args) => run_generate(args),
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

fn run_preflight(args: PreflightArgs) -> i32 {
    let (mut rule, _) = match load_rule(&args.rules) {
        Ok(value) => value,
        Err(code) => return code,
    };

    apply_format_override(&mut rule, args.format);

    let input = match load_input(&args.input) {
        Ok(value) => value,
        Err(code) => return code,
    };

    let context_value = match load_context(&args.context) {
        Ok(value) => value,
        Err(code) => return code,
    };

    let warnings = match preflight_validate_with_warnings(&rule, &input, context_value.as_ref()) {
        Ok(warnings) => warnings,
        Err(err) => {
            emit_transform_error(&err, args.error_format);
            return 3;
        }
    };

    emit_transform_warnings(&warnings, args.error_format);

    0
}

fn run_transform(args: TransformArgs) -> i32 {
    let (mut rule, yaml) = match load_rule(&args.rules) {
        Ok(value) => value,
        Err(code) => return code,
    };

    apply_format_override(&mut rule, args.format);

    if args.validate {
        if let Err(errors) = validate_rule_file_with_source(&rule, &yaml) {
            emit_validation_errors(&errors, args.error_format);
            return 2;
        }
    }

    let input = match load_input(&args.input) {
        Ok(value) => value,
        Err(code) => return code,
    };

    let context_value = match load_context(&args.context) {
        Ok(value) => value,
        Err(code) => return code,
    };

    if args.ndjson {
        return run_transform_ndjson(
            &rule,
            &input,
            context_value.as_ref(),
            args.output,
            args.error_format,
        );
    }

    let (output, warnings) = match transform_with_warnings(&rule, &input, context_value.as_ref())
    {
        Ok(result) => result,
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

    emit_transform_warnings(&warnings, args.error_format);

    if let Some(path) = args.output {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                if let Err(err) = fs::create_dir_all(parent) {
                    eprintln!("failed to create output directory: {}", err);
                    return 1;
                }
            }
        }
        if let Err(err) = fs::write(&path, output_text.as_bytes()) {
            eprintln!("failed to write output: {}", err);
            return 1;
        }
    } else {
        println!("{}", output_text);
    }

    0
}

fn run_transform_ndjson(
    rule: &RuleFile,
    input: &str,
    context: Option<&serde_json::Value>,
    output: Option<PathBuf>,
    error_format: ErrorFormat,
) -> i32 {
    let stream = match transform_stream(rule, input, context) {
        Ok(stream) => stream,
        Err(err) => {
            emit_transform_error(&err, error_format);
            return 3;
        }
    };

    let writer: Box<dyn Write> = match output {
        Some(path) => {
            if let Some(parent) = path.parent() {
                if !parent.as_os_str().is_empty() {
                    if let Err(err) = fs::create_dir_all(parent) {
                        eprintln!("failed to create output directory: {}", err);
                        return 1;
                    }
                }
            }
            match fs::File::create(&path) {
                Ok(file) => Box::new(file),
                Err(err) => {
                    eprintln!("failed to write output: {}", err);
                    return 1;
                }
            }
        }
        None => Box::new(io::stdout()),
    };

    let mut writer = io::BufWriter::new(writer);

    for item in stream {
        let item = match item {
            Ok(item) => item,
            Err(err) => {
                emit_transform_error(&err, error_format);
                return 3;
            }
        };

        emit_transform_warnings(&item.warnings, error_format);

        let output_text = match serde_json::to_string(&item.output) {
            Ok(text) => text,
            Err(err) => {
                eprintln!("failed to serialize output JSON: {}", err);
                return 1;
            }
        };

        if let Err(err) = writeln!(writer, "{}", output_text) {
            eprintln!("failed to write output: {}", err);
            return 1;
        }
    }

    if let Err(err) = writer.flush() {
        eprintln!("failed to write output: {}", err);
        return 1;
    }

    0
}

fn run_generate(args: GenerateArgs) -> i32 {
    let (rule, _) = match load_rule(&args.rules) {
        Ok(value) => value,
        Err(code) => return code,
    };

    let lang = match args.lang {
        DtoLanguageArg::Rust => DtoLanguage::Rust,
        DtoLanguageArg::TypeScript => DtoLanguage::TypeScript,
        DtoLanguageArg::Python => DtoLanguage::Python,
        DtoLanguageArg::Go => DtoLanguage::Go,
        DtoLanguageArg::Java => DtoLanguage::Java,
        DtoLanguageArg::Kotlin => DtoLanguage::Kotlin,
        DtoLanguageArg::Swift => DtoLanguage::Swift,
    };

    let output = match generate_dto(&rule, lang, args.name.as_deref()) {
        Ok(text) => text,
        Err(err) => {
            eprintln!("failed to generate dto: {}", err);
            return 1;
        }
    };

    if let Some(path) = args.output {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                if let Err(err) = fs::create_dir_all(parent) {
                    eprintln!("failed to create output directory: {}", err);
                    return 1;
                }
            }
        }
        if let Err(err) = fs::write(&path, output.as_bytes()) {
            eprintln!("failed to write output: {}", err);
            return 1;
        }
    } else {
        println!("{}", output);
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

fn apply_format_override(rule: &mut RuleFile, format: Option<FormatOverride>) {
    if let Some(format) = format {
        rule.input.format = match format {
            FormatOverride::Csv => InputFormat::Csv,
            FormatOverride::Json => InputFormat::Json,
        };
    }
}

fn load_input(path: &PathBuf) -> Result<String, i32> {
    match fs::read_to_string(path) {
        Ok(value) => Ok(value),
        Err(err) => {
            eprintln!("failed to read input: {}", err);
            Err(1)
        }
    }
}

fn load_context(path: &Option<PathBuf>) -> Result<Option<serde_json::Value>, i32> {
    match path {
        Some(path) => match fs::read_to_string(path) {
            Ok(data) => match serde_json::from_str(&data) {
                Ok(json) => Ok(Some(json)),
                Err(err) => {
                    eprintln!("failed to parse context JSON: {}", err);
                    Err(1)
                }
            },
            Err(err) => {
                eprintln!("failed to read context: {}", err);
                Err(1)
            }
        },
        None => Ok(None),
    }
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

fn emit_transform_warnings(warnings: &[TransformWarning], format: ErrorFormat) {
    if warnings.is_empty() {
        return;
    }

    match format {
        ErrorFormat::Text => {
            for warning in warnings {
                let mut parts = Vec::new();
                parts.push(format!("W {}", transform_kind_to_str(&warning.kind)));
                if let Some(path) = &warning.path {
                    parts.push(format!("path={}", path));
                }
                parts.push(format!("msg=\"{}\"", warning.message));
                eprintln!("{}", parts.join(" "));
            }
        }
        ErrorFormat::Json => {
            let values: Vec<_> = warnings
                .iter()
                .map(|warning| transform_warning_json(warning))
                .collect();
            eprintln!("{}", serde_json::to_string(&values).unwrap_or_default());
        }
    }
}

fn transform_warning_json(warning: &TransformWarning) -> serde_json::Value {
    let mut value = json!({
        "type": "warning",
        "kind": transform_kind_to_str(&warning.kind),
        "message": warning.message,
    });
    if let Some(path) = &warning.path {
        value["path"] = json!(path);
    }
    value
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
