use std::fs;
use std::io::{self, BufRead, BufReader, Write};

use serde_json::{json, Map, Value};
use transform_rules::{
    parse_rule_file, transform_stream, transform_with_warnings, validate_rule_file_with_source,
    InputFormat, RuleError, RuleFile, TransformError, TransformErrorKind, TransformWarning,
};

const PROTOCOL_VERSION: &str = "2024-11-05";

fn main() {
    if let Err(err) = run() {
        eprintln!("fatal: {}", err);
        std::process::exit(1);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OutputMode {
    Line,
    ContentLength,
}

fn run() -> Result<(), String> {
    let stdin = io::stdin();
    let stdout = io::stdout();
    let mut reader = BufReader::new(stdin.lock());
    let mut writer = io::BufWriter::new(stdout.lock());
    let mut output_mode = OutputMode::Line;

    loop {
        let message = match read_message(&mut reader, &mut output_mode) {
            Ok(Some(message)) => message,
            Ok(None) => break,
            Err(err) => return Err(err.to_string()),
        };

        let value: Value = match serde_json::from_str(&message) {
            Ok(value) => value,
            Err(err) => {
                eprintln!("invalid json: {}", err);
                continue;
            }
        };

        if let Some(response) = handle_message(value) {
            write_message(&mut writer, output_mode, &response)
                .map_err(|err| err.to_string())?;
        }
    }

    Ok(())
}

fn read_message(
    reader: &mut impl BufRead,
    output_mode: &mut OutputMode,
) -> io::Result<Option<String>> {
    let mut line = String::new();
    loop {
        line.clear();
        let bytes = reader.read_line(&mut line)?;
        if bytes == 0 {
            return Ok(None);
        }

        if let Some(length) = line.strip_prefix("Content-Length:") {
            let length = length.trim().parse::<usize>().map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "invalid Content-Length")
            })?;

            loop {
                line.clear();
                let bytes = reader.read_line(&mut line)?;
                if bytes == 0 {
                    return Ok(None);
                }
                if line == "\r\n" || line == "\n" {
                    break;
                }
            }

            let mut buffer = vec![0u8; length];
            reader.read_exact(&mut buffer)?;
            *output_mode = OutputMode::ContentLength;
            return Ok(Some(String::from_utf8_lossy(&buffer).to_string()));
        }

        let trimmed = line.trim_end_matches(['\r', '\n']);
        if trimmed.is_empty() {
            continue;
        }
        *output_mode = OutputMode::Line;
        return Ok(Some(trimmed.to_string()));
    }
}

fn write_message(writer: &mut impl Write, output_mode: OutputMode, message: &Value) -> io::Result<()> {
    let text = serde_json::to_string(message)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;

    match output_mode {
        OutputMode::Line => {
            writeln!(writer, "{}", text)?;
        }
        OutputMode::ContentLength => {
            write!(writer, "Content-Length: {}\r\n\r\n{}", text.len(), text)?;
        }
    }

    writer.flush()
}

fn handle_message(message: Value) -> Option<Value> {
    let obj = message.as_object()?;
    let id = obj.get("id").cloned();
    let method = obj.get("method").and_then(|value| value.as_str());

    let Some(method) = method else {
        return id.map(|id| error_response(id, -32600, "Invalid Request"));
    };

    match method {
        "initialize" => id.map(|id| ok_response(id, initialize_result())),
        "tools/list" => id.map(|id| ok_response(id, tools_list_result())),
        "tools/call" => {
            let id = id?;
            let params = obj.get("params").cloned().unwrap_or(Value::Null);
            match handle_tools_call(&params) {
                Ok(result) => Some(ok_response(id, result)),
                Err(CallError::InvalidParams(message)) => {
                    Some(error_response(id, -32602, &message))
                }
                Err(CallError::Tool(message)) => Some(ok_response(id, tool_error_result(&message))),
            }
        }
        "ping" => id.map(|id| ok_response(id, json!({}))),
        "shutdown" => id.map(|id| ok_response(id, Value::Null)),
        "initialized" => None,
        _ => id.map(|id| error_response(id, -32601, "Method not found")),
    }
}

fn ok_response(id: Value, result: Value) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": result,
    })
}

fn error_response(id: Value, code: i32, message: &str) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": {
            "code": code,
            "message": message,
        }
    })
}

fn initialize_result() -> Value {
    json!({
        "protocolVersion": PROTOCOL_VERSION,
        "capabilities": {
            "tools": {
                "listChanged": false
            }
        },
        "serverInfo": {
            "name": "transform-rules-mcp",
            "version": env!("CARGO_PKG_VERSION")
        }
    })
}

fn tools_list_result() -> Value {
    json!({
        "tools": [
            {
                "name": "transform",
                "description": "Transform CSV/JSON input with a YAML rule file.",
                "inputSchema": tool_input_schema()
            }
        ]
    })
}

fn tool_input_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "rules_path": {
                "type": "string",
                "description": "Path to the YAML rules file."
            },
            "input_path": {
                "type": "string",
                "description": "Path to the input CSV/JSON file."
            },
            "context_path": {
                "type": "string",
                "description": "Optional path to a JSON context file."
            },
            "format": {
                "type": "string",
                "enum": ["csv", "json"],
                "description": "Override input format from the rule file."
            },
            "ndjson": {
                "type": "boolean",
                "description": "Emit NDJSON output (one JSON object per line)."
            },
            "validate": {
                "type": "boolean",
                "description": "Validate the rule file before transforming."
            },
            "output_path": {
                "type": "string",
                "description": "Optional path to write the output."
            }
        },
        "required": ["rules_path", "input_path"]
    })
}

enum CallError {
    InvalidParams(String),
    Tool(String),
}

fn handle_tools_call(params: &Value) -> Result<Value, CallError> {
    let obj = params.as_object().ok_or_else(|| {
        CallError::InvalidParams("params must be an object".to_string())
    })?;
    let name = obj
        .get("name")
        .and_then(|value| value.as_str())
        .ok_or_else(|| CallError::InvalidParams("params.name is required".to_string()))?;
    let args = obj
        .get("arguments")
        .and_then(|value| value.as_object())
        .ok_or_else(|| {
            CallError::InvalidParams("params.arguments must be an object".to_string())
        })?;

    match name {
        "transform" => run_transform_tool(args).map_err(CallError::Tool),
        _ => Ok(tool_error_result(&format!("unknown tool: {}", name))),
    }
}

fn run_transform_tool(args: &Map<String, Value>) -> Result<Value, String> {
    let rules_path = get_required_string(args, "rules_path")?;
    let input_path = get_required_string(args, "input_path")?;
    let context_path = get_optional_string(args, "context_path")?;
    let format = get_optional_string(args, "format")?;
    let ndjson = get_optional_bool(args, "ndjson")?.unwrap_or(false);
    let validate = get_optional_bool(args, "validate")?.unwrap_or(false);
    let output_path = get_optional_string(args, "output_path")?;

    let (mut rule, yaml) = load_rule(&rules_path)?;
    apply_format_override(&mut rule, format.as_deref())?;

    if validate {
        if let Err(errors) = validate_rule_file_with_source(&rule, &yaml) {
            let error_text = validation_errors_to_text(&errors);
            return Ok(tool_error_result(&error_text));
        }
    }

    let input = read_file(&input_path, "input")?;
    let context_value = match context_path {
        Some(path) => Some(load_context(&path)?),
        None => None,
    };

    let (output_text, warnings) = if ndjson {
        transform_to_ndjson(&rule, &input, context_value.as_ref())?
    } else {
        transform_to_json(&rule, &input, context_value.as_ref())?
    };

    if let Some(path) = output_path.as_ref() {
        write_output(path, &output_text)?;
    }

    let mut result = json!({
        "content": [
            {
                "type": "text",
                "text": output_text
            }
        ]
    });

    if !warnings.is_empty() || output_path.is_some() {
        let mut meta = serde_json::Map::new();
        if !warnings.is_empty() {
            meta.insert("warnings".to_string(), warnings_to_json(&warnings));
        }
        if let Some(path) = output_path {
            meta.insert("output_path".to_string(), json!(path));
        }
        result["meta"] = Value::Object(meta);
    }

    Ok(result)
}

fn tool_error_result(message: &str) -> Value {
    json!({
        "content": [
            {
                "type": "text",
                "text": message
            }
        ],
        "isError": true
    })
}

fn get_required_string(args: &Map<String, Value>, key: &str) -> Result<String, String> {
    match args.get(key) {
        Some(Value::String(value)) => Ok(value.clone()),
        Some(_) => Err(format!("{} must be a string", key)),
        None => Err(format!("{} is required", key)),
    }
}

fn get_optional_string(args: &Map<String, Value>, key: &str) -> Result<Option<String>, String> {
    match args.get(key) {
        Some(Value::String(value)) => Ok(Some(value.clone())),
        Some(Value::Null) => Ok(None),
        Some(_) => Err(format!("{} must be a string", key)),
        None => Ok(None),
    }
}

fn get_optional_bool(args: &Map<String, Value>, key: &str) -> Result<Option<bool>, String> {
    match args.get(key) {
        Some(Value::Bool(value)) => Ok(Some(*value)),
        Some(Value::Null) => Ok(None),
        Some(_) => Err(format!("{} must be a boolean", key)),
        None => Ok(None),
    }
}

fn load_rule(path: &str) -> Result<(RuleFile, String), String> {
    let yaml = read_file(path, "rules")?;
    let rule = parse_rule_file(&yaml).map_err(|err| format!("failed to parse rules: {}", err))?;
    Ok((rule, yaml))
}

fn apply_format_override(rule: &mut RuleFile, format: Option<&str>) -> Result<(), String> {
    let Some(format) = format else { return Ok(()); };
    let normalized = format.to_lowercase();
    rule.input.format = match normalized.as_str() {
        "csv" => InputFormat::Csv,
        "json" => InputFormat::Json,
        _ => return Err(format!("unknown format: {}", format)),
    };
    Ok(())
}

fn read_file(path: &str, label: &str) -> Result<String, String> {
    fs::read_to_string(path).map_err(|err| format!("failed to read {}: {}", label, err))
}

fn load_context(path: &str) -> Result<serde_json::Value, String> {
    let data = read_file(path, "context")?;
    serde_json::from_str(&data).map_err(|err| format!("failed to parse context JSON: {}", err))
}

fn write_output(path: &str, output: &str) -> Result<(), String> {
    let path = std::path::Path::new(path);
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)
                .map_err(|err| format!("failed to create output directory: {}", err))?;
        }
    }
    fs::write(path, output.as_bytes()).map_err(|err| format!("failed to write output: {}", err))
}

fn transform_to_json(
    rule: &RuleFile,
    input: &str,
    context: Option<&serde_json::Value>,
) -> Result<(String, Vec<TransformWarning>), String> {
    let (output, warnings) = transform_with_warnings(rule, input, context)
        .map_err(|err| transform_error_to_text(&err))?;
    let output_text = serde_json::to_string(&output)
        .map_err(|err| format!("failed to serialize output JSON: {}", err))?;
    Ok((output_text, warnings))
}

fn transform_to_ndjson(
    rule: &RuleFile,
    input: &str,
    context: Option<&serde_json::Value>,
) -> Result<(String, Vec<TransformWarning>), String> {
    let stream = transform_stream(rule, input, context)
        .map_err(|err| transform_error_to_text(&err))?;
    let mut output = String::new();
    let mut warnings = Vec::new();

    for item in stream {
        let item = item.map_err(|err| transform_error_to_text(&err))?;
        warnings.extend(item.warnings);
        let line = serde_json::to_string(&item.output)
            .map_err(|err| format!("failed to serialize output JSON: {}", err))?;
        output.push_str(&line);
        output.push('\n');
    }

    Ok((output, warnings))
}

fn validation_errors_to_text(errors: &[RuleError]) -> String {
    let values: Vec<_> = errors.iter().map(validation_error_json).collect();
    serde_json::to_string(&values).unwrap_or_else(|_| "validation error".to_string())
}

fn validation_error_json(err: &RuleError) -> Value {
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

fn transform_error_to_text(err: &TransformError) -> String {
    let value = transform_error_json(err);
    serde_json::to_string(&vec![value]).unwrap_or_else(|_| err.message.clone())
}

fn transform_error_json(err: &TransformError) -> Value {
    let mut value = json!({
        "type": "transform",
        "kind": transform_kind_to_str(&err.kind),
        "message": err.message,
    });
    if let Some(path) = &err.path {
        value["path"] = json!(path);
    }
    value
}

fn warnings_to_json(warnings: &[TransformWarning]) -> Value {
    let values: Vec<_> = warnings.iter().map(transform_warning_json).collect();
    Value::Array(values)
}

fn transform_warning_json(warning: &TransformWarning) -> Value {
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
