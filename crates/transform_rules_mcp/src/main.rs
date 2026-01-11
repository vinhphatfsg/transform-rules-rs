use std::collections::HashMap;
use std::fs;
use std::io::{self, BufRead, BufReader, Write};

use csv::ReaderBuilder;
use serde_json::{json, Map, Value};
use transform_rules::{
    generate_dto, parse_rule_file, transform_stream, transform_with_warnings,
    validate_rule_file_with_source, DtoLanguage, InputFormat, RuleError, RuleFile, TransformError,
    TransformErrorKind, TransformWarning,
};

const PROTOCOL_VERSION: &str = "2024-11-05";
const RESOURCE_URI_RULES_SPEC_EN: &str = "transform-rules://docs/rules_spec_en";
const RESOURCE_URI_RULES_SPEC_JA: &str = "transform-rules://docs/rules_spec_ja";
const RESOURCE_URI_README: &str = "transform-rules://docs/readme";
const RESOURCE_RULES_SPEC_EN: &str = include_str!("../../../docs/rules_spec_en.md");
const RESOURCE_RULES_SPEC_JA: &str = include_str!("../../../docs/rules_spec_ja.md");
const RESOURCE_README: &str = include_str!("../../../README.md");

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
                Err(CallError::Tool { message, errors }) => {
                    Some(ok_response(id, tool_error_result(&message, errors)))
                }
            }
        }
        "resources/list" => id.map(|id| ok_response(id, resources_list_result())),
        "resources/read" => {
            let id = id?;
            let params = obj.get("params").cloned().unwrap_or(Value::Null);
            match resources_read_result(&params) {
                Ok(result) => Some(ok_response(id, result)),
                Err(message) => Some(error_response(id, -32602, &message)),
            }
        }
        "prompts/list" => id.map(|id| ok_response(id, prompts_list_result())),
        "prompts/get" => {
            let id = id?;
            let params = obj.get("params").cloned().unwrap_or(Value::Null);
            match prompts_get_result(&params) {
                Ok(result) => Some(ok_response(id, result)),
                Err(message) => Some(error_response(id, -32602, &message)),
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
            },
            "resources": {
                "listChanged": false
            },
            "prompts": {
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
                "inputSchema": transform_input_schema()
            },
            {
                "name": "validate_rules",
                "description": "Validate a YAML rule file.",
                "inputSchema": validate_rules_input_schema()
            },
            {
                "name": "generate_dto",
                "description": "Generate DTO definitions from a YAML rule file.",
                "inputSchema": generate_dto_input_schema()
            },
            {
                "name": "list_ops",
                "description": "List supported expression ops, comparisons, and type casts.",
                "inputSchema": list_ops_input_schema()
            },
            {
                "name": "analyze_input",
                "description": "Analyze input data and summarize field paths and types.",
                "inputSchema": analyze_input_input_schema()
            }
        ]
    })
}

fn resources_list_result() -> Value {
    json!({
        "resources": [
            {
                "uri": RESOURCE_URI_RULES_SPEC_EN,
                "name": "rules_spec_en",
                "description": "Rule specification (English).",
                "mimeType": "text/markdown"
            },
            {
                "uri": RESOURCE_URI_RULES_SPEC_JA,
                "name": "rules_spec_ja",
                "description": "ルール仕様 (日本語).",
                "mimeType": "text/markdown"
            },
            {
                "uri": RESOURCE_URI_README,
                "name": "readme",
                "description": "Project README.",
                "mimeType": "text/markdown"
            }
        ]
    })
}

fn resources_read_result(params: &Value) -> Result<Value, String> {
    let obj = params
        .as_object()
        .ok_or_else(|| "params must be an object".to_string())?;
    let uri = obj
        .get("uri")
        .and_then(|value| value.as_str())
        .ok_or_else(|| "params.uri is required".to_string())?;
    let text = match uri {
        RESOURCE_URI_RULES_SPEC_EN => RESOURCE_RULES_SPEC_EN,
        RESOURCE_URI_RULES_SPEC_JA => RESOURCE_RULES_SPEC_JA,
        RESOURCE_URI_README => RESOURCE_README,
        _ => return Err("unknown resource uri".to_string()),
    };

    Ok(json!({
        "contents": [
            {
                "uri": uri,
                "mimeType": "text/markdown",
                "text": text
            }
        ]
    }))
}

fn prompts_list_result() -> Value {
    json!({
        "prompts": [
            {
                "name": "rule_from_input_base",
                "description": "Generate rules from base rules and input samples.",
                "arguments": [
                    { "name": "rules_text", "description": "Base rules YAML.", "required": true },
                    { "name": "input_sample", "description": "Input sample (JSON/CSV).", "required": true },
                    { "name": "format", "description": "Input format (json or csv).", "required": false },
                    { "name": "records_path", "description": "Records path for JSON input.", "required": false }
                ]
            },
            {
                "name": "rule_from_dto",
                "description": "Generate rules from DTO schema and input samples.",
                "arguments": [
                    { "name": "dto_text", "description": "DTO source text.", "required": true },
                    { "name": "dto_language", "description": "DTO language (rust/typescript).", "required": true },
                    { "name": "input_sample", "description": "Input sample (JSON/CSV).", "required": true },
                    { "name": "format", "description": "Input format (json or csv).", "required": false },
                    { "name": "records_path", "description": "Records path for JSON input.", "required": false }
                ]
            },
            {
                "name": "explain_errors",
                "description": "Explain validation/transform errors and suggest fixes.",
                "arguments": [
                    { "name": "errors_json", "description": "Errors array from tool output.", "required": true },
                    { "name": "rules_text", "description": "Optional rules YAML for context.", "required": false }
                ]
            }
        ]
    })
}

fn prompts_get_result(params: &Value) -> Result<Value, String> {
    let obj = params
        .as_object()
        .ok_or_else(|| "params must be an object".to_string())?;
    let name = obj
        .get("name")
        .and_then(|value| value.as_str())
        .ok_or_else(|| "params.name is required".to_string())?;
    let args = obj.get("arguments").and_then(|value| value.as_object());

    let (description, template) = match name {
        "rule_from_input_base" => (
            "Generate rules from base rules and input samples.",
            r#"You are generating a transform-rules YAML file.
The base rules define the output shape. Keep existing expr/value/default/required unless mapping is unresolved.
Use the input sample to map sources. Unmapped targets must use value: null and required: false.
Return YAML only.

Base rules:
{{rules_text}}

Input sample:
{{input_sample}}

Optional format: {{format}}
Optional records_path: {{records_path}}
"#,
        ),
        "rule_from_dto" => (
            "Generate rules from DTO schema and input samples.",
            r#"You are generating a transform-rules YAML file whose output matches the DTO schema.
Use the input sample to map sources. Unmapped targets must use value: null and required: false.
Return YAML only.

DTO:
{{dto_text}}

DTO language: {{dto_language}}

Input sample:
{{input_sample}}

Optional format: {{format}}
Optional records_path: {{records_path}}
"#,
        ),
        "explain_errors" => (
            "Explain validation/transform errors and suggest fixes.",
            r#"Explain the following validation/transform errors and suggest fixes.

Errors:
{{errors_json}}

Rules (optional):
{{rules_text}}
"#,
        ),
        _ => return Err("unknown prompt name".to_string()),
    };

    let content = apply_prompt_args(template, args);
    Ok(json!({
        "description": description,
        "messages": [
            {
                "role": "user",
                "content": content
            }
        ]
    }))
}

fn apply_prompt_args(template: &str, args: Option<&Map<String, Value>>) -> String {
    let mut content = template.to_string();
    if let Some(args) = args {
        for (key, value) in args {
            let replacement = match value {
                Value::String(value) => value.clone(),
                _ => value.to_string(),
            };
            content = content.replace(&format!("{{{{{}}}}}", key), &replacement);
        }
    }
    content
}

fn transform_input_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "rules_path": {
                "type": "string",
                "description": "Path to the YAML rules file. Mutually exclusive with rules_text.",
                "examples": ["rules.yaml"]
            },
            "rules_text": {
                "type": "string",
                "description": "Inline YAML rules content. Mutually exclusive with rules_path.",
                "examples": ["version: 1\ninput:\n  format: json\n  json: {}\nmappings:\n  - target: \"id\"\n    source: \"id\""]
            },
            "input_path": {
                "type": "string",
                "description": "Path to the input CSV/JSON file. Mutually exclusive with input_text and input_json.",
                "examples": ["input.json"]
            },
            "input_text": {
                "type": "string",
                "description": "Inline input text (CSV or JSON). Mutually exclusive with input_path and input_json.",
                "examples": ["{\"items\":[{\"id\":1}]}"]
            },
            "input_json": {
                "type": ["object", "array"],
                "description": "Inline input JSON value. Mutually exclusive with input_path and input_text.",
                "examples": [[{"id": 1}]]
            },
            "context_path": {
                "type": "string",
                "description": "Optional path to a JSON context file. Mutually exclusive with context_json.",
                "examples": ["context.json"]
            },
            "context_json": {
                "type": "object",
                "description": "Optional inline JSON context value. Mutually exclusive with context_path.",
                "examples": [{"tenant_id": "t-001"}]
            },
            "format": {
                "type": "string",
                "enum": ["csv", "json"],
                "description": "Override input format from the rule file.",
                "examples": ["json"]
            },
            "ndjson": {
                "type": "boolean",
                "description": "Emit NDJSON output (one JSON object per line).",
                "examples": [false]
            },
            "validate": {
                "type": "boolean",
                "description": "Validate the rule file before transforming.",
                "examples": [true]
            },
            "output_path": {
                "type": "string",
                "description": "Optional path to write the output.",
                "examples": ["out.json"]
            },
            "max_output_bytes": {
                "type": "integer",
                "minimum": 1,
                "description": "Maximum output size in bytes before truncation.",
                "examples": [1000000]
            },
            "preview_rows": {
                "type": "integer",
                "minimum": 1,
                "description": "Maximum rows to return when ndjson=true.",
                "examples": [100]
            },
            "return_output_json": {
                "type": "boolean",
                "description": "Include parsed output JSON in meta.output when ndjson=false and within size limits.",
                "examples": [false]
            }
        },
        "allOf": [
            {
                "oneOf": [
                    {
                        "required": ["rules_path"],
                        "not": { "required": ["rules_text"] }
                    },
                    {
                        "required": ["rules_text"],
                        "not": { "required": ["rules_path"] }
                    }
                ]
            },
            {
                "oneOf": [
                    {
                        "required": ["input_path"],
                        "not": {
                            "anyOf": [
                                { "required": ["input_text"] },
                                { "required": ["input_json"] }
                            ]
                        }
                    },
                    {
                        "required": ["input_text"],
                        "not": {
                            "anyOf": [
                                { "required": ["input_path"] },
                                { "required": ["input_json"] }
                            ]
                        }
                    },
                    {
                        "required": ["input_json"],
                        "not": {
                            "anyOf": [
                                { "required": ["input_path"] },
                                { "required": ["input_text"] }
                            ]
                        }
                    }
                ]
            },
            {
                "not": { "required": ["context_path", "context_json"] }
            }
        ]
    })
}

fn validate_rules_input_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "rules_path": {
                "type": "string",
                "description": "Path to the YAML rules file. Mutually exclusive with rules_text.",
                "examples": ["rules.yaml"]
            },
            "rules_text": {
                "type": "string",
                "description": "Inline YAML rules content. Mutually exclusive with rules_path.",
                "examples": ["version: 1\ninput:\n  format: json\n  json: {}\nmappings:\n  - target: \"id\"\n    source: \"id\""]
            }
        },
        "allOf": [
            {
                "oneOf": [
                    {
                        "required": ["rules_path"],
                        "not": { "required": ["rules_text"] }
                    },
                    {
                        "required": ["rules_text"],
                        "not": { "required": ["rules_path"] }
                    }
                ]
            }
        ]
    })
}

fn generate_dto_input_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "rules_path": {
                "type": "string",
                "description": "Path to the YAML rules file. Mutually exclusive with rules_text.",
                "examples": ["rules.yaml"]
            },
            "rules_text": {
                "type": "string",
                "description": "Inline YAML rules content. Mutually exclusive with rules_path.",
                "examples": ["version: 1\ninput:\n  format: json\n  json: {}\nmappings:\n  - target: \"id\"\n    source: \"id\""]
            },
            "language": {
                "type": "string",
                "enum": ["rust", "typescript", "python", "go", "java", "kotlin", "swift"],
                "description": "DTO output language.",
                "examples": ["typescript"]
            },
            "name": {
                "type": "string",
                "description": "Optional DTO root type name.",
                "examples": ["Record"]
            }
        },
        "required": ["language"],
        "allOf": [
            {
                "oneOf": [
                    {
                        "required": ["rules_path"],
                        "not": { "required": ["rules_text"] }
                    },
                    {
                        "required": ["rules_text"],
                        "not": { "required": ["rules_path"] }
                    }
                ]
            }
        ]
    })
}

fn list_ops_input_schema() -> Value {
    json!({
        "type": "object",
        "properties": {}
    })
}

fn analyze_input_input_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "input_path": {
                "type": "string",
                "description": "Path to the input CSV/JSON file. Mutually exclusive with input_text and input_json.",
                "examples": ["input.json"]
            },
            "input_text": {
                "type": "string",
                "description": "Inline input text (CSV or JSON). Mutually exclusive with input_path and input_json.",
                "examples": ["{\"items\":[{\"id\":1}]}"]
            },
            "input_json": {
                "type": ["object", "array"],
                "description": "Inline input JSON value. Mutually exclusive with input_path and input_text.",
                "examples": [[{"id": 1}]]
            },
            "format": {
                "type": "string",
                "enum": ["csv", "json"],
                "description": "Input format when input_text/input_path is used.",
                "examples": ["json"]
            },
            "records_path": {
                "type": "string",
                "description": "Optional records path for JSON inputs.",
                "examples": ["items"]
            },
            "max_paths": {
                "type": "integer",
                "minimum": 1,
                "description": "Maximum number of unique paths to include in the response.",
                "examples": [200]
            }
        },
        "allOf": [
            {
                "oneOf": [
                    {
                        "required": ["input_path"],
                        "not": {
                            "anyOf": [
                                { "required": ["input_text"] },
                                { "required": ["input_json"] }
                            ]
                        }
                    },
                    {
                        "required": ["input_text"],
                        "not": {
                            "anyOf": [
                                { "required": ["input_path"] },
                                { "required": ["input_json"] }
                            ]
                        }
                    },
                    {
                        "required": ["input_json"],
                        "not": {
                            "anyOf": [
                                { "required": ["input_path"] },
                                { "required": ["input_text"] }
                            ]
                        }
                    }
                ]
            }
        ]
    })
}

enum CallError {
    InvalidParams(String),
    Tool {
        message: String,
        errors: Option<Vec<Value>>,
    },
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
        "transform" => run_transform_tool(args),
        "validate_rules" => run_validate_rules_tool(args),
        "generate_dto" => run_generate_dto_tool(args),
        "list_ops" => run_list_ops_tool(),
        "analyze_input" => run_analyze_input_tool(args),
        _ => Ok(tool_error_result(&format!("unknown tool: {}", name), None)),
    }
}

fn run_transform_tool(args: &Map<String, Value>) -> Result<Value, CallError> {
    let rules_path = get_optional_string(args, "rules_path").map_err(CallError::InvalidParams)?;
    let rules_text = get_optional_string(args, "rules_text").map_err(CallError::InvalidParams)?;
    let input_path = get_optional_string(args, "input_path").map_err(CallError::InvalidParams)?;
    let input_text = get_optional_string(args, "input_text").map_err(CallError::InvalidParams)?;
    let input_json = get_optional_json_value(args, "input_json").map_err(CallError::InvalidParams)?;
    let context_path = get_optional_string(args, "context_path").map_err(CallError::InvalidParams)?;
    let context_json = get_optional_object(args, "context_json").map_err(CallError::InvalidParams)?;
    let format = get_optional_string(args, "format").map_err(CallError::InvalidParams)?;
    let ndjson = get_optional_bool(args, "ndjson")
        .map_err(CallError::InvalidParams)?
        .unwrap_or(false);
    let validate = get_optional_bool(args, "validate")
        .map_err(CallError::InvalidParams)?
        .unwrap_or(false);
    let output_path = get_optional_string(args, "output_path").map_err(CallError::InvalidParams)?;
    let max_output_bytes =
        get_optional_usize(args, "max_output_bytes").map_err(CallError::InvalidParams)?;
    let preview_rows = get_optional_usize(args, "preview_rows").map_err(CallError::InvalidParams)?;
    let return_output_json = get_optional_bool(args, "return_output_json")
        .map_err(CallError::InvalidParams)?
        .unwrap_or(false);

    let rule_source_count = rules_path.is_some() as u8 + rules_text.is_some() as u8;
    if rule_source_count == 0 {
        return Err(CallError::InvalidParams(
            "rules_path or rules_text is required".to_string(),
        ));
    }
    if rule_source_count > 1 {
        return Err(CallError::InvalidParams(
            "rules_path and rules_text are mutually exclusive".to_string(),
        ));
    }

    let input_source_count =
        input_path.is_some() as u8 + input_text.is_some() as u8 + input_json.is_some() as u8;
    if input_source_count == 0 {
        return Err(CallError::InvalidParams(
            "input_path, input_text, or input_json is required".to_string(),
        ));
    }
    if input_source_count > 1 {
        return Err(CallError::InvalidParams(
            "input_path, input_text, and input_json are mutually exclusive".to_string(),
        ));
    }

    if context_path.is_some() && context_json.is_some() {
        return Err(CallError::InvalidParams(
            "context_path and context_json are mutually exclusive".to_string(),
        ));
    }

    if input_json.is_some()
        && format
            .as_deref()
            .is_some_and(|value| value.eq_ignore_ascii_case("csv"))
    {
        return Err(CallError::InvalidParams(
            "format must be json when input_json is provided".to_string(),
        ));
    }
    if format
        .as_deref()
        .is_some_and(|value| !value.eq_ignore_ascii_case("csv") && !value.eq_ignore_ascii_case("json"))
    {
        return Err(CallError::InvalidParams(
            "format must be csv or json".to_string(),
        ));
    }

    let (mut rule, yaml) = load_rule_from_source(rules_path.as_deref(), rules_text.as_deref())?;

    let input = match (input_path.as_deref(), input_text.as_deref(), input_json.as_ref()) {
        (Some(path), None, None) => fs::read_to_string(path).map_err(|err| {
            let message = format!("failed to read input: {}", err);
            CallError::Tool {
                message: message.clone(),
                errors: Some(vec![io_error_json(&message, Some(path))]),
            }
        })?,
        (None, Some(text), None) => text.to_string(),
        (None, None, Some(value)) => serde_json::to_string(value).map_err(|err| {
            let message = format!("failed to serialize input JSON: {}", err);
            CallError::Tool {
                message: message.clone(),
                errors: Some(vec![parse_error_json(&message, None)]),
            }
        })?,
        _ => {
            return Err(CallError::InvalidParams(
                "input_path, input_text, or input_json is required".to_string(),
            ))
        }
    };

    let context_value = match (context_path.as_deref(), context_json.as_ref()) {
        (Some(path), None) => {
            let data = fs::read_to_string(path).map_err(|err| {
                let message = format!("failed to read context: {}", err);
                CallError::Tool {
                    message: message.clone(),
                    errors: Some(vec![io_error_json(&message, Some(path))]),
                }
            })?;
            Some(serde_json::from_str(&data).map_err(|err| {
                let message = format!("failed to parse context JSON: {}", err);
                CallError::Tool {
                    message: message.clone(),
                    errors: Some(vec![parse_error_json(&message, Some(path))]),
                }
            })?)
        }
        (None, Some(value)) => Some(value.clone()),
        (None, None) => None,
        _ => None,
    };

    let format_override = if input_json.is_some() {
        Some("json".to_string())
    } else {
        format
    };
    apply_format_override(&mut rule, format_override.as_deref())
        .map_err(CallError::InvalidParams)?;

    if validate {
        if let Err(errors) = validate_rule_file_with_source(&rule, &yaml) {
            let error_text = validation_errors_to_text(&errors);
            let error_values = validation_errors_to_values(&errors);
            return Err(CallError::Tool {
                message: error_text,
                errors: Some(error_values),
            });
        }
    }

    let (output_value, output_text, warnings) = if ndjson {
        let (output_text, warnings) = transform_to_ndjson(&rule, &input, context_value.as_ref())?;
        (None, output_text, warnings)
    } else {
        let (output, warnings) =
            transform_with_warnings(&rule, &input, context_value.as_ref()).map_err(|err| {
                CallError::Tool {
                    message: transform_error_to_text(&err),
                    errors: Some(vec![transform_error_json(&err)]),
                }
            })?;
        let output_text = serde_json::to_string(&output).map_err(|err| {
            let message = format!("failed to serialize output JSON: {}", err);
            CallError::Tool {
                message: message.clone(),
                errors: Some(vec![parse_error_json(&message, None)]),
            }
        })?;
        (Some(output), output_text, warnings)
    };

    if let Some(path) = output_path.as_deref() {
        write_output(path, &output_text).map_err(|err| {
            let message = err;
            CallError::Tool {
                message: message.clone(),
                errors: Some(vec![io_error_json(&message, Some(path))]),
            }
        })?;
    }

    let output_bytes = output_text.as_bytes().len();
    let mut response_text = output_text.clone();
    let mut truncated = false;

    if ndjson {
        if let Some(limit) = preview_rows {
            let preview = preview_ndjson(&output_text, limit);
            if preview.len() != output_text.len() {
                truncated = true;
            }
            response_text = preview;
        }
    }

    if let Some(max_bytes) = max_output_bytes {
        if output_bytes > max_bytes {
            truncated = true;
        }
        if response_text.as_bytes().len() > max_bytes {
            response_text = truncate_to_bytes(&response_text, max_bytes).to_string();
            truncated = true;
        }
    }

    let mut result = json!({
        "content": [
            {
                "type": "text",
                "text": response_text
            }
        ]
    });

    let exceeds_max = max_output_bytes.map_or(false, |max| output_bytes > max);
    let mut meta = serde_json::Map::new();
    if !warnings.is_empty() {
        meta.insert("warnings".to_string(), warnings_to_json(&warnings));
    }
    if let Some(path) = output_path {
        meta.insert("output_path".to_string(), json!(path));
    }
    if truncated {
        meta.insert("output_bytes".to_string(), json!(output_bytes));
        meta.insert("truncated".to_string(), json!(true));
    }
    if return_output_json && !ndjson && !exceeds_max {
        if let Some(output) = output_value {
            meta.insert("output".to_string(), output);
        }
    }
    if !meta.is_empty() {
        result["meta"] = Value::Object(meta);
    }

    Ok(result)
}

fn run_validate_rules_tool(args: &Map<String, Value>) -> Result<Value, CallError> {
    let rules_path = get_optional_string(args, "rules_path").map_err(CallError::InvalidParams)?;
    let rules_text = get_optional_string(args, "rules_text").map_err(CallError::InvalidParams)?;

    let rule_source_count = rules_path.is_some() as u8 + rules_text.is_some() as u8;
    if rule_source_count == 0 {
        return Err(CallError::InvalidParams(
            "rules_path or rules_text is required".to_string(),
        ));
    }
    if rule_source_count > 1 {
        return Err(CallError::InvalidParams(
            "rules_path and rules_text are mutually exclusive".to_string(),
        ));
    }

    let (rule, yaml) = load_rule_from_source(rules_path.as_deref(), rules_text.as_deref())?;
    match validate_rule_file_with_source(&rule, &yaml) {
        Ok(_) => Ok(json!({
            "content": [
                {
                    "type": "text",
                    "text": "ok"
                }
            ]
        })),
        Err(errors) => {
            let error_values = validation_errors_to_values(&errors);
            Ok(json!({
                "content": [
                    {
                        "type": "text",
                        "text": "validation failed"
                    }
                ],
                "isError": true,
                "meta": {
                    "errors": error_values
                }
            }))
        }
    }
}

fn run_generate_dto_tool(args: &Map<String, Value>) -> Result<Value, CallError> {
    let rules_path = get_optional_string(args, "rules_path").map_err(CallError::InvalidParams)?;
    let rules_text = get_optional_string(args, "rules_text").map_err(CallError::InvalidParams)?;
    let language = get_optional_string(args, "language").map_err(CallError::InvalidParams)?;
    let name = get_optional_string(args, "name").map_err(CallError::InvalidParams)?;

    let rule_source_count = rules_path.is_some() as u8 + rules_text.is_some() as u8;
    if rule_source_count == 0 {
        return Err(CallError::InvalidParams(
            "rules_path or rules_text is required".to_string(),
        ));
    }
    if rule_source_count > 1 {
        return Err(CallError::InvalidParams(
            "rules_path and rules_text are mutually exclusive".to_string(),
        ));
    }

    let language = language.ok_or_else(|| {
        CallError::InvalidParams("language is required".to_string())
    })?;
    let language = parse_dto_language(&language)
        .map_err(CallError::InvalidParams)?;

    let (rule, _) = load_rule_from_source(rules_path.as_deref(), rules_text.as_deref())?;
    let dto = generate_dto(&rule, language, name.as_deref()).map_err(|err| {
        let message = format!("failed to generate dto: {}", err);
        CallError::Tool {
            message: message.clone(),
            errors: Some(vec![dto_error_json(&message)]),
        }
    })?;

    let mut meta = serde_json::Map::new();
    meta.insert(
        "language".to_string(),
        json!(dto_language_to_str(language)),
    );
    if let Some(name) = name {
        meta.insert("name".to_string(), json!(name));
    }

    Ok(json!({
        "content": [
            {
                "type": "text",
                "text": dto
            }
        ],
        "meta": meta
    }))
}

fn run_list_ops_tool() -> Result<Value, CallError> {
    let ops = json!({
        "expr_ops": [
            "concat",
            "coalesce",
            "to_string",
            "trim",
            "lowercase",
            "uppercase",
            "lookup",
            "lookup_first"
        ],
        "logical_ops": ["and", "or", "not"],
        "comparison_ops": ["==", "!=", "<", "<=", ">", ">=", "~="],
        "type_casts": ["string", "int", "float", "bool"]
    });

    let text = serde_json::to_string_pretty(&ops)
        .unwrap_or_else(|_| "{\"error\":\"failed to serialize ops\"}".to_string());

    Ok(json!({
        "content": [
            {
                "type": "text",
                "text": text
            }
        ],
        "meta": {
            "ops": ops
        }
    }))
}

fn run_analyze_input_tool(args: &Map<String, Value>) -> Result<Value, CallError> {
    let input_path = get_optional_string(args, "input_path").map_err(CallError::InvalidParams)?;
    let input_text = get_optional_string(args, "input_text").map_err(CallError::InvalidParams)?;
    let input_json = get_optional_json_value(args, "input_json").map_err(CallError::InvalidParams)?;
    let format = get_optional_string(args, "format").map_err(CallError::InvalidParams)?;
    let records_path =
        get_optional_string(args, "records_path").map_err(CallError::InvalidParams)?;
    let max_paths = get_optional_usize(args, "max_paths").map_err(CallError::InvalidParams)?;

    let input_source_count =
        input_path.is_some() as u8 + input_text.is_some() as u8 + input_json.is_some() as u8;
    if input_source_count == 0 {
        return Err(CallError::InvalidParams(
            "input_path, input_text, or input_json is required".to_string(),
        ));
    }
    if input_source_count > 1 {
        return Err(CallError::InvalidParams(
            "input_path, input_text, and input_json are mutually exclusive".to_string(),
        ));
    }

    if input_json.is_some()
        && format
            .as_deref()
            .is_some_and(|value| value.eq_ignore_ascii_case("csv"))
    {
        return Err(CallError::InvalidParams(
            "format must be json when input_json is provided".to_string(),
        ));
    }

    let input_text = match (input_path.as_deref(), input_text.as_deref()) {
        (Some(path), None) => fs::read_to_string(path).map_err(|err| {
            let message = format!("failed to read input: {}", err);
            CallError::Tool {
                message: message.clone(),
                errors: Some(vec![io_error_json(&message, Some(path))]),
            }
        })?,
        (None, Some(text)) => text.to_string(),
        (None, None) => String::new(),
        _ => {
            return Err(CallError::InvalidParams(
                "input_path, input_text, or input_json is required".to_string(),
            ))
        }
    };

    let records = if let Some(value) = input_json {
        json_records_from_value(&value, records_path.as_deref())?
    } else {
        match normalize_format(format.as_deref(), &input_text) {
            InputDataFormat::Json => {
                let value = serde_json::from_str(&input_text).map_err(|err| {
                    let message = format!("failed to parse input JSON: {}", err);
                    CallError::Tool {
                        message: message.clone(),
                        errors: Some(vec![parse_error_json(&message, input_path.as_deref())]),
                    }
                })?;
                json_records_from_value(&value, records_path.as_deref())?
            }
            InputDataFormat::Csv => parse_csv_records(&input_text).map_err(|err| {
                let message = format!("failed to parse input CSV: {}", err);
                CallError::Tool {
                    message: message.clone(),
                    errors: Some(vec![parse_error_json(&message, input_path.as_deref())]),
                }
            })?,
        }
    };

    let stats = analyze_records(&records, max_paths);
    let paths_json = stats_to_json(&stats);

    let summary = json!({
        "records": records.len(),
        "paths": stats.len()
    });

    let meta = json!({
        "summary": summary,
        "paths": paths_json
    });
    let text = serde_json::to_string_pretty(&meta)
        .unwrap_or_else(|_| "{\"error\":\"failed to serialize analysis\"}".to_string());

    Ok(json!({
        "content": [
            {
                "type": "text",
                "text": text
            }
        ],
        "meta": meta
    }))
}

fn tool_error_result(message: &str, errors: Option<Vec<Value>>) -> Value {
    let mut result = json!({
        "content": [
            {
                "type": "text",
                "text": message
            }
        ],
        "isError": true
    });

    if let Some(errors) = errors {
        result["meta"] = json!({ "errors": errors });
    }

    result
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

fn get_optional_usize(args: &Map<String, Value>, key: &str) -> Result<Option<usize>, String> {
    match args.get(key) {
        Some(Value::Number(value)) => value
            .as_u64()
            .and_then(|value| {
                if value > 0 {
                    Some(value as usize)
                } else {
                    None
                }
            })
            .ok_or_else(|| format!("{} must be a positive integer", key))
            .map(Some),
        Some(Value::Null) => Ok(None),
        Some(_) => Err(format!("{} must be a positive integer", key)),
        None => Ok(None),
    }
}

fn get_optional_json_value(args: &Map<String, Value>, key: &str) -> Result<Option<Value>, String> {
    match args.get(key) {
        Some(Value::Array(_)) | Some(Value::Object(_)) => Ok(args.get(key).cloned()),
        Some(Value::Null) => Ok(None),
        Some(_) => Err(format!("{} must be an object or array", key)),
        None => Ok(None),
    }
}

fn get_optional_object(args: &Map<String, Value>, key: &str) -> Result<Option<Value>, String> {
    match args.get(key) {
        Some(Value::Object(_)) => Ok(args.get(key).cloned()),
        Some(Value::Null) => Ok(None),
        Some(_) => Err(format!("{} must be an object", key)),
        None => Ok(None),
    }
}

fn load_rule_from_source(
    rules_path: Option<&str>,
    rules_text: Option<&str>,
) -> Result<(RuleFile, String), CallError> {
    match (rules_path, rules_text) {
        (Some(path), None) => {
            let yaml = fs::read_to_string(path).map_err(|err| {
                let message = format!("failed to read rules: {}", err);
                CallError::Tool {
                    message: message.clone(),
                    errors: Some(vec![io_error_json(&message, Some(path))]),
                }
            })?;
            let rule = parse_rule_file(&yaml).map_err(|err| {
                let message = format!("failed to parse rules: {}", err);
                CallError::Tool {
                    message: message.clone(),
                    errors: Some(vec![parse_error_json(&message, Some(path))]),
                }
            })?;
            Ok((rule, yaml))
        }
        (None, Some(text)) => {
            let rule = parse_rule_file(text).map_err(|err| {
                let message = format!("failed to parse rules: {}", err);
                CallError::Tool {
                    message: message.clone(),
                    errors: Some(vec![parse_error_json(&message, None)]),
                }
            })?;
            Ok((rule, text.to_string()))
        }
        _ => Err(CallError::InvalidParams(
            "rules_path or rules_text is required".to_string(),
        )),
    }
}

fn parse_dto_language(value: &str) -> Result<DtoLanguage, String> {
    match value.to_lowercase().as_str() {
        "rust" => Ok(DtoLanguage::Rust),
        "typescript" => Ok(DtoLanguage::TypeScript),
        "python" => Ok(DtoLanguage::Python),
        "go" => Ok(DtoLanguage::Go),
        "java" => Ok(DtoLanguage::Java),
        "kotlin" => Ok(DtoLanguage::Kotlin),
        "swift" => Ok(DtoLanguage::Swift),
        _ => Err("language must be one of rust, typescript, python, go, java, kotlin, swift"
            .to_string()),
    }
}

fn dto_language_to_str(language: DtoLanguage) -> &'static str {
    match language {
        DtoLanguage::Rust => "rust",
        DtoLanguage::TypeScript => "typescript",
        DtoLanguage::Python => "python",
        DtoLanguage::Go => "go",
        DtoLanguage::Java => "java",
        DtoLanguage::Kotlin => "kotlin",
        DtoLanguage::Swift => "swift",
    }
}

fn dto_error_json(message: &str) -> Value {
    json!({
        "type": "dto",
        "message": message,
    })
}

#[derive(Clone, Copy)]
enum InputDataFormat {
    Json,
    Csv,
}

fn normalize_format(format: Option<&str>, input_text: &str) -> InputDataFormat {
    match format.map(|value| value.to_lowercase()) {
        Some(value) if value == "csv" => InputDataFormat::Csv,
        Some(value) if value == "json" => InputDataFormat::Json,
        Some(_) => InputDataFormat::Json,
        None => match input_text.trim_start().chars().next() {
            Some('{') | Some('[') => InputDataFormat::Json,
            _ => InputDataFormat::Csv,
        },
    }
}

fn json_records_from_value(
    value: &Value,
    records_path: Option<&str>,
) -> Result<Vec<Value>, CallError> {
    let target = if let Some(path) = records_path {
        let tokens = parse_path_tokens(path).map_err(|message| {
            CallError::InvalidParams(format!("records_path is invalid: {}", message))
        })?;
        get_value_by_tokens(value, &tokens).ok_or_else(|| {
            CallError::Tool {
                message: "records_path did not match any value".to_string(),
                errors: Some(vec![parse_error_json(
                    "records_path did not match any value",
                    None,
                )]),
            }
        })?
    } else {
        value
    };

    match target {
        Value::Array(items) => Ok(items.clone()),
        Value::Object(_) => Ok(vec![target.clone()]),
        _ => Err(CallError::Tool {
            message: "records_path must resolve to an object or array".to_string(),
            errors: Some(vec![parse_error_json(
                "records_path must resolve to an object or array",
                None,
            )]),
        }),
    }
}

fn parse_csv_records(text: &str) -> Result<Vec<Value>, String> {
    let mut reader = ReaderBuilder::new()
        .has_headers(true)
        .from_reader(text.as_bytes());
    let headers = reader
        .headers()
        .map_err(|err| err.to_string())?
        .iter()
        .enumerate()
        .map(|(index, name)| {
            let trimmed = name.trim();
            if trimmed.is_empty() {
                format!("column_{}", index + 1)
            } else {
                trimmed.to_string()
            }
        })
        .collect::<Vec<_>>();

    let mut records = Vec::new();
    for result in reader.records() {
        let record = result.map_err(|err| err.to_string())?;
        let mut obj = Map::new();
        for (index, value) in record.iter().enumerate() {
            if let Some(key) = headers.get(index) {
                obj.insert(key.clone(), csv_cell_to_value(value));
            }
        }
        records.push(Value::Object(obj));
    }
    Ok(records)
}

fn csv_cell_to_value(value: &str) -> Value {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Value::Null;
    }
    let lower = trimmed.to_ascii_lowercase();
    if lower == "true" {
        return Value::Bool(true);
    }
    if lower == "false" {
        return Value::Bool(false);
    }
    if let Ok(number) = trimmed.parse::<i64>() {
        return Value::Number(number.into());
    }
    if let Ok(number) = trimmed.parse::<f64>() {
        if let Some(number) = serde_json::Number::from_f64(number) {
            return Value::Number(number);
        }
    }
    Value::String(trimmed.to_string())
}

#[derive(Default)]
struct PathStats {
    count: usize,
    type_counts: HashMap<&'static str, usize>,
    examples: Vec<Value>,
}

fn analyze_records(records: &[Value], max_paths: Option<usize>) -> HashMap<String, PathStats> {
    let mut stats = HashMap::new();
    for record in records {
        collect_path_stats(record, "", &mut stats, max_paths);
    }
    stats
}

fn collect_path_stats(
    value: &Value,
    prefix: &str,
    stats: &mut HashMap<String, PathStats>,
    max_paths: Option<usize>,
) {
    match value {
        Value::Object(map) => {
            if map.is_empty() {
                record_path_value(stats, prefix, value, max_paths);
                return;
            }
            for (key, child) in map {
                let next = append_path(prefix, key);
                collect_path_stats(child, &next, stats, max_paths);
            }
        }
        Value::Array(_) => {
            record_path_value(stats, prefix, value, max_paths);
        }
        _ => record_path_value(stats, prefix, value, max_paths),
    }
}

fn record_path_value(
    stats: &mut HashMap<String, PathStats>,
    path: &str,
    value: &Value,
    max_paths: Option<usize>,
) {
    let path = if path.is_empty() {
        "$".to_string()
    } else {
        path.to_string()
    };
    if !stats.contains_key(&path) && max_paths.is_some_and(|max| stats.len() >= max) {
        return;
    }
    let entry = stats.entry(path).or_default();
    entry.count += 1;
    let type_name = value_type_name(value);
    *entry.type_counts.entry(type_name).or_insert(0) += 1;
    if entry.examples.len() < 3 && is_primitive(value) && !entry.examples.contains(value) {
        entry.examples.push(value.clone());
    }
}

fn stats_to_json(stats: &HashMap<String, PathStats>) -> Value {
    let mut entries: Vec<(&String, &PathStats)> = stats.iter().collect();
    entries.sort_by(|a, b| a.0.cmp(b.0));

    let mut values = Vec::new();
    for (path, stat) in entries {
        let mut types = serde_json::Map::new();
        let mut type_entries: Vec<_> = stat.type_counts.iter().collect();
        type_entries.sort_by(|a, b| a.0.cmp(b.0));
        for (type_name, count) in type_entries {
            types.insert(type_name.to_string(), json!(count));
        }

        let mut obj = json!({
            "path": path,
            "count": stat.count,
            "types": types
        });
        if !stat.examples.is_empty() {
            obj["examples"] = Value::Array(stat.examples.clone());
        }
        values.push(obj);
    }
    Value::Array(values)
}

fn value_type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

fn is_primitive(value: &Value) -> bool {
    matches!(
        value,
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_)
    )
}

fn append_path(prefix: &str, key: &str) -> String {
    let needs_quote = key
        .chars()
        .any(|ch| ch == '.' || ch == '[' || ch == ']' || ch == '"' || ch == '\'' || ch == '\\');
    let segment = if needs_quote {
        let escaped = key.replace('\\', "\\\\").replace('"', "\\\"");
        format!("[\"{}\"]", escaped)
    } else {
        key.to_string()
    };
    if prefix.is_empty() {
        segment
    } else if segment.starts_with('[') {
        format!("{}{}", prefix, segment)
    } else {
        format!("{}.{}", prefix, segment)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PathToken {
    Key(String),
    Index(usize),
}

fn parse_path_tokens(path: &str) -> Result<Vec<PathToken>, String> {
    if path.is_empty() {
        return Err("path is empty".to_string());
    }

    let chars: Vec<char> = path.chars().collect();
    let mut tokens = Vec::new();
    let mut index = 0;

    while index < chars.len() {
        if chars[index] == '.' {
            return Err("path segment is empty".to_string());
        }

        if chars[index] == '[' {
            let (token, next) = parse_bracket(&chars, index)?;
            tokens.push(token);
            index = next;
        } else {
            let start = index;
            while index < chars.len() && chars[index] != '.' && chars[index] != '[' {
                index += 1;
            }
            if start == index {
                return Err("path segment is empty".to_string());
            }
            let key: String = chars[start..index].iter().collect();
            if key.is_empty() {
                return Err("path segment is empty".to_string());
            }
            tokens.push(PathToken::Key(key));
        }

        while index < chars.len() && chars[index] == '[' {
            let (token, next) = parse_bracket(&chars, index)?;
            tokens.push(token);
            index = next;
        }

        if index < chars.len() {
            if chars[index] == '.' {
                index += 1;
                if index == chars.len() {
                    return Err("path syntax is invalid".to_string());
                }
            } else {
                return Err("path syntax is invalid".to_string());
            }
        }
    }

    Ok(tokens)
}

fn parse_bracket(chars: &[char], start: usize) -> Result<(PathToken, usize), String> {
    if chars.get(start) != Some(&'[') {
        return Err("path syntax is invalid".to_string());
    }
    let index = start + 1;
    if index >= chars.len() {
        return Err("path syntax is invalid".to_string());
    }

    match chars[index] {
        '"' | '\'' => parse_quoted(chars, index),
        c if c.is_ascii_digit() => parse_index(chars, index),
        _ => Err("path syntax is invalid".to_string()),
    }
}

fn parse_index(chars: &[char], start: usize) -> Result<(PathToken, usize), String> {
    let mut index = start;
    let mut value: usize = 0;
    let mut has_digit = false;

    while index < chars.len() && chars[index].is_ascii_digit() {
        has_digit = true;
        value = value
            .saturating_mul(10)
            .saturating_add(chars[index].to_digit(10).unwrap_or(0) as usize);
        index += 1;
    }

    if !has_digit {
        return Err("path syntax is invalid".to_string());
    }
    if chars.get(index) != Some(&']') {
        return Err("path syntax is invalid".to_string());
    }
    index += 1;
    Ok((PathToken::Index(value), index))
}

fn parse_quoted(chars: &[char], start: usize) -> Result<(PathToken, usize), String> {
    let quote = chars[start];
    let mut index = start + 1;
    let mut value = String::new();

    while index < chars.len() {
        let ch = chars[index];
        if ch == '\\' {
            index += 1;
            if index >= chars.len() {
                return Err("path escape is invalid".to_string());
            }
            let escaped = chars[index];
            if escaped == '\\' || escaped == quote {
                value.push(escaped);
                index += 1;
                continue;
            }
            return Err("path escape is invalid".to_string());
        }

        if ch == '[' || ch == ']' {
            return Err("path syntax is invalid".to_string());
        }

        if ch == quote {
            index += 1;
            break;
        }

        value.push(ch);
        index += 1;
    }

    if value.is_empty() {
        return Err("path segment is empty".to_string());
    }
    if chars.get(index - 1) != Some(&quote) {
        return Err("path syntax is invalid".to_string());
    }
    if chars.get(index) != Some(&']') {
        return Err("path syntax is invalid".to_string());
    }
    index += 1;
    Ok((PathToken::Key(value), index))
}

fn get_value_by_tokens<'a>(value: &'a Value, tokens: &[PathToken]) -> Option<&'a Value> {
    let mut current = value;
    for token in tokens {
        match token {
            PathToken::Key(key) => match current {
                Value::Object(map) => current = map.get(key)?,
                _ => return None,
            },
            PathToken::Index(index) => match current {
                Value::Array(items) => current = items.get(*index)?,
                _ => return None,
            },
        }
    }
    Some(current)
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

fn transform_to_ndjson(
    rule: &RuleFile,
    input: &str,
    context: Option<&serde_json::Value>,
) -> Result<(String, Vec<TransformWarning>), CallError> {
    let stream = transform_stream(rule, input, context).map_err(|err| CallError::Tool {
        message: transform_error_to_text(&err),
        errors: Some(vec![transform_error_json(&err)]),
    })?;
    let mut output = String::new();
    let mut warnings = Vec::new();

    for item in stream {
        let item = item.map_err(|err| CallError::Tool {
            message: transform_error_to_text(&err),
            errors: Some(vec![transform_error_json(&err)]),
        })?;
        warnings.extend(item.warnings);
        let line = serde_json::to_string(&item.output).map_err(|err| {
            let message = format!("failed to serialize output JSON: {}", err);
            CallError::Tool {
                message: message.clone(),
                errors: Some(vec![parse_error_json(&message, None)]),
            }
        })?;
        output.push_str(&line);
        output.push('\n');
    }

    Ok((output, warnings))
}

fn validation_errors_to_text(errors: &[RuleError]) -> String {
    let values = validation_errors_to_values(errors);
    serde_json::to_string(&values).unwrap_or_else(|_| "validation error".to_string())
}

fn validation_errors_to_values(errors: &[RuleError]) -> Vec<Value> {
    errors.iter().map(validation_error_json).collect()
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

fn parse_error_json(message: &str, path: Option<&str>) -> Value {
    let mut value = json!({
        "type": "parse",
        "message": message,
    });
    if let Some(path) = path {
        value["path"] = json!(path);
    }
    value
}

fn io_error_json(message: &str, path: Option<&str>) -> Value {
    let mut value = json!({
        "type": "io",
        "message": message,
    });
    if let Some(path) = path {
        value["path"] = json!(path);
    }
    value
}

fn truncate_to_bytes(text: &str, max_bytes: usize) -> &str {
    if text.len() <= max_bytes {
        return text;
    }
    let mut end = max_bytes;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

fn preview_ndjson(text: &str, max_rows: usize) -> String {
    let mut preview = String::new();
    for (index, line) in text.split_terminator('\n').enumerate() {
        if index >= max_rows {
            break;
        }
        preview.push_str(line);
        preview.push('\n');
    }
    preview
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
