use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{self, BufRead, BufReader, Write};

use csv::ReaderBuilder;
use serde_json::{json, Map, Value};
use serde_yaml::{Mapping as YamlMapping, Value as YamlValue};
use transform_rules::{
    generate_dto, parse_rule_file, transform_stream, transform_with_warnings,
    validate_rule_file_with_source, DtoLanguage, Expr, ExprChain, ExprOp, InputFormat, RuleError,
    RuleFile, TransformError, TransformErrorKind, TransformWarning,
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
            },
            {
                "name": "generate_rules_from_base",
                "description": "Generate rules by mapping input data to existing rule targets.",
                "inputSchema": generate_rules_from_base_input_schema()
            },
            {
                "name": "generate_rules_from_dto",
                "description": "Generate rules by mapping input data to a DTO schema.",
                "inputSchema": generate_rules_from_dto_input_schema()
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
        }
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
        }
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
        "required": ["language"]
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
        }
    })
}

fn generate_rules_from_base_input_schema() -> Value {
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
            "format": {
                "type": "string",
                "enum": ["csv", "json"],
                "description": "Override input format.",
                "examples": ["json"]
            },
            "records_path": {
                "type": "string",
                "description": "Optional records path for JSON inputs.",
                "examples": ["items"]
            },
            "max_candidates": {
                "type": "integer",
                "minimum": 1,
                "description": "Maximum number of candidates to return per target.",
                "examples": [3]
            }
        }
    })
}

fn generate_rules_from_dto_input_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "dto_text": {
                "type": "string",
                "description": "DTO source text.",
                "examples": ["export interface Record { id: string; }"]
            },
            "dto_language": {
                "type": "string",
                "enum": ["rust", "typescript", "python", "go", "java", "kotlin", "swift"],
                "description": "DTO language.",
                "examples": ["typescript"]
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
            "format": {
                "type": "string",
                "enum": ["csv", "json"],
                "description": "Override input format.",
                "examples": ["json"]
            },
            "records_path": {
                "type": "string",
                "description": "Optional records path for JSON inputs.",
                "examples": ["items"]
            },
            "max_candidates": {
                "type": "integer",
                "minimum": 1,
                "description": "Maximum number of candidates to return per target.",
                "examples": [3]
            }
        },
        "required": ["dto_text", "dto_language"]
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
        "generate_rules_from_base" => run_generate_rules_from_base_tool(args),
        "generate_rules_from_dto" => run_generate_rules_from_dto_tool(args),
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
        Ok(_) => {
            let warnings = collect_rule_warnings(&rule);
            let mut result = json!({
                "content": [
                    {
                        "type": "text",
                        "text": "ok"
                    }
                ]
            });
            if !warnings.is_empty() {
                result["meta"] = json!({
                    "warnings": rule_warnings_to_json(&warnings)
                });
            }
            Ok(result)
        }
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
            "replace",
            "split",
            "pad_start",
            "pad_end",
            "lookup",
            "lookup_first",
            "+",
            "-",
            "*",
            "/",
            "round",
            "to_base",
            "date_format",
            "to_unixtime"
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

fn run_generate_rules_from_base_tool(args: &Map<String, Value>) -> Result<Value, CallError> {
    let rules_path = get_optional_string(args, "rules_path").map_err(CallError::InvalidParams)?;
    let rules_text = get_optional_string(args, "rules_text").map_err(CallError::InvalidParams)?;
    let input_path = get_optional_string(args, "input_path").map_err(CallError::InvalidParams)?;
    let input_text = get_optional_string(args, "input_text").map_err(CallError::InvalidParams)?;
    let input_json = get_optional_json_value(args, "input_json").map_err(CallError::InvalidParams)?;
    let format = get_optional_string(args, "format").map_err(CallError::InvalidParams)?;
    let records_path =
        get_optional_string(args, "records_path").map_err(CallError::InvalidParams)?;
    let max_candidates =
        get_optional_usize(args, "max_candidates").map_err(CallError::InvalidParams)?;

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

    let (rule, yaml) = load_rule_from_source(rules_path.as_deref(), rules_text.as_deref())?;
    let mut yaml_value: YamlValue = serde_yaml::from_str(&yaml).map_err(|err| {
        let message = format!("failed to parse rules yaml: {}", err);
        CallError::Tool {
            message: message.clone(),
            errors: Some(vec![parse_error_json(&message, None)]),
        }
    })?;

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

    let records_path = records_path.or_else(|| {
        rule.input
            .json
            .as_ref()
            .and_then(|json| json.records_path.clone())
    });

    let parse_format = if input_json.is_some() {
        InputDataFormat::Json
    } else if let Some(format) = format.as_deref() {
        if format.eq_ignore_ascii_case("csv") {
            InputDataFormat::Csv
        } else {
            InputDataFormat::Json
        }
    } else {
        match rule.input.format {
            InputFormat::Csv => InputDataFormat::Csv,
            InputFormat::Json => InputDataFormat::Json,
        }
    };

    let has_input_json = input_json.is_some();
    let records = match (parse_format, input_json) {
        (InputDataFormat::Json, Some(value)) => {
            json_records_from_value(&value, records_path.as_deref())?
        }
        (InputDataFormat::Json, None) => {
            let value = serde_json::from_str(&input_text).map_err(|err| {
                let message = format!("failed to parse input JSON: {}", err);
                CallError::Tool {
                    message: message.clone(),
                    errors: Some(vec![parse_error_json(&message, input_path.as_deref())]),
                }
            })?;
            json_records_from_value(&value, records_path.as_deref())?
        }
        (InputDataFormat::Csv, _) => parse_csv_records(&input_text).map_err(|err| {
            let message = format!("failed to parse input CSV: {}", err);
            CallError::Tool {
                message: message.clone(),
                errors: Some(vec![parse_error_json(&message, input_path.as_deref())]),
            }
        })?,
    };

    let format_override = if has_input_json {
        Some("json".to_string())
    } else {
        format
    };
    let format_for_yaml = if format_override.is_some() {
        format_override.as_deref()
    } else if records_path.is_some() {
        Some("json")
    } else {
        None
    };
    update_yaml_input_spec(&mut yaml_value, format_for_yaml, records_path.as_deref());

    let stats = analyze_records(&records, None);
    let input_paths = build_input_paths(&stats);
    let input_path_set: HashSet<String> =
        input_paths.iter().map(|info| info.path.clone()).collect();

    let max_candidates = max_candidates.unwrap_or(3);
    let mut candidates_meta = Vec::new();
    let mut unmapped = Vec::new();
    let mut missing_refs = Vec::new();
    let mut missing_ref_set = HashSet::new();
    let mut mapped = 0usize;
    let mut with_expr = 0usize;
    let mut with_value = 0usize;

    let mappings = yaml_mappings_sequence_mut(&mut yaml_value)?;

    for (index, mapping) in rule.mappings.iter().enumerate() {
        collect_missing_refs(
            &mapping.target,
            mapping.expr.as_ref(),
            mapping.when.as_ref(),
            &input_path_set,
            &mut missing_refs,
            &mut missing_ref_set,
        );
        if mapping.expr.is_some() {
            with_expr += 1;
            continue;
        }
        if mapping.value.is_some() {
            with_value += 1;
            continue;
        }

        let target_leaf = leaf_from_path(&mapping.target).unwrap_or_default();
        let candidates = select_candidates(
            &target_leaf,
            mapping.source.as_deref(),
            mapping.value_type.as_deref(),
            &input_paths,
            max_candidates,
        );
        let selected = candidates.first().cloned();

        if let Some(selected) = selected.as_ref() {
            mapped += 1;
            update_yaml_mapping(mappings, index, Some(&selected.source))?;
        } else {
            unmapped.push(mapping.target.clone());
            update_yaml_mapping(mappings, index, None)?;
        }

        let candidates_json: Vec<Value> = candidates
            .iter()
            .map(|candidate| {
                json!({
                    "source": candidate.source,
                    "score": candidate.score,
                    "reason": candidate.reason,
                    "confidence": candidate.confidence
                })
            })
            .collect();
        let mut entry = json!({
            "target": mapping.target,
            "candidates": candidates_json
        });
        if let Some(selected) = selected {
            entry["selected"] = json!(selected.source);
            entry["confidence"] = json!(selected.confidence);
        }
        candidates_meta.push(entry);
    }

    let output_text = serde_yaml::to_string(&yaml_value).map_err(|err| {
        let message = format!("failed to serialize rules yaml: {}", err);
        CallError::Tool {
            message: message.clone(),
            errors: Some(vec![parse_error_json(&message, None)]),
        }
    })?;

    let mut meta = serde_json::Map::new();
    meta.insert(
        "summary".to_string(),
        json!({
            "total": rule.mappings.len(),
            "mapped": mapped,
            "unmapped": unmapped.len(),
            "with_expr": with_expr,
            "with_value": with_value
        }),
    );
    meta.insert("candidates".to_string(), Value::Array(candidates_meta));
    if !unmapped.is_empty() {
        meta.insert("unmapped".to_string(), json!(unmapped));
    }
    if !missing_refs.is_empty() {
        meta.insert("missing_refs".to_string(), Value::Array(missing_refs));
    }

    Ok(json!({
        "content": [
            {
                "type": "text",
                "text": output_text
            }
        ],
        "meta": meta
    }))
}

fn run_generate_rules_from_dto_tool(args: &Map<String, Value>) -> Result<Value, CallError> {
    let dto_text = get_optional_string(args, "dto_text").map_err(CallError::InvalidParams)?;
    let dto_language = get_optional_string(args, "dto_language").map_err(CallError::InvalidParams)?;
    let input_path = get_optional_string(args, "input_path").map_err(CallError::InvalidParams)?;
    let input_text = get_optional_string(args, "input_text").map_err(CallError::InvalidParams)?;
    let input_json = get_optional_json_value(args, "input_json").map_err(CallError::InvalidParams)?;
    let format = get_optional_string(args, "format").map_err(CallError::InvalidParams)?;
    let records_path =
        get_optional_string(args, "records_path").map_err(CallError::InvalidParams)?;
    let max_candidates =
        get_optional_usize(args, "max_candidates").map_err(CallError::InvalidParams)?;

    let dto_text = dto_text.ok_or_else(|| {
        CallError::InvalidParams("dto_text is required".to_string())
    })?;
    let dto_language = dto_language.ok_or_else(|| {
        CallError::InvalidParams("dto_language is required".to_string())
    })?;
    let dto_language = parse_dto_source_language(&dto_language)
        .map_err(CallError::InvalidParams)?;

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
    if format
        .as_deref()
        .is_some_and(|value| !value.eq_ignore_ascii_case("csv") && !value.eq_ignore_ascii_case("json"))
    {
        return Err(CallError::InvalidParams(
            "format must be csv or json".to_string(),
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

    let has_input_json = input_json.is_some();
    let parse_format = if has_input_json {
        InputDataFormat::Json
    } else if let Some(format) = format.as_deref() {
        if format.eq_ignore_ascii_case("csv") {
            InputDataFormat::Csv
        } else {
            InputDataFormat::Json
        }
    } else {
        normalize_format(None, &input_text)
    };

    let records = match (parse_format, input_json) {
        (InputDataFormat::Json, Some(value)) => {
            json_records_from_value(&value, records_path.as_deref())?
        }
        (InputDataFormat::Json, None) => {
            let value = serde_json::from_str(&input_text).map_err(|err| {
                let message = format!("failed to parse input JSON: {}", err);
                CallError::Tool {
                    message: message.clone(),
                    errors: Some(vec![parse_error_json(&message, input_path.as_deref())]),
                }
            })?;
            json_records_from_value(&value, records_path.as_deref())?
        }
        (InputDataFormat::Csv, _) => parse_csv_records(&input_text).map_err(|err| {
            let message = format!("failed to parse input CSV: {}", err);
            CallError::Tool {
                message: message.clone(),
                errors: Some(vec![parse_error_json(&message, input_path.as_deref())]),
            }
        })?,
    };

    let schema = parse_dto_schema(&dto_text, dto_language).map_err(|message| {
        CallError::Tool {
            message: message.clone(),
            errors: Some(vec![dto_error_json(&message)]),
        }
    })?;
    let generated = generate_mappings_from_schema(&schema).map_err(|message| {
        CallError::Tool {
            message: message.clone(),
            errors: Some(vec![dto_error_json(&message)]),
        }
    })?;

    let stats = analyze_records(&records, None);
    let input_paths = build_input_paths(&stats);
    let max_candidates = max_candidates.unwrap_or(3);

    let mut candidates_meta = Vec::new();
    let mut unmapped = Vec::new();
    let mut mapped = 0usize;

    let mut mappings_yaml = Vec::new();
    for mapping in &generated {
        let target_leaf = leaf_from_path(&mapping.target).unwrap_or_default();
        let candidates =
            select_candidates(&target_leaf, None, mapping.value_type.as_deref(), &input_paths, max_candidates);
        let selected = candidates.first().cloned();

        let mut mapping_map = YamlMapping::new();
        mapping_map.insert(yaml_key("target"), YamlValue::String(mapping.target.clone()));
        if let Some(value_type) = mapping.value_type.as_deref() {
            mapping_map.insert(yaml_key("type"), YamlValue::String(value_type.to_string()));
        }
        if let Some(selected) = selected.as_ref() {
            mapped += 1;
            mapping_map.insert(
                yaml_key("source"),
                YamlValue::String(selected.source.clone()),
            );
            if mapping.required {
                mapping_map.insert(yaml_key("required"), YamlValue::Bool(true));
            }
        } else {
            unmapped.push(mapping.target.clone());
            mapping_map.insert(yaml_key("value"), YamlValue::Null);
            mapping_map.insert(yaml_key("required"), YamlValue::Bool(false));
        }
        mappings_yaml.push(YamlValue::Mapping(mapping_map));

        let candidates_json: Vec<Value> = candidates
            .iter()
            .map(|candidate| {
                json!({
                    "source": candidate.source,
                    "score": candidate.score,
                    "reason": candidate.reason,
                    "confidence": candidate.confidence
                })
            })
            .collect();
        let mut entry = json!({
            "target": mapping.target,
            "candidates": candidates_json
        });
        if let Some(selected) = selected {
            entry["selected"] = json!(selected.source);
            entry["confidence"] = json!(selected.confidence);
        }
        candidates_meta.push(entry);
    }

    let format_str = if has_input_json {
        "json".to_string()
    } else if let Some(format) = format.as_deref() {
        if format.eq_ignore_ascii_case("csv") {
            "csv".to_string()
        } else {
            "json".to_string()
        }
    } else {
        match parse_format {
            InputDataFormat::Csv => "csv".to_string(),
            InputDataFormat::Json => "json".to_string(),
        }
    };

    let input_yaml = build_input_yaml(&format_str, records_path.as_deref());
    let mut root = YamlMapping::new();
    root.insert(yaml_key("version"), YamlValue::Number(1.into()));
    root.insert(yaml_key("input"), input_yaml);
    root.insert(yaml_key("mappings"), YamlValue::Sequence(mappings_yaml));
    let yaml_value = YamlValue::Mapping(root);
    let output_text = serde_yaml::to_string(&yaml_value).map_err(|err| {
        let message = format!("failed to serialize rules yaml: {}", err);
        CallError::Tool {
            message: message.clone(),
            errors: Some(vec![parse_error_json(&message, None)]),
        }
    })?;

    let mut meta = serde_json::Map::new();
    meta.insert(
        "summary".to_string(),
        json!({
            "total": generated.len(),
            "mapped": mapped,
            "unmapped": unmapped.len()
        }),
    );
    meta.insert("candidates".to_string(), Value::Array(candidates_meta));
    if !unmapped.is_empty() {
        meta.insert("unmapped".to_string(), json!(unmapped));
    }

    Ok(json!({
        "content": [
            {
                "type": "text",
                "text": output_text
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

#[derive(Clone, Copy)]
enum DtoSourceLanguage {
    Rust,
    TypeScript,
    Python,
    Go,
    Java,
    Kotlin,
    Swift,
}

fn parse_dto_source_language(value: &str) -> Result<DtoSourceLanguage, String> {
    match value.to_lowercase().as_str() {
        "rust" => Ok(DtoSourceLanguage::Rust),
        "typescript" => Ok(DtoSourceLanguage::TypeScript),
        "python" => Ok(DtoSourceLanguage::Python),
        "go" => Ok(DtoSourceLanguage::Go),
        "java" => Ok(DtoSourceLanguage::Java),
        "kotlin" => Ok(DtoSourceLanguage::Kotlin),
        "swift" => Ok(DtoSourceLanguage::Swift),
        _ => Err("dto_language must be rust, typescript, python, go, java, kotlin, or swift"
            .to_string()),
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

#[derive(Clone)]
struct InputPathInfo {
    path: String,
    leaf: String,
    tokens: Vec<String>,
    type_counts: HashMap<&'static str, usize>,
}

#[derive(Clone)]
struct Candidate {
    source: String,
    score: f64,
    reason: &'static str,
    confidence: &'static str,
}

fn build_input_paths(stats: &HashMap<String, PathStats>) -> Vec<InputPathInfo> {
    let mut paths = Vec::new();
    for (path, stat) in stats {
        if path == "$" {
            continue;
        }
        let leaf = leaf_from_path(path).unwrap_or_else(|| path.clone());
        let tokens = split_tokens(&leaf);
        paths.push(InputPathInfo {
            path: path.clone(),
            leaf,
            tokens,
            type_counts: stat.type_counts.clone(),
        });
    }
    paths
}

fn leaf_from_path(path: &str) -> Option<String> {
    match parse_path_tokens(path) {
        Ok(tokens) => {
            for token in tokens.iter().rev() {
                if let PathToken::Key(key) = token {
                    return Some(key.clone());
                }
            }
            None
        }
        Err(_) => Some(path.to_string()),
    }
}

fn split_tokens(value: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() {
            current.push(ch.to_ascii_lowercase());
        } else if !current.is_empty() {
            tokens.push(current.clone());
            current.clear();
        }
    }
    if !current.is_empty() {
        tokens.push(current);
    }
    tokens
}

fn token_similarity(a: &[String], b: &[String]) -> f64 {
    if a.is_empty() || b.is_empty() {
        return 0.0;
    }
    let set_a: HashSet<&str> = a.iter().map(String::as_str).collect();
    let set_b: HashSet<&str> = b.iter().map(String::as_str).collect();
    let overlap = set_a.intersection(&set_b).count() as f64;
    let denom = set_a.len().max(set_b.len()) as f64;
    if denom == 0.0 {
        0.0
    } else {
        overlap / denom
    }
}

fn select_candidates(
    target_leaf: &str,
    source_hint: Option<&str>,
    value_type: Option<&str>,
    input_paths: &[InputPathInfo],
    max_candidates: usize,
) -> Vec<Candidate> {
    let mut candidates = Vec::new();
    let target_tokens = split_tokens(target_leaf);
    let source_leaf = source_hint.and_then(leaf_from_path);
    let source_tokens = source_leaf
        .as_deref()
        .map(split_tokens)
        .unwrap_or_default();

    for input in input_paths {
        let mut score = 0.0;
        let mut reason = None;

        if let Some(source_hint) = source_hint {
            if input.path == source_hint {
                score = 1.0;
                reason = Some("exact_source");
            }
        }

        if reason.is_none() && !target_leaf.is_empty() {
            if input.leaf.eq_ignore_ascii_case(target_leaf) {
                score = 0.8;
                reason = Some("leaf_match");
            }
        }

        if reason.is_none() {
            if let Some(source_leaf) = source_leaf.as_deref() {
                if input.leaf.eq_ignore_ascii_case(source_leaf) {
                    score = 0.75;
                    reason = Some("leaf_match");
                }
            }
        }

        if reason.is_none() {
            let mut similarity = token_similarity(&target_tokens, &input.tokens);
            if !source_tokens.is_empty() {
                similarity = similarity.max(token_similarity(&source_tokens, &input.tokens));
            }
            if similarity > 0.0 {
                score = 0.6 * similarity;
                reason = Some("token_match");
            }
        }

        if let Some(reason) = reason {
            score += type_boost(&input.type_counts, value_type);
            let confidence = confidence_for_score(score);
            candidates.push(Candidate {
                source: input.path.clone(),
                score,
                reason,
                confidence,
            });
        }
    }

    candidates.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.source.cmp(&b.source))
    });
    candidates.truncate(max_candidates);
    candidates
}

fn type_boost(type_counts: &HashMap<&'static str, usize>, value_type: Option<&str>) -> f64 {
    let Some(value_type) = value_type else { return 0.0 };
    let type_name = match value_type {
        "string" => "string",
        "int" | "float" => "number",
        "bool" => "bool",
        _ => return 0.0,
    };
    if type_counts.contains_key(type_name) {
        0.1
    } else {
        0.0
    }
}

fn confidence_for_score(score: f64) -> &'static str {
    if score >= 0.9 {
        "high"
    } else if score >= 0.7 {
        "medium"
    } else {
        "low"
    }
}

struct DtoSchema {
    root: String,
    types: HashMap<String, DtoType>,
}

struct DtoType {
    fields: Vec<DtoField>,
}

struct DtoField {
    json_key: String,
    field_type: DtoFieldType,
    optional: bool,
}

enum DtoFieldType {
    Primitive(PrimitiveKind),
    Object(String),
    Unknown,
}

enum PrimitiveKind {
    String,
    Int,
    Float,
    Bool,
}

struct GeneratedMapping {
    target: String,
    value_type: Option<String>,
    required: bool,
}

fn parse_dto_schema(text: &str, language: DtoSourceLanguage) -> Result<DtoSchema, String> {
    let (types, order) = match language {
        DtoSourceLanguage::TypeScript => parse_typescript_types(text)?,
        DtoSourceLanguage::Rust => parse_rust_types(text)?,
        DtoSourceLanguage::Python => parse_python_types(text)?,
        DtoSourceLanguage::Go => parse_go_types(text)?,
        DtoSourceLanguage::Java => parse_java_types(text)?,
        DtoSourceLanguage::Kotlin => parse_kotlin_types(text)?,
        DtoSourceLanguage::Swift => parse_swift_types(text)?,
    };

    let root = if types.contains_key("Record") {
        "Record".to_string()
    } else {
        order
            .first()
            .cloned()
            .ok_or_else(|| "no dto types found".to_string())?
    };

    Ok(DtoSchema { root, types })
}

fn normalize_typescript_text(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars().peekable();
    let mut in_line_comment = false;
    let mut in_block_comment = false;

    while let Some(ch) = chars.next() {
        if in_line_comment {
            out.push(ch);
            if ch == '\n' {
                in_line_comment = false;
            }
            continue;
        }

        if in_block_comment {
            out.push(ch);
            if ch == '*' && matches!(chars.peek(), Some('/')) {
                out.push('/');
                chars.next();
                in_block_comment = false;
            }
            continue;
        }

        if ch == '/' {
            if let Some(next) = chars.peek() {
                if *next == '/' {
                    out.push(ch);
                    out.push(*next);
                    chars.next();
                    in_line_comment = true;
                    continue;
                }
                if *next == '*' {
                    out.push(ch);
                    out.push(*next);
                    chars.next();
                    in_block_comment = true;
                    continue;
                }
            }
        }

        match ch {
            '{' => {
                out.push(ch);
                out.push('\n');
            }
            '}' => {
                out.push('\n');
                out.push(ch);
            }
            ';' => {
                out.push(ch);
                out.push('\n');
            }
            _ => out.push(ch),
        }
    }

    out
}

fn normalize_rust_text(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars().peekable();
    let mut in_line_comment = false;
    let mut in_block_comment = false;
    let mut in_string: Option<char> = None;
    let mut escape = false;
    let mut angle_depth = 0usize;
    let mut paren_depth = 0usize;
    let mut bracket_depth = 0usize;
    let mut last_newline = false;

    while let Some(ch) = chars.next() {
        if in_line_comment {
            out.push(ch);
            last_newline = ch == '\n';
            if last_newline {
                in_line_comment = false;
            }
            continue;
        }

        if in_block_comment {
            out.push(ch);
            last_newline = ch == '\n';
            if ch == '*' && matches!(chars.peek(), Some('/')) {
                out.push('/');
                chars.next();
                in_block_comment = false;
                last_newline = false;
            }
            continue;
        }

        if let Some(quote) = in_string {
            out.push(ch);
            last_newline = ch == '\n';
            if escape {
                escape = false;
                continue;
            }
            if ch == '\\' {
                escape = true;
                continue;
            }
            if ch == quote {
                in_string = None;
            }
            continue;
        }

        if ch == '/' {
            if let Some(next) = chars.peek() {
                if *next == '/' {
                    out.push(ch);
                    out.push(*next);
                    chars.next();
                    in_line_comment = true;
                    last_newline = false;
                    continue;
                }
                if *next == '*' {
                    out.push(ch);
                    out.push(*next);
                    chars.next();
                    in_block_comment = true;
                    last_newline = false;
                    continue;
                }
            }
        }

        if ch == '"' || ch == '\'' {
            out.push(ch);
            in_string = Some(ch);
            last_newline = false;
            continue;
        }

        match ch {
            '<' => {
                angle_depth += 1;
                out.push(ch);
                last_newline = false;
            }
            '>' => {
                if angle_depth > 0 {
                    angle_depth -= 1;
                }
                out.push(ch);
                last_newline = false;
            }
            '(' => {
                paren_depth += 1;
                out.push(ch);
                last_newline = false;
            }
            ')' => {
                if paren_depth > 0 {
                    paren_depth -= 1;
                }
                out.push(ch);
                last_newline = false;
            }
            '[' => {
                bracket_depth += 1;
                out.push(ch);
                last_newline = false;
            }
            ']' => {
                if bracket_depth > 0 {
                    bracket_depth -= 1;
                }
                out.push(ch);
                last_newline = false;
            }
            '{' => {
                out.push(ch);
                out.push('\n');
                last_newline = true;
            }
            '}' => {
                if !last_newline {
                    out.push('\n');
                }
                out.push(ch);
                out.push('\n');
                last_newline = true;
            }
            ',' | ';' => {
                out.push(ch);
                if angle_depth == 0 && paren_depth == 0 && bracket_depth == 0 {
                    out.push('\n');
                    last_newline = true;
                } else {
                    last_newline = false;
                }
            }
            _ => {
                out.push(ch);
                last_newline = false;
            }
        }
    }

    out
}

fn normalize_python_text(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut in_string: Option<char> = None;
    let mut escape = false;

    for ch in text.chars() {
        if let Some(quote) = in_string {
            out.push(ch);
            if escape {
                escape = false;
                continue;
            }
            if ch == '\\' {
                escape = true;
                continue;
            }
            if ch == quote {
                in_string = None;
            }
            continue;
        }

        if ch == '"' || ch == '\'' {
            in_string = Some(ch);
            out.push(ch);
            continue;
        }

        if ch == ';' {
            out.push(ch);
            out.push('\n');
            continue;
        }

        out.push(ch);
    }

    out
}

fn normalize_braced_text(text: &str, split_commas_in_parens: bool) -> String {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars().peekable();
    let mut in_line_comment = false;
    let mut in_block_comment = false;
    let mut in_string: Option<char> = None;
    let mut escape = false;
    let mut angle_depth = 0usize;
    let mut paren_depth = 0usize;
    let mut bracket_depth = 0usize;

    while let Some(ch) = chars.next() {
        if in_line_comment {
            out.push(ch);
            if ch == '\n' {
                in_line_comment = false;
            }
            continue;
        }

        if in_block_comment {
            out.push(ch);
            if ch == '*' && matches!(chars.peek(), Some('/')) {
                out.push('/');
                chars.next();
                in_block_comment = false;
            }
            continue;
        }

        if let Some(quote) = in_string {
            out.push(ch);
            if escape {
                escape = false;
                continue;
            }
            if ch == '\\' {
                escape = true;
                continue;
            }
            if ch == quote {
                in_string = None;
            }
            continue;
        }

        if ch == '/' {
            if let Some(next) = chars.peek() {
                if *next == '/' {
                    out.push(ch);
                    out.push(*next);
                    chars.next();
                    in_line_comment = true;
                    continue;
                }
                if *next == '*' {
                    out.push(ch);
                    out.push(*next);
                    chars.next();
                    in_block_comment = true;
                    continue;
                }
            }
        }

        if ch == '"' || ch == '\'' {
            in_string = Some(ch);
            out.push(ch);
            continue;
        }

        match ch {
            '<' => {
                angle_depth += 1;
                out.push(ch);
            }
            '>' => {
                if angle_depth > 0 {
                    angle_depth -= 1;
                }
                out.push(ch);
            }
            '(' => {
                paren_depth += 1;
                out.push(ch);
            }
            ')' => {
                if paren_depth > 0 {
                    paren_depth -= 1;
                }
                out.push(ch);
            }
            '[' => {
                bracket_depth += 1;
                out.push(ch);
            }
            ']' => {
                if bracket_depth > 0 {
                    bracket_depth -= 1;
                }
                out.push(ch);
            }
            '{' => {
                out.push(ch);
                out.push('\n');
            }
            '}' => {
                out.push('\n');
                out.push(ch);
                out.push('\n');
            }
            ';' => {
                out.push(ch);
                out.push('\n');
            }
            ',' => {
                out.push(ch);
                if split_commas_in_parens
                    && paren_depth > 0
                    && angle_depth == 0
                    && bracket_depth == 0
                {
                    out.push('\n');
                }
            }
            _ => out.push(ch),
        }
    }

    out
}

fn normalize_java_text(text: &str) -> String {
    normalize_braced_text(text, true)
}

fn normalize_kotlin_text(text: &str) -> String {
    normalize_braced_text(text, true)
}

fn normalize_swift_text(text: &str) -> String {
    normalize_braced_text(text, false)
}

fn parse_typescript_types(text: &str) -> Result<(HashMap<String, DtoType>, Vec<String>), String> {
    let mut types: HashMap<String, DtoType> = HashMap::new();
    let mut order = Vec::new();
    let mut current: Option<String> = None;
    let mut pending_json_key: Option<String> = None;

    let normalized = normalize_typescript_text(text);
    for raw_line in normalized.lines() {
        let mut line = raw_line.trim();
        if line.is_empty() {
            continue;
        }

        if line.starts_with("export interface ") || line.starts_with("interface ") {
            let line = line.strip_prefix("export ").unwrap_or(line);
            let name_part = line
                .strip_prefix("interface ")
                .unwrap_or(line)
                .trim();
            let name = name_part
                .split(|ch: char| ch.is_whitespace() || ch == '{')
                .next()
                .unwrap_or("")
                .trim();
            if name.is_empty() {
                continue;
            }
            current = Some(name.to_string());
            pending_json_key = None;
            types
                .entry(name.to_string())
                .or_insert_with(|| DtoType { fields: Vec::new() });
            order.push(name.to_string());
            continue;
        }

        let Some(current_name) = current.clone() else { continue };
        if line.starts_with('}') {
            current = None;
            pending_json_key = None;
            continue;
        }

        if let Some((json_key, rest)) = parse_json_comment(line) {
            pending_json_key = Some(json_key);
            line = rest.trim();
            if line.is_empty() {
                continue;
            }
        }

        if !line.contains(':') {
            continue;
        }

        let line = line.trim_end_matches(';').trim();
        let mut parts = line.splitn(2, ':');
        let name_part = parts.next().unwrap_or("").trim();
        let type_part = parts.next().unwrap_or("").trim();
        if name_part.is_empty() || type_part.is_empty() {
            continue;
        }
        let optional = name_part.ends_with('?');
        let field_name = name_part.trim_end_matches('?').trim().to_string();

        let type_token = type_part
            .split(|ch| ch == '|' || ch == '&')
            .next()
            .unwrap_or("")
            .trim()
            .trim_end_matches(';');
        let field_type = if type_token.contains('[') {
            DtoFieldType::Unknown
        } else {
            match type_token {
                "string" => DtoFieldType::Primitive(PrimitiveKind::String),
                "number" => DtoFieldType::Primitive(PrimitiveKind::Float),
                "boolean" => DtoFieldType::Primitive(PrimitiveKind::Bool),
                "unknown" | "any" => DtoFieldType::Unknown,
                "" => DtoFieldType::Unknown,
                other => DtoFieldType::Object(other.to_string()),
            }
        };

        let json_key = pending_json_key.take().unwrap_or_else(|| field_name.clone());
        if let Some(dto_type) = types.get_mut(&current_name) {
            dto_type.fields.push(DtoField {
                json_key,
                field_type,
                optional,
            });
        }
    }

    Ok((types, order))
}

fn parse_rust_types(text: &str) -> Result<(HashMap<String, DtoType>, Vec<String>), String> {
    let mut types: HashMap<String, DtoType> = HashMap::new();
    let mut order = Vec::new();
    let mut current: Option<String> = None;
    let mut pending_json_key: Option<String> = None;

    let normalized = normalize_rust_text(text);
    for raw_line in normalized.lines() {
        let mut line = raw_line.trim();
        if line.is_empty() {
            continue;
        }

        if line.starts_with("pub struct ") {
            let name_part = line.strip_prefix("pub struct ").unwrap_or(line).trim();
            let name = name_part
                .split(|ch: char| ch.is_whitespace() || ch == '{')
                .next()
                .unwrap_or("")
                .trim();
            if name.is_empty() {
                continue;
            }
            current = Some(name.to_string());
            pending_json_key = None;
            types
                .entry(name.to_string())
                .or_insert_with(|| DtoType { fields: Vec::new() });
            order.push(name.to_string());
            continue;
        }

        let Some(current_name) = current.clone() else { continue };
        if line.starts_with('}') {
            current = None;
            pending_json_key = None;
            continue;
        }

        if line.starts_with("#[serde") {
            if let Some(rename) = parse_serde_rename(line) {
                pending_json_key = Some(rename);
            }
            if let Some(end) = line.find(']') {
                let rest = line[end + 1..].trim();
                if rest.is_empty() {
                    continue;
                }
                line = rest;
            } else {
                continue;
            }
        }

        if !line.starts_with("pub ") {
            continue;
        }

        let line = line.trim_end_matches(',');
        let rest = line.strip_prefix("pub ").unwrap_or(line).trim();
        let mut parts = rest.splitn(2, ':');
        let field_name = parts.next().unwrap_or("").trim();
        let type_part = parts.next().unwrap_or("").trim();
        if field_name.is_empty() || type_part.is_empty() {
            continue;
        }

        let compact = type_part.replace(' ', "");
        let (type_name, optional) = if compact.starts_with("Option<") && compact.ends_with('>') {
            (
                compact[7..compact.len() - 1].to_string(),
                true,
            )
        } else {
            (compact, false)
        };

        let type_key = type_name
            .rsplit("::")
            .next()
            .unwrap_or(&type_name)
            .to_string();
        let field_type = match type_key.as_str() {
            "String" => DtoFieldType::Primitive(PrimitiveKind::String),
            "bool" => DtoFieldType::Primitive(PrimitiveKind::Bool),
            "i8" | "i16" | "i32" | "i64" | "isize" | "u8" | "u16" | "u32" | "u64" | "usize" => {
                DtoFieldType::Primitive(PrimitiveKind::Int)
            }
            "f32" | "f64" => DtoFieldType::Primitive(PrimitiveKind::Float),
            _ if type_key.ends_with("Value") => DtoFieldType::Unknown,
            _ => DtoFieldType::Object(type_key),
        };

        let json_key = pending_json_key.take().unwrap_or_else(|| field_name.to_string());
        if let Some(dto_type) = types.get_mut(&current_name) {
            dto_type.fields.push(DtoField {
                json_key,
                field_type,
                optional,
            });
        }
    }

    Ok((types, order))
}

fn parse_first_quoted_value(text: &str) -> Option<String> {
    let mut best: Option<(usize, char)> = None;
    for quote in ['"', '\''] {
        if let Some(pos) = text.find(quote) {
            if best.map_or(true, |(best_pos, _)| pos < best_pos) {
                best = Some((pos, quote));
            }
        }
    }

    let (pos, quote) = best?;
    let after = &text[pos + 1..];
    let end = after.find(quote)?;
    Some(after[..end].to_string())
}

fn parse_quoted_value_after(line: &str, marker: &str) -> Option<String> {
    let start = line.find(marker)?;
    let after = &line[start + marker.len()..];
    parse_first_quoted_value(after)
}

fn parse_named_argument(line: &str, key: &str) -> Option<String> {
    let start = line.find(key)?;
    let after = &line[start + key.len()..];
    let eq_pos = after.find('=')?;
    let after_eq = after[eq_pos + 1..].trim_start();
    parse_first_quoted_value(after_eq)
}

fn parse_common_rename_annotation(line: &str) -> Option<String> {
    parse_quoted_value_after(line, "@JsonProperty")
        .or_else(|| parse_quoted_value_after(line, "@SerializedName"))
        .or_else(|| parse_quoted_value_after(line, "@SerialName"))
        .or_else(|| parse_quoted_value_after(line, "@Json"))
}

fn strip_leading_annotations(
    line: &str,
    pending_json_key: &mut Option<String>,
    pending_optional: &mut bool,
) -> String {
    let mut rest = line.trim();
    loop {
        if !rest.starts_with('@') {
            break;
        }
        if let Some(rename) = parse_common_rename_annotation(rest) {
            *pending_json_key = Some(rename);
        }
        if rest.starts_with("@Nullable") {
            *pending_optional = true;
        }
        if let Some(end) = rest.find(')') {
            rest = rest[end + 1..].trim();
            if rest.is_empty() {
                return String::new();
            }
        } else if let Some(space) = rest.find(' ') {
            rest = rest[space + 1..].trim();
            if rest.is_empty() {
                return String::new();
            }
        } else {
            return String::new();
        }
    }

    rest.to_string()
}

fn parse_python_alias(line: &str) -> Option<String> {
    parse_named_argument(line, "alias")
}

fn parse_python_types(text: &str) -> Result<(HashMap<String, DtoType>, Vec<String>), String> {
    let mut types: HashMap<String, DtoType> = HashMap::new();
    let mut order = Vec::new();
    let mut current: Option<String> = None;
    let mut current_indent: Option<usize> = None;
    let normalized = normalize_python_text(text);

    for raw_line in normalized.lines() {
        let indent = raw_line.chars().take_while(|ch| ch.is_whitespace()).count();
        let mut line = raw_line.trim();
        let mut class_line = false;
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        if line.starts_with("class ") {
            class_line = true;
            let name_part = line.strip_prefix("class ").unwrap_or(line).trim();
            let name = name_part
                .split(|ch: char| ch.is_whitespace() || ch == '(' || ch == ':')
                .next()
                .unwrap_or("")
                .trim();
            if name.is_empty() {
                continue;
            }
            current = Some(name.to_string());
            current_indent = Some(indent);
            types
                .entry(name.to_string())
                .or_insert_with(|| DtoType { fields: Vec::new() });
            order.push(name.to_string());
            if let Some(colon_pos) = line.find(':') {
                line = line[colon_pos + 1..].trim();
                if line.is_empty() {
                    continue;
                }
            } else {
                continue;
            }
        }

        if let Some(indent_level) = current_indent {
            if !class_line && indent <= indent_level && !line.is_empty() {
                current = None;
                current_indent = None;
            }
        }

        let Some(current_name) = current.clone() else { continue };

        if line.starts_with('@') {
            continue;
        }

        if let Some(comment_pos) = line.find('#') {
            line = line[..comment_pos].trim();
        }
        if line.is_empty() || !line.contains(':') {
            continue;
        }

        let mut parts = line.splitn(2, ':');
        let field_name = parts.next().unwrap_or("").trim();
        let mut rest = parts.next().unwrap_or("").trim();
        rest = rest.trim_end_matches(';').trim();
        if field_name.is_empty() || rest.is_empty() {
            continue;
        }

        let mut optional = false;
        if let Some(eq_pos) = rest.find('=') {
            let (type_part, value_part) = rest.split_at(eq_pos);
            rest = type_part.trim();
            if value_part.contains("None") {
                optional = true;
            }
        }

        if rest.contains("Optional[")
            || rest.contains("None")
            || rest.contains("| None")
            || rest.contains("None |")
        {
            optional = true;
        }

        let mut type_token = rest.trim();
        if let Some(start) = type_token.find("Optional[") {
            let after = &type_token[start + "Optional[".len()..];
            if let Some(end) = after.find(']') {
                type_token = after[..end].trim();
            }
        } else if let Some(start) = type_token.find("Union[") {
            let after = &type_token[start + "Union[".len()..];
            if let Some(end) = after.find(']') {
                let inner = &after[..end];
                if let Some(first) = inner
                    .split(',')
                    .map(|item| item.trim())
                    .find(|item| !item.contains("None"))
                {
                    type_token = first;
                }
            }
        } else if type_token.contains('|') {
            if let Some(first) = type_token
                .split('|')
                .map(|item| item.trim())
                .find(|item| !item.contains("None"))
            {
                type_token = first;
            }
        }

        let type_token = type_token.trim_start_matches("typing.");
        let field_type = if type_token.contains('[')
            || type_token.contains("List")
            || type_token.contains("Dict")
            || type_token.contains("list")
            || type_token.contains("dict")
        {
            DtoFieldType::Unknown
        } else {
            match type_token {
                "str" | "string" => DtoFieldType::Primitive(PrimitiveKind::String),
                "int" => DtoFieldType::Primitive(PrimitiveKind::Int),
                "float" => DtoFieldType::Primitive(PrimitiveKind::Float),
                "bool" | "boolean" => DtoFieldType::Primitive(PrimitiveKind::Bool),
                "Any" | "any" => DtoFieldType::Unknown,
                "" => DtoFieldType::Unknown,
                other => DtoFieldType::Object(other.to_string()),
            }
        };

        let json_key = parse_python_alias(line).unwrap_or_else(|| field_name.to_string());
        if let Some(dto_type) = types.get_mut(&current_name) {
            dto_type.fields.push(DtoField {
                json_key,
                field_type,
                optional,
            });
        }
    }

    Ok((types, order))
}

fn parse_go_types(text: &str) -> Result<(HashMap<String, DtoType>, Vec<String>), String> {
    let mut types: HashMap<String, DtoType> = HashMap::new();
    let mut order = Vec::new();
    let mut index = 0usize;
    let bytes = text.as_bytes();

    while index < bytes.len() {
        let slice = &text[index..];
        let Some(pos) = slice.find("type ") else { break };
        index += pos + 5;
        let rest = &text[index..];
        let name = rest
            .trim_start()
            .split_whitespace()
            .next()
            .unwrap_or("");
        if name.is_empty() {
            index = index.saturating_add(1);
            continue;
        }
        let name_start = rest.find(name).unwrap_or(0);
        index += name_start + name.len();
        let after_name = &text[index..];
        let Some(struct_pos) = after_name.find("struct") else {
            index = index.saturating_add(1);
            continue;
        };
        index += struct_pos + "struct".len();
        let after_struct = &text[index..];
        let Some(brace_pos) = after_struct.find('{') else {
            index = index.saturating_add(1);
            continue;
        };
        index += brace_pos + 1;

        let mut brace_depth = 1usize;
        let mut body_end = index;
        while body_end < bytes.len() {
            match bytes[body_end] as char {
                '{' => brace_depth += 1,
                '}' => {
                    brace_depth -= 1;
                    if brace_depth == 0 {
                        break;
                    }
                }
                _ => {}
            }
            body_end += 1;
        }
        if brace_depth != 0 {
            break;
        }

        let body = &text[index..body_end];
        let dto_type = types
            .entry(name.to_string())
            .or_insert_with(|| DtoType { fields: Vec::new() });
        parse_go_struct_fields(body, dto_type);
        order.push(name.to_string());
        index = body_end + 1;
    }

    Ok((types, order))
}

fn parse_go_struct_fields(body: &str, dto_type: &mut DtoType) {
    let mut chars = body.chars().peekable();
    while let Some(ch) = chars.peek().copied() {
        if ch.is_whitespace() {
            chars.next();
            continue;
        }
        if ch == '/' {
            chars.next();
            if matches!(chars.peek(), Some('/')) {
                while let Some(next) = chars.next() {
                    if next == '\n' {
                        break;
                    }
                }
                continue;
            }
            if matches!(chars.peek(), Some('*')) {
                chars.next();
                while let Some(next) = chars.next() {
                    if next == '*' && matches!(chars.peek(), Some('/')) {
                        chars.next();
                        break;
                    }
                }
                continue;
            }
            continue;
        }

        let field_name = read_go_token(&mut chars);
        if field_name.is_empty() {
            chars.next();
            continue;
        }
        skip_go_whitespace(&mut chars);
        let field_type = read_go_token(&mut chars);
        if field_type.is_empty() {
            continue;
        }

        skip_go_whitespace(&mut chars);
        let tag = if matches!(chars.peek(), Some('`')) {
            chars.next();
            let mut tag_value = String::new();
            while let Some(next) = chars.next() {
                if next == '`' {
                    break;
                }
                tag_value.push(next);
            }
            Some(tag_value)
        } else {
            None
        };

        let (json_key, tag_optional, skip_field) = parse_go_json_tag(tag.as_deref());
        if skip_field {
            continue;
        }

        let mut optional = tag_optional;
        let mut type_token = field_type.trim().to_string();
        if let Some(stripped) = type_token.strip_prefix('*') {
            optional = true;
            type_token = stripped.to_string();
        }

        let field_type = if type_token.contains('[') || type_token.contains("map[") {
            DtoFieldType::Unknown
        } else {
            match type_token.as_str() {
                "string" => DtoFieldType::Primitive(PrimitiveKind::String),
                "bool" => DtoFieldType::Primitive(PrimitiveKind::Bool),
                "int" | "int8" | "int16" | "int32" | "int64" | "uint" | "uint8" | "uint16"
                | "uint32" | "uint64" | "uintptr" => DtoFieldType::Primitive(PrimitiveKind::Int),
                "float32" | "float64" => DtoFieldType::Primitive(PrimitiveKind::Float),
                "" => DtoFieldType::Unknown,
                other => DtoFieldType::Object(other.to_string()),
            }
        };

        let json_key = json_key.unwrap_or_else(|| field_name.clone());
        dto_type.fields.push(DtoField {
            json_key,
            field_type,
            optional,
        });
    }
}

fn read_go_token(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) -> String {
    let mut token = String::new();
    while let Some(&ch) = chars.peek() {
        if ch.is_whitespace() || ch == '`' || ch == '{' || ch == '}' {
            break;
        }
        token.push(ch);
        chars.next();
    }
    token
}

fn skip_go_whitespace(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) {
    while let Some(&ch) = chars.peek() {
        if !ch.is_whitespace() {
            break;
        }
        chars.next();
    }
}

fn parse_go_json_tag(tag: Option<&str>) -> (Option<String>, bool, bool) {
    let Some(tag) = tag else {
        return (None, false, false);
    };
    let Some(start) = tag.find("json:\"") else {
        return (None, false, false);
    };
    let after = &tag[start + 6..];
    let Some(end) = after.find('"') else {
        return (None, false, false);
    };
    let content = &after[..end];
    if content == "-" {
        return (None, false, true);
    }
    let mut parts = content.split(',');
    let name_part = parts.next().unwrap_or("");
    let omitempty = parts.any(|part| part.trim() == "omitempty");
    let name = if name_part.is_empty() {
        None
    } else {
        Some(name_part.to_string())
    };
    (name, omitempty, false)
}

fn parse_java_types(text: &str) -> Result<(HashMap<String, DtoType>, Vec<String>), String> {
    let mut types: HashMap<String, DtoType> = HashMap::new();
    let mut order = Vec::new();
    let mut current: Option<String> = None;
    let mut pending_json_key: Option<String> = None;
    let mut pending_optional = false;
    let mut record_param_depth = 0i32;
    let normalized = normalize_java_text(text);

    for raw_line in normalized.lines() {
        let mut line = raw_line.trim();
        if line.is_empty() {
            continue;
        }

        if line.contains(" class ") || line.starts_with("class ") {
            let name_part = if let Some(idx) = line.find("class ") {
                &line[idx + 6..]
            } else {
                line
            };
            let name = name_part
                .split(|ch: char| ch.is_whitespace() || ch == '{' || ch == '(')
                .next()
                .unwrap_or("")
                .trim();
            if !name.is_empty() {
                current = Some(name.to_string());
                types
                    .entry(name.to_string())
                    .or_insert_with(|| DtoType { fields: Vec::new() });
                order.push(name.to_string());
            }
            record_param_depth = 0;
            pending_json_key = None;
            pending_optional = false;
            continue;
        }

        if line.contains(" record ") || line.starts_with("record ") {
            let name_part = if let Some(idx) = line.find("record ") {
                &line[idx + 7..]
            } else {
                line
            };
            let name = name_part
                .split(|ch: char| ch.is_whitespace() || ch == '{' || ch == '(')
                .next()
                .unwrap_or("")
                .trim();
            if !name.is_empty() {
                current = Some(name.to_string());
                types
                    .entry(name.to_string())
                    .or_insert_with(|| DtoType { fields: Vec::new() });
                order.push(name.to_string());
                if let Some(paren_pos) = line.find('(') {
                    record_param_depth = 1;
                    line = line[paren_pos + 1..].trim();
                } else {
                    record_param_depth = 0;
                    continue;
                }
                pending_json_key = None;
                pending_optional = false;
            }
        }

        let Some(current_name) = current.clone() else { continue };
        if line.starts_with('}') {
            current = None;
            record_param_depth = 0;
            pending_json_key = None;
            pending_optional = false;
            continue;
        }

        if record_param_depth > 0 {
            let open_parens = line.matches('(').count() as i32;
            let close_parens = line.matches(')').count() as i32;
            let next_depth = record_param_depth + open_parens - close_parens;
            if next_depth <= 0 {
                if let Some(end) = line.rfind(')') {
                    line = line[..end].trim();
                }
                record_param_depth = 0;
            } else {
                record_param_depth = next_depth;
            }
            if line.is_empty() {
                continue;
            }
            let stripped =
                strip_leading_annotations(line, &mut pending_json_key, &mut pending_optional);
            line = stripped.trim();
            if line.is_empty() {
                continue;
            }
            parse_java_field_line(
                line,
                &current_name,
                &mut types,
                &mut pending_json_key,
                &mut pending_optional,
            );
            continue;
        }

        let stripped = strip_leading_annotations(line, &mut pending_json_key, &mut pending_optional);
        line = stripped.trim();
        if line.is_empty() || !line.contains(';') {
            continue;
        }
        parse_java_field_line(
            line,
            &current_name,
            &mut types,
            &mut pending_json_key,
            &mut pending_optional,
        );
    }

    Ok((types, order))
}

fn parse_java_field_line(
    line: &str,
    current_name: &str,
    types: &mut HashMap<String, DtoType>,
    pending_json_key: &mut Option<String>,
    pending_optional: &mut bool,
) {
    let mut cleaned = line;
    if let Some(comment_pos) = cleaned.find("//") {
        cleaned = cleaned[..comment_pos].trim();
    }
    cleaned = cleaned.split('=').next().unwrap_or(cleaned).trim();
    cleaned = cleaned.trim_end_matches(';').trim();
    cleaned = cleaned.trim_end_matches(',').trim();
    if cleaned.is_empty() {
        return;
    }

    let modifiers = [
        "public", "private", "protected", "static", "final", "transient", "volatile",
    ];
    let mut rest = cleaned;
    loop {
        let mut stripped = None;
        for modifier in modifiers {
            if rest.starts_with(modifier) {
                let after = rest[modifier.len()..].trim_start();
                if after.len() != rest.len() {
                    stripped = Some(after);
                    break;
                }
            }
        }
        if let Some(value) = stripped {
            rest = value;
            continue;
        }
        break;
    }

    let Some(split_pos) = rest.rfind(|ch: char| ch.is_whitespace()) else { return };
    let type_part = rest[..split_pos].trim();
    let field_name = rest[split_pos..].trim();
    if field_name.is_empty() || type_part.is_empty() {
        return;
    }

    let optional = *pending_optional || type_part.replace(' ', "").contains("Optional<");
    *pending_optional = false;

    let type_key = type_part
        .rsplit('.')
        .next()
        .unwrap_or(type_part)
        .trim()
        .trim_end_matches('>');
    let type_key = type_key
        .rsplit('<')
        .next()
        .unwrap_or(type_key)
        .trim();
    let field_type = match type_key {
        "String" => DtoFieldType::Primitive(PrimitiveKind::String),
        "boolean" | "Boolean" => DtoFieldType::Primitive(PrimitiveKind::Bool),
        "byte" | "short" | "int" | "long" | "Byte" | "Short" | "Integer" | "Long" => {
            DtoFieldType::Primitive(PrimitiveKind::Int)
        }
        "float" | "double" | "Float" | "Double" => DtoFieldType::Primitive(PrimitiveKind::Float),
        "" => DtoFieldType::Unknown,
        other => DtoFieldType::Object(other.to_string()),
    };

    let json_key = pending_json_key.take().unwrap_or_else(|| field_name.to_string());
    if let Some(dto_type) = types.get_mut(current_name) {
        dto_type.fields.push(DtoField {
            json_key,
            field_type,
            optional,
        });
    }
}

fn parse_kotlin_types(text: &str) -> Result<(HashMap<String, DtoType>, Vec<String>), String> {
    let mut types: HashMap<String, DtoType> = HashMap::new();
    let mut order = Vec::new();
    let mut current: Option<String> = None;
    let mut pending_json_key: Option<String> = None;
    let mut pending_optional = false;
    let mut param_depth = 0i32;
    let normalized = normalize_kotlin_text(text);

    for raw_line in normalized.lines() {
        let mut line = raw_line.trim();
        if line.is_empty() {
            continue;
        }

        if line.contains(" class ") || line.starts_with("class ") || line.starts_with("data class ")
        {
            let name_part = if let Some(idx) = line.find("class ") {
                &line[idx + 6..]
            } else {
                line
            };
            let name = name_part
                .split(|ch: char| ch.is_whitespace() || ch == '(' || ch == '{')
                .next()
                .unwrap_or("")
                .trim();
            if !name.is_empty() {
                current = Some(name.to_string());
                types
                    .entry(name.to_string())
                    .or_insert_with(|| DtoType { fields: Vec::new() });
                order.push(name.to_string());
                pending_json_key = None;
                pending_optional = false;
                param_depth = 0;
                if let Some(paren_pos) = line.find('(') {
                    param_depth += 1;
                    line = line[paren_pos + 1..].trim();
                } else {
                    continue;
                }
            }
        }

        let Some(current_name) = current.clone() else { continue };
        if line.starts_with('}') {
            current = None;
            param_depth = 0;
            pending_json_key = None;
            pending_optional = false;
            continue;
        }

        if param_depth <= 0 {
            continue;
        }

        let open_parens = line.matches('(').count() as i32;
        let close_parens = line.matches(')').count() as i32;
        let next_depth = param_depth + open_parens - close_parens;
        let mut slice = line;
        if next_depth <= 0 {
            if let Some(end) = slice.rfind(')') {
                slice = slice[..end].trim();
            }
        }

        if param_depth <= 0 && slice.is_empty() {
            param_depth = next_depth.max(0);
            continue;
        }

        let stripped =
            strip_leading_annotations(slice, &mut pending_json_key, &mut pending_optional);
        line = stripped.trim();
        if line.is_empty() {
            param_depth = next_depth.max(0);
            continue;
        }

        let line = line.trim_end_matches(',').trim();
        let rest = if let Some(stripped) = line.strip_prefix("val ") {
            stripped
        } else if let Some(stripped) = line.strip_prefix("var ") {
            stripped
        } else {
            line
        };

        let mut parts = rest.splitn(2, ':');
        let field_name = parts.next().unwrap_or("").trim();
        let type_part = parts.next().unwrap_or("").trim();
        if field_name.is_empty() || type_part.is_empty() {
            continue;
        }

        let mut optional = pending_optional;
        pending_optional = false;
        if type_part.contains('?') || type_part.contains("= null") {
            optional = true;
        }

        let type_token = type_part
            .split('=')
            .next()
            .unwrap_or(type_part)
            .trim()
            .trim_end_matches('?');
        let field_type = if type_token.contains('<') {
            DtoFieldType::Unknown
        } else {
            match type_token {
                "String" => DtoFieldType::Primitive(PrimitiveKind::String),
                "Boolean" => DtoFieldType::Primitive(PrimitiveKind::Bool),
                "Int" | "Long" | "Short" | "Byte" => DtoFieldType::Primitive(PrimitiveKind::Int),
                "Float" | "Double" => DtoFieldType::Primitive(PrimitiveKind::Float),
                "" => DtoFieldType::Unknown,
                other => DtoFieldType::Object(other.to_string()),
            }
        };

        let json_key = pending_json_key.take().unwrap_or_else(|| field_name.to_string());
        if let Some(dto_type) = types.get_mut(&current_name) {
            dto_type.fields.push(DtoField {
                json_key,
                field_type,
                optional,
            });
        }

        param_depth = next_depth.max(0);
    }

    Ok((types, order))
}

fn parse_swift_types(text: &str) -> Result<(HashMap<String, DtoType>, Vec<String>), String> {
    let mut types: HashMap<String, DtoType> = HashMap::new();
    let mut order = Vec::new();
    let mut current: Option<String> = None;
    let mut coding_keys: HashMap<String, String> = HashMap::new();
    let mut in_coding_keys = false;
    let mut coding_depth = 0i32;
    let mut type_depth = 0i32;
    let normalized = normalize_swift_text(text);

    for raw_line in normalized.lines() {
        let line = raw_line.trim();
        if line.is_empty() {
            continue;
        }

        if line.contains(" struct ") || line.starts_with("struct ") || line.contains(" class ")
            || line.starts_with("class ")
        {
            let keyword_pos = if let Some(pos) = line.find("struct ") {
                pos + 7
            } else if let Some(pos) = line.find("class ") {
                pos + 6
            } else {
                0
            };
            let name_part = line[keyword_pos..]
                .split_whitespace()
                .next()
                .unwrap_or("");
            let name = name_part
                .split(|ch: char| ch == ':' || ch == '{')
                .next()
                .unwrap_or("")
                .trim();
            if !name.is_empty() {
                current = Some(name.to_string());
                types
                    .entry(name.to_string())
                    .or_insert_with(|| DtoType { fields: Vec::new() });
                order.push(name.to_string());
                coding_keys.clear();
                in_coding_keys = false;
                coding_depth = 0;
                type_depth = 0;
            }
        }

        let open_braces = line.matches('{').count() as i32;
        let close_braces = line.matches('}').count() as i32;
        if current.is_some() {
            type_depth += open_braces - close_braces;
            if type_depth < 0 {
                type_depth = 0;
            }
        }

        let Some(current_name) = current.clone() else { continue };

        if line.starts_with("enum CodingKeys") {
            in_coding_keys = true;
            coding_depth = open_braces - close_braces;
            continue;
        }

        if in_coding_keys {
            coding_depth += open_braces - close_braces;
            if line.starts_with("case ") {
                let cases = parse_swift_cases(line);
                if let Some(dto_type) = types.get_mut(&current_name) {
                    for (field, rename) in cases {
                        coding_keys.insert(field.clone(), rename.clone());
                        for existing in &mut dto_type.fields {
                            if existing.json_key == field {
                                existing.json_key = rename.clone();
                            }
                        }
                    }
                }
            }
            if coding_depth <= 0 {
                in_coding_keys = false;
                coding_depth = 0;
            }
            continue;
        }

        if type_depth == 0 && line.starts_with('}') {
            current = None;
            continue;
        }

        if !(line.starts_with("let ") || line.starts_with("var ")) {
            continue;
        }

        let rest = line
            .trim_end_matches(';')
            .trim_end_matches(',')
            .trim();
        let rest = rest.strip_prefix("let ").or_else(|| rest.strip_prefix("var "));
        let Some(rest) = rest else { continue };
        let mut parts = rest.splitn(2, ':');
        let field_name = parts.next().unwrap_or("").trim();
        let mut type_part = parts.next().unwrap_or("").trim();
        if field_name.is_empty() || type_part.is_empty() {
            continue;
        }
        if let Some(eq_pos) = type_part.find('=') {
            type_part = type_part[..eq_pos].trim();
        }

        let mut optional = type_part.contains('?');
        let type_token = type_part.trim_end_matches('?');
        let field_type = if type_token.contains('<') {
            DtoFieldType::Unknown
        } else {
            match type_token {
                "String" => DtoFieldType::Primitive(PrimitiveKind::String),
                "Bool" => DtoFieldType::Primitive(PrimitiveKind::Bool),
                "Int" | "Int8" | "Int16" | "Int32" | "Int64" | "UInt" | "UInt8" | "UInt16"
                | "UInt32" | "UInt64" => DtoFieldType::Primitive(PrimitiveKind::Int),
                "Float" | "Double" => DtoFieldType::Primitive(PrimitiveKind::Float),
                "" => DtoFieldType::Unknown,
                other => DtoFieldType::Object(other.to_string()),
            }
        };

        if type_part.contains("Optional<") {
            optional = true;
        }

        let json_key = coding_keys
            .get(field_name)
            .cloned()
            .unwrap_or_else(|| field_name.to_string());
        if let Some(dto_type) = types.get_mut(&current_name) {
            dto_type.fields.push(DtoField {
                json_key,
                field_type,
                optional,
            });
        }
    }

    Ok((types, order))
}

fn parse_swift_cases(line: &str) -> Vec<(String, String)> {
    let mut cases = Vec::new();
    let rest = line.strip_prefix("case ").unwrap_or(line).trim();
    let mut current = String::new();
    let mut in_string = false;
    let mut escape = false;

    for ch in rest.chars() {
        if in_string {
            current.push(ch);
            if escape {
                escape = false;
                continue;
            }
            if ch == '\\' {
                escape = true;
                continue;
            }
            if ch == '"' {
                in_string = false;
            }
            continue;
        }

        if ch == '"' {
            in_string = true;
            current.push(ch);
            continue;
        }

        if ch == ',' {
            push_swift_case(&mut cases, &current);
            current.clear();
            continue;
        }

        current.push(ch);
    }
    push_swift_case(&mut cases, &current);
    cases
}

fn push_swift_case(cases: &mut Vec<(String, String)>, text: &str) {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return;
    }
    let mut parts = trimmed.splitn(2, '=');
    let name = parts.next().unwrap_or("").trim();
    if name.is_empty() {
        return;
    }
    let rename = parts
        .next()
        .and_then(|value| parse_first_quoted_value(value))
        .unwrap_or_else(|| name.to_string());
    cases.push((name.to_string(), rename));
}

fn parse_json_comment(line: &str) -> Option<(String, &str)> {
    let marker = line.find("json:")?;
    let after_marker = &line[marker + 5..];
    let quote_start = after_marker.find('"')?;
    let after_quote = &after_marker[quote_start + 1..];
    let quote_end = after_quote.find('"')?;
    let json_key = after_quote[..quote_end].to_string();
    let rest = if let Some(end) = line.find("*/") {
        &line[end + 2..]
    } else {
        ""
    };
    Some((json_key, rest))
}

fn parse_serde_rename(line: &str) -> Option<String> {
    let marker = line.find("rename")?;
    let after_marker = &line[marker..];
    let quote_start = after_marker.find('"')?;
    let after_quote = &after_marker[quote_start + 1..];
    let quote_end = after_quote.find('"')?;
    Some(after_quote[..quote_end].to_string())
}

fn generate_mappings_from_schema(schema: &DtoSchema) -> Result<Vec<GeneratedMapping>, String> {
    let mut mappings = Vec::new();
    let mut visiting = HashSet::new();
    build_mappings_for_type(schema, &schema.root, "", false, &mut visiting, &mut mappings)?;
    Ok(mappings)
}

fn build_mappings_for_type(
    schema: &DtoSchema,
    type_name: &str,
    prefix: &str,
    parent_optional: bool,
    visiting: &mut HashSet<String>,
    out: &mut Vec<GeneratedMapping>,
) -> Result<(), String> {
    if !visiting.insert(type_name.to_string()) {
        return Ok(());
    }
    let dto_type = schema
        .types
        .get(type_name)
        .ok_or_else(|| format!("unknown dto type: {}", type_name))?;

    for field in &dto_type.fields {
        let target = append_path(prefix, &field.json_key);
        let optional = parent_optional || field.optional;
        match &field.field_type {
            DtoFieldType::Primitive(kind) => {
                let value_type = primitive_to_value_type(kind);
                out.push(GeneratedMapping {
                    target,
                    value_type,
                    required: !optional,
                });
            }
            DtoFieldType::Unknown => {
                out.push(GeneratedMapping {
                    target,
                    value_type: None,
                    required: !optional,
                });
            }
            DtoFieldType::Object(child) => {
                build_mappings_for_type(schema, child, &target, optional, visiting, out)?;
            }
        }
    }

    visiting.remove(type_name);
    Ok(())
}

fn primitive_to_value_type(kind: &PrimitiveKind) -> Option<String> {
    match kind {
        PrimitiveKind::String => Some("string".to_string()),
        PrimitiveKind::Int => Some("int".to_string()),
        PrimitiveKind::Float => Some("float".to_string()),
        PrimitiveKind::Bool => Some("bool".to_string()),
    }
}

fn build_input_yaml(format: &str, records_path: Option<&str>) -> YamlValue {
    let mut input_map = YamlMapping::new();
    input_map.insert(yaml_key("format"), YamlValue::String(format.to_string()));
    if format.eq_ignore_ascii_case("json") {
        let mut json_map = YamlMapping::new();
        if let Some(records_path) = records_path {
            json_map.insert(
                yaml_key("records_path"),
                YamlValue::String(records_path.to_string()),
            );
        }
        input_map.insert(yaml_key("json"), YamlValue::Mapping(json_map));
    } else {
        input_map.insert(yaml_key("csv"), YamlValue::Mapping(YamlMapping::new()));
    }
    YamlValue::Mapping(input_map)
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

fn update_yaml_input_spec(
    root: &mut YamlValue,
    format: Option<&str>,
    records_path: Option<&str>,
) {
    if format.is_none() && records_path.is_none() {
        return;
    }
    let Some(root_map) = root.as_mapping_mut() else { return; };
    let input_value = root_map
        .entry(yaml_key("input"))
        .or_insert_with(|| YamlValue::Mapping(YamlMapping::new()));
    let Some(input_map) = input_value.as_mapping_mut() else { return; };

    if let Some(format) = format {
        input_map.insert(yaml_key("format"), YamlValue::String(format.to_string()));
    }
    if let Some(records_path) = records_path {
        let json_value = input_map
            .entry(yaml_key("json"))
            .or_insert_with(|| YamlValue::Mapping(YamlMapping::new()));
        if let Some(json_map) = json_value.as_mapping_mut() {
            json_map.insert(
                yaml_key("records_path"),
                YamlValue::String(records_path.to_string()),
            );
        }
    }
}

fn yaml_mappings_sequence_mut(root: &mut YamlValue) -> Result<&mut Vec<YamlValue>, CallError> {
    let Some(root_map) = root.as_mapping_mut() else {
        let message = "rules yaml must be a mapping".to_string();
        return Err(CallError::Tool {
            message: message.clone(),
            errors: Some(vec![parse_error_json(&message, None)]),
        });
    };
    let Some(mappings_value) = root_map.get_mut(&yaml_key("mappings")) else {
        let message = "rules yaml is missing mappings".to_string();
        return Err(CallError::Tool {
            message: message.clone(),
            errors: Some(vec![parse_error_json(&message, None)]),
        });
    };
    mappings_value
        .as_sequence_mut()
        .ok_or_else(|| {
            let message = "rules yaml mappings must be a sequence".to_string();
            CallError::Tool {
                message: message.clone(),
                errors: Some(vec![parse_error_json(&message, None)]),
            }
        })
}

fn update_yaml_mapping(
    mappings: &mut Vec<YamlValue>,
    index: usize,
    source: Option<&str>,
) -> Result<(), CallError> {
    let Some(mapping_value) = mappings.get_mut(index) else {
        let message = "mapping index out of range".to_string();
        return Err(CallError::Tool {
            message: message.clone(),
            errors: Some(vec![parse_error_json(&message, None)]),
        });
    };
    let Some(mapping_map) = mapping_value.as_mapping_mut() else {
        let message = "mapping entry must be a mapping".to_string();
        return Err(CallError::Tool {
            message: message.clone(),
            errors: Some(vec![parse_error_json(&message, None)]),
        });
    };

    if let Some(source) = source {
        mapping_map.insert(yaml_key("source"), YamlValue::String(source.to_string()));
        mapping_map.remove(&yaml_key("value"));
        mapping_map.remove(&yaml_key("expr"));
    } else {
        mapping_map.remove(&yaml_key("source"));
        mapping_map.remove(&yaml_key("expr"));
        mapping_map.insert(yaml_key("value"), YamlValue::Null);
        mapping_map.insert(yaml_key("required"), YamlValue::Bool(false));
    }
    Ok(())
}

fn yaml_key(key: &str) -> YamlValue {
    YamlValue::String(key.to_string())
}

fn collect_missing_refs(
    target: &str,
    expr: Option<&Expr>,
    when: Option<&Expr>,
    input_paths: &HashSet<String>,
    out: &mut Vec<Value>,
    seen: &mut HashSet<String>,
) {
    for expr in [expr, when] {
        let Some(expr) = expr else { continue };
        let mut refs = Vec::new();
        collect_expr_refs(expr, &mut refs);
        for reference in refs {
            let Some(path) = input_ref_path(&reference) else { continue };
            if input_paths.contains(&path) {
                continue;
            }
            let key = format!("{}|{}", target, reference);
            if seen.insert(key) {
                out.push(json!({
                    "target": target,
                    "ref": reference,
                    "path": path
                }));
            }
        }
    }
}

fn collect_expr_refs(expr: &Expr, out: &mut Vec<String>) {
    match expr {
        Expr::Ref(reference) => out.push(reference.ref_path.clone()),
        Expr::Op(op) => {
            for arg in &op.args {
                collect_expr_refs(arg, out);
            }
        }
        Expr::Chain(chain) => {
            for item in &chain.chain {
                collect_expr_refs(item, out);
            }
        }
        Expr::Literal(_) => {}
    }
}

fn input_ref_path(reference: &str) -> Option<String> {
    let trimmed = reference.trim();
    if let Some(rest) = trimmed.strip_prefix("input.") {
        if rest.is_empty() {
            None
        } else {
            Some(rest.to_string())
        }
    } else {
        None
    }
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

struct RuleWarning {
    code: &'static str,
    message: String,
    path: Option<String>,
}

fn collect_rule_warnings(rule: &RuleFile) -> Vec<RuleWarning> {
    let mut warnings = Vec::new();
    for (index, mapping) in rule.mappings.iter().enumerate() {
        let base_path = format!("mappings[{}]", index);
        if let Some(expr) = &mapping.expr {
            collect_expr_warnings(expr, &format!("{}.expr", base_path), &mut warnings);
        }
        if let Some(expr) = &mapping.when {
            collect_expr_warnings(expr, &format!("{}.when", base_path), &mut warnings);
        }
    }
    warnings
}

fn collect_expr_warnings(expr: &Expr, path: &str, warnings: &mut Vec<RuleWarning>) {
    match expr {
        Expr::Ref(_) | Expr::Literal(_) => {}
        Expr::Op(expr_op) => collect_op_warnings(expr_op, path, false, warnings),
        Expr::Chain(chain) => collect_chain_warnings(chain, path, warnings),
    }
}

fn collect_chain_warnings(chain: &ExprChain, path: &str, warnings: &mut Vec<RuleWarning>) {
    for (index, step) in chain.chain.iter().enumerate() {
        let step_path = format!("{}.chain[{}]", path, index);
        if index == 0 {
            collect_expr_warnings(step, &step_path, warnings);
            continue;
        }

        match step {
            Expr::Op(expr_op) => collect_op_warnings(expr_op, &step_path, true, warnings),
            _ => collect_expr_warnings(step, &step_path, warnings),
        }
    }
}

fn collect_op_warnings(
    expr_op: &ExprOp,
    path: &str,
    chain_step: bool,
    warnings: &mut Vec<RuleWarning>,
) {
    if expr_op.op == "date_format" {
        warn_date_format_missing_input_format(expr_op, path, chain_step, warnings);
    } else if expr_op.op == "to_unixtime" {
        warnings.push(RuleWarning {
            code: "to_unixtime_auto_parse",
            message: "to_unixtime relies on heuristic date parsing; consider normalizing with date_format + input_format.".to_string(),
            path: Some(path.to_string()),
        });
    }

    for (index, arg) in expr_op.args.iter().enumerate() {
        let arg_path = format!("{}.args[{}]", path, index);
        collect_expr_warnings(arg, &arg_path, warnings);
    }
}

fn warn_date_format_missing_input_format(
    expr_op: &ExprOp,
    path: &str,
    chain_step: bool,
    warnings: &mut Vec<RuleWarning>,
) {
    let input_index = if chain_step { 1 } else { 2 };
    if expr_op.args.len() <= input_index {
        warnings.push(RuleWarning {
            code: "date_format_missing_input_format",
            message: "date_format without input_format relies on heuristic parsing; consider providing input_format.".to_string(),
            path: Some(format!("{}.args", path)),
        });
        return;
    }

    if expr_looks_like_timezone(&expr_op.args[input_index]) {
        warnings.push(RuleWarning {
            code: "date_format_missing_input_format",
            message: "date_format without input_format relies on heuristic parsing; consider providing input_format.".to_string(),
            path: Some(format!("{}.args[{}]", path, input_index)),
        });
    }
}

fn expr_looks_like_timezone(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(Value::String(value)) => looks_like_timezone(value),
        _ => false,
    }
}

fn looks_like_timezone(value: &str) -> bool {
    if value.eq_ignore_ascii_case("utc") || value == "Z" {
        return true;
    }
    matches!(value.chars().next(), Some('+') | Some('-'))
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

fn rule_warnings_to_json(warnings: &[RuleWarning]) -> Value {
    let values: Vec<_> = warnings.iter().map(rule_warning_json).collect();
    Value::Array(values)
}

fn rule_warning_json(warning: &RuleWarning) -> Value {
    let mut value = json!({
        "type": "warning",
        "code": warning.code,
        "message": warning.message,
    });
    if let Some(path) = &warning.path {
        value["path"] = json!(path);
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
