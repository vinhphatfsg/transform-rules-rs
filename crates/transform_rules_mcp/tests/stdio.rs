use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, Command, Stdio};

use serde_json::{json, Value};
use tempfile::tempdir;
use transform_rules::parse_rule_file;

struct McpServer {
    child: Child,
    stdin: Option<ChildStdin>,
    stdout: BufReader<std::process::ChildStdout>,
}

impl McpServer {
    fn start() -> Self {
        let bin = env!("CARGO_BIN_EXE_transform-rules-mcp");
        let mut child = Command::new(bin)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn mcp server");

        let stdin = child.stdin.take().expect("take stdin");
        let stdout = child.stdout.take().expect("take stdout");

        Self {
            child,
            stdin: Some(stdin),
            stdout: BufReader::new(stdout),
        }
    }

    fn send(&mut self, message: &Value) -> Value {
        let text = serde_json::to_string(message).expect("serialize request");
        let stdin = self.stdin.as_mut().expect("stdin available");
        writeln!(stdin, "{}", text).expect("write request");
        stdin.flush().expect("flush request");

        let mut line = String::new();
        self.stdout
            .read_line(&mut line)
            .expect("read response");
        assert!(!line.trim().is_empty(), "empty response");
        serde_json::from_str(&line).expect("parse response")
    }

    fn shutdown(mut self) {
        self.stdin.take();
        let _ = self.child.wait();
    }
}

fn initialize(server: &mut McpServer) {
    let request = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {
                "name": "tests",
                "version": "0.0"
            }
        }
    });
    let response = server.send(&request);
    assert_eq!(response["result"]["protocolVersion"], "2024-11-05");
}

#[test]
fn initialize_and_list_tools() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let request = json!({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "tools/list"
    });
    let response = server.send(&request);

    let tools = response["result"]["tools"]
        .as_array()
        .expect("tools array");
    let expected = [
        "transform",
        "validate_rules",
        "generate_dto",
        "list_ops",
        "analyze_input",
        "generate_rules_from_base",
        "generate_rules_from_dto",
    ];
    for name in expected {
        assert!(tools.iter().any(|tool| tool["name"] == name));
    }

    server.shutdown();
}

#[test]
fn transform_json_success() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let dir = tempdir().expect("temp dir");
    let rules_path = dir.path().join("rules.yaml");
    let input_path = dir.path().join("input.json");

    fs::write(
        &rules_path,
        r#"version: 1
input:
  format: json
  json: {}
mappings:
  - target: "id"
    source: "id"
"#,
    )
    .expect("write rules");
    fs::write(&input_path, r#"{"id": 1}"#).expect("write input");

    let request = json!({
        "jsonrpc": "2.0",
        "id": 3,
        "method": "tools/call",
        "params": {
            "name": "transform",
            "arguments": {
                "rules_path": rules_path.to_string_lossy(),
                "input_path": input_path.to_string_lossy()
            }
        }
    });

    let response = server.send(&request);
    let output_text = response["result"]["content"][0]["text"]
        .as_str()
        .expect("output text");
    let output: Value = serde_json::from_str(output_text).expect("output json");

    assert_eq!(output, json!([{ "id": 1 }]));
    assert!(response["result"]["isError"].is_null() || response["result"]["isError"] == false);

    server.shutdown();
}

#[test]
fn tools_call_invalid_params_returns_error() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let request = json!({
        "jsonrpc": "2.0",
        "id": 4,
        "method": "tools/call",
        "params": {
            "name": "transform"
        }
    });

    let response = server.send(&request);
    assert_eq!(response["error"]["code"], -32602);

    server.shutdown();
}

#[test]
fn tools_call_missing_files_returns_tool_error() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let request = json!({
        "jsonrpc": "2.0",
        "id": 5,
        "method": "tools/call",
        "params": {
            "name": "transform",
            "arguments": {
                "rules_path": "nope.yaml",
                "input_path": "nope.json"
            }
        }
    });

    let response = server.send(&request);
    assert_eq!(response["result"]["isError"], true);
    let message = response["result"]["content"][0]["text"]
        .as_str()
        .expect("error text");
    assert!(message.contains("failed to read rules"));

    server.shutdown();
}

#[test]
fn ndjson_and_output_path() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let dir = tempdir().expect("temp dir");
    let rules_path = dir.path().join("rules.yaml");
    let input_path = dir.path().join("input.json");
    let output_path = dir.path().join("out.ndjson");

    fs::write(
        &rules_path,
        r#"version: 1
input:
  format: json
  json: {}
mappings:
  - target: "id"
    source: "id"
"#,
    )
    .expect("write rules");
    fs::write(&input_path, r#"[{"id": 1}, {"id": 2}]"#).expect("write input");

    let request = json!({
        "jsonrpc": "2.0",
        "id": 6,
        "method": "tools/call",
        "params": {
            "name": "transform",
            "arguments": {
                "rules_path": rules_path.to_string_lossy(),
                "input_path": input_path.to_string_lossy(),
                "ndjson": true,
                "output_path": output_path.to_string_lossy()
            }
        }
    });

    let response = server.send(&request);
    let output_text = response["result"]["content"][0]["text"]
        .as_str()
        .expect("output text");
    let lines: Vec<&str> = output_text.trim_end_matches('\n').split('\n').collect();

    assert_eq!(lines.len(), 2);
    let first: Value = serde_json::from_str(lines[0]).expect("line 1 json");
    let second: Value = serde_json::from_str(lines[1]).expect("line 2 json");
    assert_eq!(first, json!({"id": 1}));
    assert_eq!(second, json!({"id": 2}));

    let output_file = fs::read_to_string(&output_path).expect("read output file");
    assert_eq!(output_file, output_text);

    server.shutdown();
}

#[test]
fn transform_csv_success() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let dir = tempdir().expect("temp dir");
    let rules_path = dir.path().join("rules.yaml");
    let input_path = dir.path().join("input.csv");

    fs::write(
        &rules_path,
        r#"version: 1
input:
  format: csv
  csv: {}
mappings:
  - target: "name"
    source: "name"
  - target: "age"
    source: "age"
"#,
    )
    .expect("write rules");
    fs::write(&input_path, "name,age\nAlice,30\nBob,25\n").expect("write input");

    let request = json!({
        "jsonrpc": "2.0",
        "id": 7,
        "method": "tools/call",
        "params": {
            "name": "transform",
            "arguments": {
                "rules_path": rules_path.to_string_lossy(),
                "input_path": input_path.to_string_lossy()
            }
        }
    });

    let response = server.send(&request);
    let output_text = response["result"]["content"][0]["text"]
        .as_str()
        .expect("output text");
    let output: Value = serde_json::from_str(output_text).expect("output json");

    assert_eq!(
        output,
        json!([
            { "name": "Alice", "age": "30" },
            { "name": "Bob", "age": "25" }
        ])
    );

    server.shutdown();
}

#[test]
fn validate_rules_success() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let rules_text = r#"version: 1
input:
  format: json
  json: {}
mappings:
  - target: "id"
    source: "id"
"#;

    let request = json!({
        "jsonrpc": "2.0",
        "id": 8,
        "method": "tools/call",
        "params": {
            "name": "validate_rules",
            "arguments": {
                "rules_text": rules_text
            }
        }
    });

    let response = server.send(&request);
    let text = response["result"]["content"][0]["text"]
        .as_str()
        .expect("result text");
    assert_eq!(text, "ok");

    server.shutdown();
}

#[test]
fn validate_rules_failure() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let rules_text = r#"version: 1
input:
  format: csv
mappings: []
"#;

    let request = json!({
        "jsonrpc": "2.0",
        "id": 9,
        "method": "tools/call",
        "params": {
            "name": "validate_rules",
            "arguments": {
                "rules_text": rules_text
            }
        }
    });

    let response = server.send(&request);
    assert_eq!(response["result"]["isError"], true);
    assert!(response["result"]["meta"]["errors"].is_array());

    server.shutdown();
}

#[test]
fn generate_dto_typescript() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let rules_text = r#"version: 1
input:
  format: json
  json: {}
mappings:
  - target: "id"
    source: "id"
"#;

    let request = json!({
        "jsonrpc": "2.0",
        "id": 10,
        "method": "tools/call",
        "params": {
            "name": "generate_dto",
            "arguments": {
                "rules_text": rules_text,
                "language": "typescript"
            }
        }
    });

    let response = server.send(&request);
    let text = response["result"]["content"][0]["text"]
        .as_str()
        .expect("dto text");
    assert!(text.contains("export interface"));

    server.shutdown();
}

#[test]
fn list_ops_success() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let request = json!({
        "jsonrpc": "2.0",
        "id": 11,
        "method": "tools/call",
        "params": {
            "name": "list_ops",
            "arguments": {}
        }
    });

    let response = server.send(&request);
    assert!(response["result"]["meta"]["ops"]["type_casts"].is_array());
    assert!(response["result"]["meta"]["ops"]["categories"]["json_ops"].is_array());
    assert!(response["result"]["meta"]["ops"]["categories"]["array_ops"].is_array());
    assert!(response["result"]["meta"]["ops"]["category_docs"]["json_ops"]["examples"].is_array());
    assert!(response["result"]["meta"]["ops"]["category_docs"]["string_ops"]["examples"].is_array());

    server.shutdown();
}

#[test]
fn analyze_input_json_success() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let request = json!({
        "jsonrpc": "2.0",
        "id": 12,
        "method": "tools/call",
        "params": {
            "name": "analyze_input",
            "arguments": {
                "input_json": {
                    "id": 1,
                    "name": "Ada"
                }
            }
        }
    });

    let response = server.send(&request);
    let paths = response["result"]["meta"]["paths"]
        .as_array()
        .expect("paths array");
    assert!(paths.iter().any(|item| item["path"] == "id"));
    assert!(paths.iter().any(|item| item["path"] == "name"));

    server.shutdown();
}

#[test]
fn analyze_input_csv_success() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let request = json!({
        "jsonrpc": "2.0",
        "id": 13,
        "method": "tools/call",
        "params": {
            "name": "analyze_input",
            "arguments": {
                "input_text": "id,name\n1,Ada\n2,Bob\n",
                "format": "csv"
            }
        }
    });

    let response = server.send(&request);
    let paths = response["result"]["meta"]["paths"]
        .as_array()
        .expect("paths array");
    assert!(paths.iter().any(|item| item["path"] == "id"));

    server.shutdown();
}

#[test]
fn generate_rules_from_base_success() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let rules_text = r#"version: 1
input:
  format: json
  json: {}
mappings:
  - target: "id"
    source: "old_id"
  - target: "name"
    source: "old_name"
"#;

    let request = json!({
        "jsonrpc": "2.0",
        "id": 14,
        "method": "tools/call",
        "params": {
            "name": "generate_rules_from_base",
            "arguments": {
                "rules_text": rules_text,
                "input_json": {
                    "id": 1,
                    "name": "Ada"
                }
            }
        }
    });

    let response = server.send(&request);
    let output_text = response["result"]["content"][0]["text"]
        .as_str()
        .expect("output text");
    let rule = parse_rule_file(output_text).expect("parse output rules");
    assert_eq!(rule.mappings[0].source.as_deref(), Some("id"));
    assert_eq!(rule.mappings[1].source.as_deref(), Some("name"));

    server.shutdown();
}

#[test]
fn generate_rules_from_dto_success() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let dto_text = r#"export interface Record {
  id: string;
  name?: string;
}"#;

    let request = json!({
        "jsonrpc": "2.0",
        "id": 15,
        "method": "tools/call",
        "params": {
            "name": "generate_rules_from_dto",
            "arguments": {
                "dto_text": dto_text,
                "dto_language": "typescript",
                "input_json": {
                    "id": 1,
                    "name": "Ada"
                }
            }
        }
    });

    let response = server.send(&request);
    let output_text = response["result"]["content"][0]["text"]
        .as_str()
        .expect("output text");
    let rule = parse_rule_file(output_text).expect("parse output rules");
    assert_eq!(rule.mappings[0].source.as_deref(), Some("id"));
    assert_eq!(rule.mappings[1].source.as_deref(), Some("name"));

    server.shutdown();
}

#[test]
fn generate_rules_from_dto_single_line_interface() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let dto_text = "export interface Record { id: string; name?: string; }";

    let request = json!({
        "jsonrpc": "2.0",
        "id": 16,
        "method": "tools/call",
        "params": {
            "name": "generate_rules_from_dto",
            "arguments": {
                "dto_text": dto_text,
                "dto_language": "typescript",
                "input_json": {
                    "id": 1,
                    "name": "Ada"
                }
            }
        }
    });

    let response = server.send(&request);
    let output_text = response["result"]["content"][0]["text"]
        .as_str()
        .expect("output text");
    let rule = parse_rule_file(output_text).expect("parse output rules");
    assert_eq!(rule.mappings[0].source.as_deref(), Some("id"));
    assert_eq!(rule.mappings[1].source.as_deref(), Some("name"));

    server.shutdown();
}

#[test]
fn generate_rules_from_dto_single_line_rust_struct() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let dto_text = "pub struct Record { pub id: String, pub name: Option<String>, pub price: f64 }";

    let request = json!({
        "jsonrpc": "2.0",
        "id": 19,
        "method": "tools/call",
        "params": {
            "name": "generate_rules_from_dto",
            "arguments": {
                "dto_text": dto_text,
                "dto_language": "rust",
                "input_json": {
                    "id": "001",
                    "name": "Ada",
                    "price": 100.0
                }
            }
        }
    });

    let response = server.send(&request);
    let output_text = response["result"]["content"][0]["text"]
        .as_str()
        .expect("output text");
    let rule = parse_rule_file(output_text).expect("parse output rules");
    assert_eq!(rule.mappings[0].source.as_deref(), Some("id"));
    assert_eq!(rule.mappings[1].source.as_deref(), Some("name"));
    assert_eq!(rule.mappings[2].source.as_deref(), Some("price"));

    server.shutdown();
}

#[test]
fn generate_rules_from_dto_python_single_line_alias() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let dto_text = "class Record(BaseModel): id: str; name: Optional[str] = None; price: float = Field(alias=\"price_cents\")";

    let request = json!({
        "jsonrpc": "2.0",
        "id": 20,
        "method": "tools/call",
        "params": {
            "name": "generate_rules_from_dto",
            "arguments": {
                "dto_text": dto_text,
                "dto_language": "python",
                "input_json": {
                    "id": "001",
                    "name": "Ada",
                    "price_cents": 100.0
                }
            }
        }
    });

    let response = server.send(&request);
    let output_text = response["result"]["content"][0]["text"]
        .as_str()
        .expect("output text");
    let rule = parse_rule_file(output_text).expect("parse output rules");

    let id_mapping = rule
        .mappings
        .iter()
        .find(|mapping| mapping.target == "id")
        .expect("id mapping");
    assert_eq!(id_mapping.source.as_deref(), Some("id"));
    assert!(id_mapping.required);

    let name_mapping = rule
        .mappings
        .iter()
        .find(|mapping| mapping.target == "name")
        .expect("name mapping");
    assert_eq!(name_mapping.source.as_deref(), Some("name"));
    assert!(!name_mapping.required);

    let price_mapping = rule
        .mappings
        .iter()
        .find(|mapping| mapping.target == "price_cents")
        .expect("price mapping");
    assert_eq!(price_mapping.source.as_deref(), Some("price_cents"));
    assert!(price_mapping.required);

    server.shutdown();
}

#[test]
fn generate_rules_from_dto_go_single_line_tags() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let dto_text = "type Record struct { ID string `json:\"id\"` Name *string `json:\"name,omitempty\"` Price float64 `json:\"price\"` }";

    let request = json!({
        "jsonrpc": "2.0",
        "id": 21,
        "method": "tools/call",
        "params": {
            "name": "generate_rules_from_dto",
            "arguments": {
                "dto_text": dto_text,
                "dto_language": "go",
                "input_json": {
                    "id": "001",
                    "name": "Ada",
                    "price": 100.0
                }
            }
        }
    });

    let response = server.send(&request);
    let output_text = response["result"]["content"][0]["text"]
        .as_str()
        .expect("output text");
    let rule = parse_rule_file(output_text).expect("parse output rules");

    let id_mapping = rule
        .mappings
        .iter()
        .find(|mapping| mapping.target == "id")
        .expect("id mapping");
    assert_eq!(id_mapping.source.as_deref(), Some("id"));
    assert!(id_mapping.required);

    let name_mapping = rule
        .mappings
        .iter()
        .find(|mapping| mapping.target == "name")
        .expect("name mapping");
    assert_eq!(name_mapping.source.as_deref(), Some("name"));
    assert!(!name_mapping.required);

    server.shutdown();
}

#[test]
fn generate_rules_from_dto_java_single_line_annotations() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let dto_text = "public class Record { @JsonProperty(\"user_id\") private String id; @SerializedName(\"full_name\") private Optional<String> name; }";

    let request = json!({
        "jsonrpc": "2.0",
        "id": 22,
        "method": "tools/call",
        "params": {
            "name": "generate_rules_from_dto",
            "arguments": {
                "dto_text": dto_text,
                "dto_language": "java",
                "input_json": {
                    "user_id": "001",
                    "full_name": "Ada"
                }
            }
        }
    });

    let response = server.send(&request);
    let output_text = response["result"]["content"][0]["text"]
        .as_str()
        .expect("output text");
    let rule = parse_rule_file(output_text).expect("parse output rules");

    let id_mapping = rule
        .mappings
        .iter()
        .find(|mapping| mapping.target == "user_id")
        .expect("id mapping");
    assert_eq!(id_mapping.source.as_deref(), Some("user_id"));
    assert!(id_mapping.required);

    let name_mapping = rule
        .mappings
        .iter()
        .find(|mapping| mapping.target == "full_name")
        .expect("name mapping");
    assert_eq!(name_mapping.source.as_deref(), Some("full_name"));
    assert!(!name_mapping.required);

    server.shutdown();
}

#[test]
fn generate_rules_from_dto_kotlin_single_line_annotations() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let dto_text = "data class Record(@SerialName(\"user_id\") val id: String, @Json(name = \"full_name\") val name: String?, val price: Double)";

    let request = json!({
        "jsonrpc": "2.0",
        "id": 23,
        "method": "tools/call",
        "params": {
            "name": "generate_rules_from_dto",
            "arguments": {
                "dto_text": dto_text,
                "dto_language": "kotlin",
                "input_json": {
                    "user_id": "001",
                    "full_name": "Ada",
                    "price": 100.0
                }
            }
        }
    });

    let response = server.send(&request);
    let output_text = response["result"]["content"][0]["text"]
        .as_str()
        .expect("output text");
    let rule = parse_rule_file(output_text).expect("parse output rules");

    let id_mapping = rule
        .mappings
        .iter()
        .find(|mapping| mapping.target == "user_id")
        .expect("id mapping");
    assert_eq!(id_mapping.source.as_deref(), Some("user_id"));
    assert!(id_mapping.required);

    let name_mapping = rule
        .mappings
        .iter()
        .find(|mapping| mapping.target == "full_name")
        .expect("name mapping");
    assert_eq!(name_mapping.source.as_deref(), Some("full_name"));
    assert!(!name_mapping.required);

    let price_mapping = rule
        .mappings
        .iter()
        .find(|mapping| mapping.target == "price")
        .expect("price mapping");
    assert_eq!(price_mapping.source.as_deref(), Some("price"));
    assert!(price_mapping.required);

    server.shutdown();
}

#[test]
fn generate_rules_from_dto_swift_single_line_coding_keys() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let dto_text = "struct Record: Codable { let id: String; let name: String?; let price: Double; enum CodingKeys: String, CodingKey { case id = \"user_id\", name, price = \"price_cents\" } }";

    let request = json!({
        "jsonrpc": "2.0",
        "id": 24,
        "method": "tools/call",
        "params": {
            "name": "generate_rules_from_dto",
            "arguments": {
                "dto_text": dto_text,
                "dto_language": "swift",
                "input_json": {
                    "user_id": "001",
                    "name": "Ada",
                    "price_cents": 100.0
                }
            }
        }
    });

    let response = server.send(&request);
    let output_text = response["result"]["content"][0]["text"]
        .as_str()
        .expect("output text");
    let rule = parse_rule_file(output_text).expect("parse output rules");

    let id_mapping = rule
        .mappings
        .iter()
        .find(|mapping| mapping.target == "user_id")
        .expect("id mapping");
    assert_eq!(id_mapping.source.as_deref(), Some("user_id"));
    assert!(id_mapping.required);

    let name_mapping = rule
        .mappings
        .iter()
        .find(|mapping| mapping.target == "name")
        .expect("name mapping");
    assert_eq!(name_mapping.source.as_deref(), Some("name"));
    assert!(!name_mapping.required);

    let price_mapping = rule
        .mappings
        .iter()
        .find(|mapping| mapping.target == "price_cents")
        .expect("price mapping");
    assert_eq!(price_mapping.source.as_deref(), Some("price_cents"));
    assert!(price_mapping.required);

    server.shutdown();
}

#[test]
fn resources_list_and_read() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let list_request = json!({
        "jsonrpc": "2.0",
        "id": 17,
        "method": "resources/list"
    });
    let list_response = server.send(&list_request);
    let resources = list_response["result"]["resources"]
        .as_array()
        .expect("resources array");
    assert!(resources.iter().any(|item| item["uri"] == "transform-rules://docs/rules_spec_en"));

    let read_request = json!({
        "jsonrpc": "2.0",
        "id": 18,
        "method": "resources/read",
        "params": {
            "uri": "transform-rules://docs/rules_spec_en"
        }
    });
    let read_response = server.send(&read_request);
    let text = read_response["result"]["contents"][0]["text"]
        .as_str()
        .expect("resource text");
    assert!(text.contains("Expr"));

    server.shutdown();
}

#[test]
fn prompts_list_and_get() {
    let mut server = McpServer::start();
    initialize(&mut server);

    let list_request = json!({
        "jsonrpc": "2.0",
        "id": 18,
        "method": "prompts/list"
    });
    let list_response = server.send(&list_request);
    let prompts = list_response["result"]["prompts"]
        .as_array()
        .expect("prompts array");
    assert!(prompts.iter().any(|item| item["name"] == "rule_from_input_base"));

    let get_request = json!({
        "jsonrpc": "2.0",
        "id": 19,
        "method": "prompts/get",
        "params": {
            "name": "explain_errors",
            "arguments": {
                "errors_json": "[{\"message\":\"oops\"}]"
            }
        }
    });
    let get_response = server.send(&get_request);
    let content = get_response["result"]["messages"][0]["content"]
        .as_str()
        .expect("prompt content");
    assert!(content.contains("Errors:"));

    server.shutdown();
}
