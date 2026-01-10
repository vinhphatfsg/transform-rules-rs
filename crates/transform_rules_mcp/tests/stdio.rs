use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, Command, Stdio};

use serde_json::{json, Value};
use tempfile::tempdir;

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
    assert!(tools.iter().any(|tool| tool["name"] == "transform"));

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
