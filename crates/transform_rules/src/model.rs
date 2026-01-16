use serde::Deserialize;
use serde_json::Value as JsonValue;

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct RuleFile {
    pub version: u8,
    pub input: InputSpec,
    #[serde(default)]
    pub output: Option<OutputSpec>,
    pub mappings: Vec<Mapping>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct OutputSpec {
    pub name: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct InputSpec {
    pub format: InputFormat,
    pub csv: Option<CsvInput>,
    pub json: Option<JsonInput>,
}

#[derive(Debug, Deserialize, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum InputFormat {
    Csv,
    Json,
}

fn default_true() -> bool {
    true
}

fn default_delimiter() -> String {
    ",".to_string()
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct CsvInput {
    #[serde(default = "default_true")]
    pub has_header: bool,
    #[serde(default = "default_delimiter")]
    pub delimiter: String,
    pub columns: Option<Vec<Column>>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct Column {
    pub name: String,
    #[serde(rename = "type")]
    pub value_type: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct JsonInput {
    pub records_path: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct Mapping {
    pub target: String,
    pub source: Option<String>,
    pub value: Option<JsonValue>,
    pub expr: Option<Expr>,
    pub when: Option<Expr>,
    #[serde(rename = "type")]
    pub value_type: Option<String>,
    #[serde(default)]
    pub required: bool,
    pub default: Option<JsonValue>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(untagged)]
pub enum Expr {
    Ref(ExprRef),
    Op(ExprOp),
    Chain(ExprChain),
    Literal(JsonValue),
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct ExprRef {
    #[serde(rename = "ref")]
    pub ref_path: String,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct ExprOp {
    pub op: String,
    #[serde(default)]
    pub args: Vec<Expr>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct ExprChain {
    pub chain: Vec<Expr>,
}
