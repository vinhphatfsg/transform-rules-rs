use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordUser {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<Value>,
    pub age: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record {
    pub id: String,
    pub user: RecordUser,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub price: Option<f64>,
    pub active: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub meta: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none", rename = "user-name")]
    pub user_name: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub class: Option<Value>,
    pub status: String,
    pub source: String,
}
