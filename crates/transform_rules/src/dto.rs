use std::collections::{HashMap, HashSet};

use crate::model::RuleFile;
use crate::path::{parse_path, PathToken};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DtoLanguage {
    Rust,
    TypeScript,
    Python,
    Go,
    Java,
    Kotlin,
    Swift,
}

#[derive(Debug, Clone)]
pub struct DtoError {
    message: String,
}

impl DtoError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl std::fmt::Display for DtoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for DtoError {}

pub fn generate_dto(
    rule: &RuleFile,
    language: DtoLanguage,
    name: Option<&str>,
) -> Result<String, DtoError> {
    let name = name.unwrap_or("Record");
    let schema = build_schema(rule)?;

    match language {
        DtoLanguage::Rust => render_rust(&schema, name),
        DtoLanguage::TypeScript => render_typescript(&schema, name),
        DtoLanguage::Python => render_python(&schema, name),
        DtoLanguage::Go => render_go(&schema, name),
        DtoLanguage::Java => render_java(&schema, name),
        DtoLanguage::Kotlin => render_kotlin(&schema, name),
        DtoLanguage::Swift => render_swift(&schema, name),
    }
}

#[derive(Clone)]
struct SchemaNode {
    fields: Vec<Field>,
}

#[derive(Clone)]
struct Field {
    key: String,
    field_type: FieldType,
    optional: bool,
}

#[derive(Clone)]
enum FieldType {
    Primitive(PrimitiveType),
    Object(Box<SchemaNode>),
    JsonValue,
}

#[derive(Clone, Copy)]
enum PrimitiveType {
    String,
    Int,
    Float,
    Bool,
}

fn build_schema(rule: &RuleFile) -> Result<SchemaNode, DtoError> {
    let mut root = SchemaNode { fields: Vec::new() };

    for mapping in &rule.mappings {
        let tokens = parse_path(&mapping.target)
            .map_err(|_| DtoError::new("target path is invalid"))?;
        if tokens.iter().any(|token| matches!(token, PathToken::Index(_))) {
            return Err(DtoError::new("target path must not include indexes"));
        }

        let mut keys = Vec::new();
        for token in tokens {
            match token {
                PathToken::Key(key) => keys.push(key),
                PathToken::Index(_) => {}
            }
        }

        if keys.is_empty() {
            return Err(DtoError::new("target path is invalid"));
        }

        let field_type = match mapping.value_type.as_deref() {
            Some("string") => FieldType::Primitive(PrimitiveType::String),
            Some("int") => FieldType::Primitive(PrimitiveType::Int),
            Some("float") => FieldType::Primitive(PrimitiveType::Float),
            Some("bool") => FieldType::Primitive(PrimitiveType::Bool),
            Some(_) => return Err(DtoError::new("unsupported type in mapping")),
            None => FieldType::JsonValue,
        };
        let optional = !(mapping.required || mapping.value.is_some() || mapping.default.is_some());

        insert_field(&mut root, &keys, field_type, optional)?;
    }

    Ok(root)
}

fn insert_field(
    node: &mut SchemaNode,
    keys: &[String],
    field_type: FieldType,
    optional: bool,
) -> Result<(), DtoError> {
    if keys.is_empty() {
        return Err(DtoError::new("target path is invalid"));
    }

    let key = &keys[0];
    if keys.len() == 1 {
        if node.fields.iter().any(|field| field.key == *key) {
            return Err(DtoError::new("duplicate target in dto"));
        }
        node.fields.push(Field {
            key: key.clone(),
            field_type,
            optional,
        });
        return Ok(());
    }

    if let Some(field) = node.fields.iter_mut().find(|field| field.key == *key) {
        match &mut field.field_type {
            FieldType::Object(child) => {
                return insert_field(child, &keys[1..], field_type, optional)
            }
            _ => return Err(DtoError::new("target conflicts with non-object")),
        }
    }

    let mut child = SchemaNode { fields: Vec::new() };
    insert_field(&mut child, &keys[1..], field_type, optional)?;
    node.fields.push(Field {
        key: key.clone(),
        field_type: FieldType::Object(Box::new(child)),
        optional: false,
    });
    Ok(())
}

fn node_has_required(node: &SchemaNode) -> bool {
    for field in &node.fields {
        match &field.field_type {
            FieldType::Object(child) => {
                if node_has_required(child) {
                    return true;
                }
            }
            _ => {
                if !field.optional {
                    return true;
                }
            }
        }
    }
    false
}

fn node_uses_json(node: &SchemaNode) -> bool {
    for field in &node.fields {
        match &field.field_type {
            FieldType::JsonValue => return true,
            FieldType::Object(child) => {
                if node_uses_json(child) {
                    return true;
                }
            }
            _ => {}
        }
    }
    false
}

struct TypeDef<'a> {
    name: String,
    node: &'a SchemaNode,
    path: Vec<String>,
}

struct NameRegistry {
    base: String,
    used: HashSet<String>,
    names: HashMap<Vec<String>, String>,
}

impl NameRegistry {
    fn new(base: &str) -> Self {
        Self {
            base: base.to_string(),
            used: HashSet::new(),
            names: HashMap::new(),
        }
    }

    fn type_name_for_path(&mut self, path: &[String]) -> String {
        if let Some(name) = self.names.get(path) {
            return name.clone();
        }

        let mut name = self.base.clone();
        for segment in path {
            name.push_str(&pascal_case(&words_from_key(segment)));
        }

        if name.is_empty() {
            name = "Record".to_string();
        }

        let mut unique = name.clone();
        let mut suffix = 2;
        while self.used.contains(&unique) {
            unique = format!("{}_{}", name, suffix);
            suffix += 1;
        }
        self.used.insert(unique.clone());
        self.names.insert(path.to_vec(), unique.clone());
        unique
    }

    fn get(&self, path: &[String]) -> Option<&String> {
        self.names.get(path)
    }
}

fn collect_types<'a>(
    node: &'a SchemaNode,
    path: Vec<String>,
    registry: &mut NameRegistry,
    out: &mut Vec<TypeDef<'a>>,
) {
    for field in &node.fields {
        if let FieldType::Object(child) = &field.field_type {
            let mut child_path = path.clone();
            child_path.push(field.key.clone());
            registry.type_name_for_path(&child_path);
            collect_types(child, child_path, registry, out);
        }
    }

    let name = registry.type_name_for_path(&path);
    out.push(TypeDef { name, node, path });
}

fn field_identifier(
    lang: DtoLanguage,
    key: &str,
    used: &mut HashMap<String, usize>,
) -> String {
    let base = match lang {
        DtoLanguage::Rust | DtoLanguage::Python => snake_case(&words_from_key(key)),
        DtoLanguage::TypeScript | DtoLanguage::Java | DtoLanguage::Kotlin | DtoLanguage::Swift => {
            lower_camel(&words_from_key(key))
        }
        DtoLanguage::Go => pascal_case(&words_from_key(key)),
    };

    let mut ident = if base.is_empty() {
        match lang {
            DtoLanguage::Go => "Field".to_string(),
            DtoLanguage::Java | DtoLanguage::Kotlin | DtoLanguage::Swift => "field".to_string(),
            _ => "field".to_string(),
        }
    } else {
        base
    };

    if ident
        .chars()
        .next()
        .map(|c| c.is_ascii_digit())
        .unwrap_or(true)
    {
        ident = match lang {
            DtoLanguage::Go => format!("Field{}", ident),
            DtoLanguage::Java | DtoLanguage::Kotlin | DtoLanguage::Swift => {
                format!("field{}", capitalize(&ident))
            }
            _ => format!("_{}", ident),
        };
    }

    if is_reserved(lang, &ident) {
        ident = match lang {
            DtoLanguage::Go => format!("{}Field", ident),
            _ => format!("{}_", ident),
        };
    }

    let entry = used.entry(ident.clone()).or_insert(0);
    if *entry > 0 {
        *entry += 1;
        format!("{}_{}", ident, *entry)
    } else {
        *entry = 1;
        ident
    }
}

fn words_from_key(key: &str) -> Vec<String> {
    let mut words = Vec::new();
    let mut current = String::new();
    for ch in key.chars() {
        if ch.is_ascii_alphanumeric() {
            current.push(ch);
        } else if !current.is_empty() {
            words.push(current);
            current = String::new();
        }
    }
    if !current.is_empty() {
        words.push(current);
    }
    if words.is_empty() {
        words.push("field".to_string());
    }
    words
}

fn snake_case(words: &[String]) -> String {
    words
        .iter()
        .map(|word| word.to_lowercase())
        .collect::<Vec<String>>()
        .join("_")
}

fn lower_camel(words: &[String]) -> String {
    if words.is_empty() {
        return String::new();
    }

    let mut iter = words.iter();
    let first = iter
        .next()
        .map(|word| word.to_lowercase())
        .unwrap_or_default();
    let mut result = first;
    for word in iter {
        result.push_str(&capitalize(word));
    }
    result
}

fn pascal_case(words: &[String]) -> String {
    let mut result = String::new();
    for word in words {
        result.push_str(&capitalize(word));
    }
    result
}

fn capitalize(value: &str) -> String {
    let mut chars = value.chars();
    match chars.next() {
        Some(first) => first.to_uppercase().collect::<String>() + &chars.as_str().to_lowercase(),
        None => String::new(),
    }
}

fn is_reserved(lang: DtoLanguage, ident: &str) -> bool {
    match lang {
        DtoLanguage::Rust => is_reserved_rust(ident),
        DtoLanguage::TypeScript => is_reserved_typescript(ident),
        DtoLanguage::Python => is_reserved_python(ident),
        DtoLanguage::Go => is_reserved_go(ident),
        DtoLanguage::Java => is_reserved_java(ident),
        DtoLanguage::Kotlin => is_reserved_kotlin(ident),
        DtoLanguage::Swift => is_reserved_swift(ident),
    }
}

fn is_reserved_rust(value: &str) -> bool {
    matches!(
        value,
        "as"
            | "break"
            | "const"
            | "continue"
            | "crate"
            | "else"
            | "enum"
            | "extern"
            | "false"
            | "fn"
            | "for"
            | "if"
            | "impl"
            | "in"
            | "let"
            | "loop"
            | "match"
            | "mod"
            | "move"
            | "mut"
            | "pub"
            | "ref"
            | "return"
            | "self"
            | "Self"
            | "static"
            | "struct"
            | "super"
            | "trait"
            | "true"
            | "type"
            | "unsafe"
            | "use"
            | "where"
            | "while"
    )
}

fn is_reserved_typescript(value: &str) -> bool {
    matches!(
        value,
        "break"
            | "case"
            | "catch"
            | "class"
            | "const"
            | "continue"
            | "debugger"
            | "default"
            | "delete"
            | "do"
            | "else"
            | "enum"
            | "export"
            | "extends"
            | "false"
            | "finally"
            | "for"
            | "function"
            | "if"
            | "import"
            | "in"
            | "instanceof"
            | "new"
            | "null"
            | "return"
            | "super"
            | "switch"
            | "this"
            | "throw"
            | "true"
            | "try"
            | "typeof"
            | "var"
            | "void"
            | "while"
            | "with"
            | "as"
            | "implements"
            | "interface"
            | "let"
            | "package"
            | "private"
            | "protected"
            | "public"
            | "static"
            | "yield"
            | "any"
            | "boolean"
            | "number"
            | "string"
            | "symbol"
            | "type"
            | "from"
            | "of"
    )
}

fn is_reserved_python(value: &str) -> bool {
    matches!(
        value,
        "False"
            | "None"
            | "True"
            | "and"
            | "as"
            | "assert"
            | "async"
            | "await"
            | "break"
            | "class"
            | "continue"
            | "def"
            | "del"
            | "elif"
            | "else"
            | "except"
            | "finally"
            | "for"
            | "from"
            | "global"
            | "if"
            | "import"
            | "in"
            | "is"
            | "lambda"
            | "nonlocal"
            | "not"
            | "or"
            | "pass"
            | "raise"
            | "return"
            | "try"
            | "while"
            | "with"
            | "yield"
    )
}

fn is_reserved_go(value: &str) -> bool {
    matches!(
        value,
        "break"
            | "default"
            | "func"
            | "interface"
            | "select"
            | "case"
            | "defer"
            | "go"
            | "map"
            | "struct"
            | "chan"
            | "else"
            | "goto"
            | "package"
            | "switch"
            | "const"
            | "fallthrough"
            | "if"
            | "range"
            | "type"
            | "continue"
            | "for"
            | "import"
            | "return"
            | "var"
    )
}

fn is_reserved_java(value: &str) -> bool {
    matches!(
        value,
        "abstract"
            | "assert"
            | "boolean"
            | "break"
            | "byte"
            | "case"
            | "catch"
            | "char"
            | "class"
            | "const"
            | "continue"
            | "default"
            | "do"
            | "double"
            | "else"
            | "enum"
            | "extends"
            | "final"
            | "finally"
            | "float"
            | "for"
            | "goto"
            | "if"
            | "implements"
            | "import"
            | "instanceof"
            | "int"
            | "interface"
            | "long"
            | "native"
            | "new"
            | "package"
            | "private"
            | "protected"
            | "public"
            | "return"
            | "short"
            | "static"
            | "strictfp"
            | "super"
            | "switch"
            | "synchronized"
            | "this"
            | "throw"
            | "throws"
            | "transient"
            | "try"
            | "void"
            | "volatile"
            | "while"
    )
}

fn is_reserved_kotlin(value: &str) -> bool {
    matches!(
        value,
        "as"
            | "break"
            | "class"
            | "continue"
            | "do"
            | "else"
            | "false"
            | "for"
            | "fun"
            | "if"
            | "in"
            | "interface"
            | "is"
            | "null"
            | "object"
            | "package"
            | "return"
            | "super"
            | "this"
            | "throw"
            | "true"
            | "try"
            | "typealias"
            | "val"
            | "var"
            | "when"
            | "while"
    )
}

fn is_reserved_swift(value: &str) -> bool {
    matches!(
        value,
        "class"
            | "deinit"
            | "enum"
            | "extension"
            | "func"
            | "import"
            | "init"
            | "let"
            | "protocol"
            | "static"
            | "struct"
            | "subscript"
            | "typealias"
            | "var"
            | "break"
            | "case"
            | "continue"
            | "default"
            | "defer"
            | "do"
            | "else"
            | "fallthrough"
            | "for"
            | "guard"
            | "if"
            | "in"
            | "repeat"
            | "return"
            | "switch"
            | "where"
            | "while"
            | "as"
            | "Any"
            | "catch"
            | "false"
            | "is"
            | "nil"
            | "rethrows"
            | "super"
            | "self"
            | "Self"
            | "throw"
            | "throws"
            | "true"
            | "try"
    )
}

fn render_rust(schema: &SchemaNode, name: &str) -> Result<String, DtoError> {
    let mut registry = NameRegistry::new(name);
    let mut defs = Vec::new();
    collect_types(schema, Vec::new(), &mut registry, &mut defs);

    let mut out = String::new();
    out.push_str("use serde::{Deserialize, Serialize};\n");
    if node_uses_json(schema) {
        out.push_str("use serde_json::Value;\n");
    }
    out.push('\n');

    for def in defs {
        out.push_str("#[derive(Debug, Clone, Serialize, Deserialize)]\n");
        out.push_str(&format!("pub struct {} {{\n", def.name));

        let mut used = HashMap::new();
        for field in &def.node.fields {
            let ident = field_identifier(DtoLanguage::Rust, &field.key, &mut used);
            let rename = ident != field.key;
            let optional = match &field.field_type {
                FieldType::Object(child) => !node_has_required(child),
                _ => field.optional,
            };
            let field_type = rust_type_for_field(field, &def.path, &registry);

            let mut attrs = Vec::new();
            if optional {
                attrs.push("default".to_string());
                attrs.push("skip_serializing_if = \"Option::is_none\"".to_string());
            }
            if rename {
                attrs.push(format!("rename = \"{}\"", field.key));
            }

            if !attrs.is_empty() {
                out.push_str(&format!("    #[serde({})]\n", attrs.join(", ")));
            }

            let final_type = if optional {
                format!("Option<{}>", field_type)
            } else {
                field_type
            };
            out.push_str(&format!("    pub {}: {},\n", ident, final_type));
        }

        out.push_str("}\n\n");
    }

    Ok(out.trim_end().to_string())
}

fn rust_type_for_field(field: &Field, parent_path: &[String], registry: &NameRegistry) -> String {
    match &field.field_type {
        FieldType::Primitive(PrimitiveType::String) => "String".to_string(),
        FieldType::Primitive(PrimitiveType::Int) => "i64".to_string(),
        FieldType::Primitive(PrimitiveType::Float) => "f64".to_string(),
        FieldType::Primitive(PrimitiveType::Bool) => "bool".to_string(),
        FieldType::JsonValue => "Value".to_string(),
        FieldType::Object(_) => {
            let mut path = parent_path.to_vec();
            path.push(field.key.clone());
            registry
                .get(&path)
                .cloned()
                .unwrap_or_else(|| "Record".to_string())
        }
    }
}

fn render_typescript(schema: &SchemaNode, name: &str) -> Result<String, DtoError> {
    let mut registry = NameRegistry::new(name);
    let mut defs = Vec::new();
    collect_types(schema, Vec::new(), &mut registry, &mut defs);

    let mut out = String::new();
    for def in defs {
        out.push_str(&format!("export interface {} {{\n", def.name));
        let mut used = HashMap::new();
        for field in &def.node.fields {
            let ident = field_identifier(DtoLanguage::TypeScript, &field.key, &mut used);
            let rename = ident != field.key;
            let optional = match &field.field_type {
                FieldType::Object(child) => !node_has_required(child),
                _ => field.optional,
            };
            let field_type = typescript_type_for_field(field, &def.path, &registry);
            if rename {
                out.push_str(&format!("  /** json: \"{}\" */\n", field.key));
            }
            let suffix = if optional { "?" } else { "" };
            out.push_str(&format!("  {}{}: {};\n", ident, suffix, field_type));
        }
        out.push_str("}\n\n");
    }

    Ok(out.trim_end().to_string())
}

fn typescript_type_for_field(
    field: &Field,
    parent_path: &[String],
    registry: &NameRegistry,
) -> String {
    match &field.field_type {
        FieldType::Primitive(PrimitiveType::String) => "string".to_string(),
        FieldType::Primitive(PrimitiveType::Int) => "number".to_string(),
        FieldType::Primitive(PrimitiveType::Float) => "number".to_string(),
        FieldType::Primitive(PrimitiveType::Bool) => "boolean".to_string(),
        FieldType::JsonValue => "unknown".to_string(),
        FieldType::Object(_) => {
            let mut path = parent_path.to_vec();
            path.push(field.key.clone());
            registry
                .get(&path)
                .cloned()
                .unwrap_or_else(|| "Record".to_string())
        }
    }
}

fn render_python(schema: &SchemaNode, name: &str) -> Result<String, DtoError> {
    let mut registry = NameRegistry::new(name);
    let mut defs = Vec::new();
    collect_types(schema, Vec::new(), &mut registry, &mut defs);

    let uses_json = node_uses_json(schema);
    let uses_optional = schema_has_optional(schema);
    let uses_rename = schema_has_rename(schema, DtoLanguage::Python);

    let mut out = String::new();
    out.push_str("from dataclasses import dataclass");
    if uses_rename {
        out.push_str(", field");
    }
    out.push('\n');

    if uses_json || uses_optional {
        let mut parts = Vec::new();
        if uses_optional {
            parts.push("Optional");
        }
        if uses_json {
            parts.push("Any");
        }
        out.push_str(&format!("from typing import {}\n", parts.join(", ")));
    }
    out.push('\n');

    for def in defs {
        out.push_str("@dataclass\n");
        out.push_str(&format!("class {}:\n", def.name));
        if def.node.fields.is_empty() {
            out.push_str("    pass\n\n");
            continue;
        }

        struct RenderField {
            key: String,
            ident: String,
            field_type: String,
            optional: bool,
            rename: bool,
        }

        let mut used = HashMap::new();
        let mut fields = Vec::new();
        for field in &def.node.fields {
            let ident = field_identifier(DtoLanguage::Python, &field.key, &mut used);
            let rename = ident != field.key;
            let optional = match &field.field_type {
                FieldType::Object(child) => !node_has_required(child),
                _ => field.optional,
            };
            let field_type = python_type_for_field(field, &def.path, &registry, optional);
            fields.push(RenderField {
                key: field.key.clone(),
                ident,
                field_type,
                optional,
                rename,
            });
        }

        for field in fields
            .iter()
            .filter(|field| !field.optional)
            .chain(fields.iter().filter(|field| field.optional))
        {
            if field.rename {
                out.push_str(&format!("    # json: \"{}\"\n", field.key));
            }

            if field.rename {
                if field.optional {
                    out.push_str(&format!(
                        "    {}: {} = field(default=None, metadata={{\"json_key\": \"{}\"}})\n",
                        field.ident, field.field_type, field.key
                    ));
                } else {
                    out.push_str(&format!(
                        "    {}: {} = field(metadata={{\"json_key\": \"{}\"}})\n",
                        field.ident, field.field_type, field.key
                    ));
                }
            } else if field.optional {
                out.push_str(&format!("    {}: {} = None\n", field.ident, field.field_type));
            } else {
                out.push_str(&format!("    {}: {}\n", field.ident, field.field_type));
            }
        }
        out.push('\n');
    }

    Ok(out.trim_end().to_string())
}

fn python_type_for_field(
    field: &Field,
    parent_path: &[String],
    registry: &NameRegistry,
    optional: bool,
) -> String {
    let base = match &field.field_type {
        FieldType::Primitive(PrimitiveType::String) => "str".to_string(),
        FieldType::Primitive(PrimitiveType::Int) => "int".to_string(),
        FieldType::Primitive(PrimitiveType::Float) => "float".to_string(),
        FieldType::Primitive(PrimitiveType::Bool) => "bool".to_string(),
        FieldType::JsonValue => "Any".to_string(),
        FieldType::Object(_) => {
            let mut path = parent_path.to_vec();
            path.push(field.key.clone());
            registry
                .get(&path)
                .cloned()
                .unwrap_or_else(|| "Record".to_string())
        }
    };

    if optional {
        format!("Optional[{}]", base)
    } else {
        base
    }
}

fn render_go(schema: &SchemaNode, name: &str) -> Result<String, DtoError> {
    let mut registry = NameRegistry::new(name);
    let mut defs = Vec::new();
    collect_types(schema, Vec::new(), &mut registry, &mut defs);

    let uses_json = node_uses_json(schema);

    let mut out = String::new();
    out.push_str("package dto\n\n");
    if uses_json {
        out.push_str("import \"encoding/json\"\n\n");
    }

    for def in defs {
        out.push_str(&format!("type {} struct {{\n", def.name));
        let mut used = HashMap::new();
        for field in &def.node.fields {
            let ident = field_identifier(DtoLanguage::Go, &field.key, &mut used);
            let optional = match &field.field_type {
                FieldType::Object(child) => !node_has_required(child),
                _ => field.optional,
            };
            let field_type = go_type_for_field(field, &def.path, &registry, optional);
            let tag = if optional {
                format!("`json:\"{},omitempty\"`", field.key)
            } else {
                format!("`json:\"{}\"`", field.key)
            };
            out.push_str(&format!("    {} {} {}\n", ident, field_type, tag));
        }
        out.push_str("}\n\n");
    }

    Ok(out.trim_end().to_string())
}

fn go_type_for_field(
    field: &Field,
    parent_path: &[String],
    registry: &NameRegistry,
    optional: bool,
) -> String {
    let base = match &field.field_type {
        FieldType::Primitive(PrimitiveType::String) => "string".to_string(),
        FieldType::Primitive(PrimitiveType::Int) => "int64".to_string(),
        FieldType::Primitive(PrimitiveType::Float) => "float64".to_string(),
        FieldType::Primitive(PrimitiveType::Bool) => "bool".to_string(),
        FieldType::JsonValue => "json.RawMessage".to_string(),
        FieldType::Object(_) => {
            let mut path = parent_path.to_vec();
            path.push(field.key.clone());
            registry
                .get(&path)
                .cloned()
                .unwrap_or_else(|| "Record".to_string())
        }
    };

    if optional {
        format!("*{}", base)
    } else {
        base
    }
}

fn render_java(schema: &SchemaNode, name: &str) -> Result<String, DtoError> {
    let mut registry = NameRegistry::new(name);
    let mut defs = Vec::new();
    collect_types(schema, Vec::new(), &mut registry, &mut defs);

    let uses_json = node_uses_json(schema);
    let uses_optional = schema_has_optional(schema);
    let uses_rename = schema_has_rename(schema, DtoLanguage::Java);

    let mut out = String::new();
    if uses_rename {
        out.push_str("import com.fasterxml.jackson.annotation.JsonProperty;\n");
    }
    if uses_json {
        out.push_str("import com.fasterxml.jackson.databind.JsonNode;\n");
    }
    if uses_optional {
        out.push_str("import java.util.Optional;\n");
    }
    if uses_rename || uses_json || uses_optional {
        out.push('\n');
    }

    for def in defs {
        let visibility = if def.path.is_empty() { "public " } else { "" };
        out.push_str(&format!("{}class {} {{\n", visibility, def.name));
        let mut used = HashMap::new();
        for field in &def.node.fields {
            let ident = field_identifier(DtoLanguage::Java, &field.key, &mut used);
            let rename = ident != field.key;
            let optional = match &field.field_type {
                FieldType::Object(child) => !node_has_required(child),
                _ => field.optional,
            };
            let field_type = java_type_for_field(field, &def.path, &registry, optional);

            if rename {
                out.push_str(&format!("    @JsonProperty(\"{}\")\n", field.key));
            }
            out.push_str(&format!("    public {} {};\n", field_type, ident));
        }
        out.push_str("}\n\n");
    }

    Ok(out.trim_end().to_string())
}

fn java_type_for_field(
    field: &Field,
    parent_path: &[String],
    registry: &NameRegistry,
    optional: bool,
) -> String {
    let base = match &field.field_type {
        FieldType::Primitive(PrimitiveType::String) => "String".to_string(),
        FieldType::Primitive(PrimitiveType::Int) => "Long".to_string(),
        FieldType::Primitive(PrimitiveType::Float) => "Double".to_string(),
        FieldType::Primitive(PrimitiveType::Bool) => "Boolean".to_string(),
        FieldType::JsonValue => "JsonNode".to_string(),
        FieldType::Object(_) => {
            let mut path = parent_path.to_vec();
            path.push(field.key.clone());
            registry
                .get(&path)
                .cloned()
                .unwrap_or_else(|| "Record".to_string())
        }
    };

    if optional {
        format!("Optional<{}>", base)
    } else {
        base
    }
}

fn render_kotlin(schema: &SchemaNode, name: &str) -> Result<String, DtoError> {
    let mut registry = NameRegistry::new(name);
    let mut defs = Vec::new();
    collect_types(schema, Vec::new(), &mut registry, &mut defs);

    let uses_json = node_uses_json(schema);
    let uses_rename = schema_has_rename(schema, DtoLanguage::Kotlin);

    let mut out = String::new();
    if uses_rename {
        out.push_str("import com.fasterxml.jackson.annotation.JsonProperty\n");
    }
    if uses_json {
        out.push_str("import com.fasterxml.jackson.databind.JsonNode\n");
    }
    if uses_rename || uses_json {
        out.push('\n');
    }

    for def in defs {
        out.push_str(&format!("data class {}(\n", def.name));
        let mut used = HashMap::new();
        for (index, field) in def.node.fields.iter().enumerate() {
            let ident = field_identifier(DtoLanguage::Kotlin, &field.key, &mut used);
            let rename = ident != field.key;
            let optional = match &field.field_type {
                FieldType::Object(child) => !node_has_required(child),
                _ => field.optional,
            };
            let field_type = kotlin_type_for_field(field, &def.path, &registry, optional);

            if rename {
                out.push_str(&format!("    @JsonProperty(\"{}\")\n", field.key));
            }
            let suffix = if index + 1 == def.node.fields.len() {
                ""
            } else {
                ","
            };
            out.push_str(&format!("    val {}: {}{}\n", ident, field_type, suffix));
        }
        out.push_str(")\n\n");
    }

    Ok(out.trim_end().to_string())
}

fn kotlin_type_for_field(
    field: &Field,
    parent_path: &[String],
    registry: &NameRegistry,
    optional: bool,
) -> String {
    let base = match &field.field_type {
        FieldType::Primitive(PrimitiveType::String) => "String".to_string(),
        FieldType::Primitive(PrimitiveType::Int) => "Long".to_string(),
        FieldType::Primitive(PrimitiveType::Float) => "Double".to_string(),
        FieldType::Primitive(PrimitiveType::Bool) => "Boolean".to_string(),
        FieldType::JsonValue => "JsonNode".to_string(),
        FieldType::Object(_) => {
            let mut path = parent_path.to_vec();
            path.push(field.key.clone());
            registry
                .get(&path)
                .cloned()
                .unwrap_or_else(|| "Record".to_string())
        }
    };

    if optional {
        format!("{}?", base)
    } else {
        base
    }
}

fn render_swift(schema: &SchemaNode, name: &str) -> Result<String, DtoError> {
    let mut registry = NameRegistry::new(name);
    let mut defs = Vec::new();
    collect_types(schema, Vec::new(), &mut registry, &mut defs);

    let uses_json = node_uses_json(schema);

    let mut out = String::new();
    for def in defs {
        out.push_str(&format!("struct {}: Codable {{\n", def.name));
        let mut used = HashMap::new();
        let mut coding_keys = Vec::new();
        for field in &def.node.fields {
            let ident = field_identifier(DtoLanguage::Swift, &field.key, &mut used);
            let rename = ident != field.key;
            let optional = match &field.field_type {
                FieldType::Object(child) => !node_has_required(child),
                _ => field.optional,
            };
            let field_type = swift_type_for_field(field, &def.path, &registry, optional);

            out.push_str(&format!("    let {}: {}\n", ident, field_type));
            if rename {
                coding_keys.push(format!("        case {} = \"{}\"", ident, field.key));
            }
        }

        if !coding_keys.is_empty() {
            out.push('\n');
            out.push_str("    enum CodingKeys: String, CodingKey {\n");
            for line in coding_keys {
                out.push_str(&format!("{}\n", line));
            }
            out.push_str("    }\n");
        }
        out.push_str("}\n\n");
    }

    if uses_json {
        out.push_str(SWIFT_JSON_VALUE);
        out.push('\n');
    }

    Ok(out.trim_end().to_string())
}

fn swift_type_for_field(
    field: &Field,
    parent_path: &[String],
    registry: &NameRegistry,
    optional: bool,
) -> String {
    let base = match &field.field_type {
        FieldType::Primitive(PrimitiveType::String) => "String".to_string(),
        FieldType::Primitive(PrimitiveType::Int) => "Int".to_string(),
        FieldType::Primitive(PrimitiveType::Float) => "Double".to_string(),
        FieldType::Primitive(PrimitiveType::Bool) => "Bool".to_string(),
        FieldType::JsonValue => "JSONValue".to_string(),
        FieldType::Object(_) => {
            let mut path = parent_path.to_vec();
            path.push(field.key.clone());
            registry
                .get(&path)
                .cloned()
                .unwrap_or_else(|| "Record".to_string())
        }
    };

    if optional {
        format!("{}?", base)
    } else {
        base
    }
}

fn schema_has_optional(node: &SchemaNode) -> bool {
    for field in &node.fields {
        match &field.field_type {
            FieldType::Object(child) => {
                if !node_has_required(child) || schema_has_optional(child) {
                    return true;
                }
            }
            _ => {
                if field.optional {
                    return true;
                }
            }
        }
    }
    false
}

fn schema_has_rename(node: &SchemaNode, lang: DtoLanguage) -> bool {
    let mut used = HashMap::new();
    for field in &node.fields {
        let ident = field_identifier(lang, &field.key, &mut used);
        if ident != field.key {
            return true;
        }
        if let FieldType::Object(child) = &field.field_type {
            if schema_has_rename(child, lang) {
                return true;
            }
        }
    }
    false
}

const SWIFT_JSON_VALUE: &str = "enum JSONValue: Codable {\n    case string(String)\n    case number(Double)\n    case bool(Bool)\n    case object([String: JSONValue])\n    case array([JSONValue])\n    case null\n\n    init(from decoder: Decoder) throws {\n        let container = try decoder.singleValueContainer()\n        if container.decodeNil() {\n            self = .null\n        } else if let value = try? container.decode(Bool.self) {\n            self = .bool(value)\n        } else if let value = try? container.decode(Double.self) {\n            self = .number(value)\n        } else if let value = try? container.decode(String.self) {\n            self = .string(value)\n        } else if let value = try? container.decode([String: JSONValue].self) {\n            self = .object(value)\n        } else if let value = try? container.decode([JSONValue].self) {\n            self = .array(value)\n        } else {\n            throw DecodingError.typeMismatch(JSONValue.self, DecodingError.Context(codingPath: decoder.codingPath, debugDescription: \"Unsupported JSON value\"))\n        }\n    }\n\n    func encode(to encoder: Encoder) throws {\n        var container = encoder.singleValueContainer()\n        switch self {\n        case .string(let value):\n            try container.encode(value)\n        case .number(let value):\n            try container.encode(value)\n        case .bool(let value):\n            try container.encode(value)\n        case .object(let value):\n            try container.encode(value)\n        case .array(let value):\n            try container.encode(value)\n        case .null:\n            try container.encodeNil()\n        }\n    }\n}\n";
