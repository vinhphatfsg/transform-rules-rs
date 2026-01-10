use std::collections::HashMap;

use crate::error::YamlLocation;

#[derive(Debug, Default)]
pub struct YamlLocator {
    locations: HashMap<String, YamlLocation>,
}

impl YamlLocator {
    pub fn from_str(source: &str) -> Self {
        let mut locator = YamlLocator {
            locations: HashMap::new(),
        };
        locator.build(source);
        locator
    }

    pub fn location_for(&self, path: &str) -> Option<YamlLocation> {
        self.locations.get(path).cloned()
    }

    fn build(&mut self, source: &str) {
        let mut scopes: Vec<Scope> = vec![Scope {
            indent: 0,
            path: String::new(),
        }];
        let mut seq_indices: HashMap<String, usize> = HashMap::new();

        for (line_index, raw_line) in source.lines().enumerate() {
            let line_number = line_index + 1;
            let trimmed = raw_line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }

            let indent = raw_line.chars().take_while(|c| *c == ' ').count();
            let content = &raw_line[indent..];

            if content.starts_with('-') {
                while scopes.len() > 1 && scopes.last().unwrap().indent >= indent {
                    scopes.pop();
                }
                let parent_path = scopes.last().map(|s| s.path.clone()).unwrap_or_default();
                if parent_path.is_empty() {
                    continue;
                }

                let index = seq_indices.entry(parent_path.clone()).or_insert(0);
                let item_index = *index;
                *index += 1;

                let item_path = format!("{}[{}]", parent_path, item_index);
                self.insert_location(&item_path, line_number, indent + 1);

                scopes.push(Scope {
                    indent,
                    path: item_path.clone(),
                });

                let after_dash = &content[1..];
                let trimmed_after_dash = after_dash.trim_start();
                let offset = 1 + (after_dash.len() - trimmed_after_dash.len());
                if let Some((key, column, has_value, is_block)) =
                    parse_key_at(trimmed_after_dash, indent, offset)
                {
                    let full_path = format!("{}.{}", item_path, key);
                    self.insert_location(&full_path, line_number, column);
                    if !has_value || is_block {
                        scopes.push(Scope {
                            indent: indent + offset,
                            path: full_path,
                        });
                    }
                }
                continue;
            }

            while scopes.len() > 1 && scopes.last().unwrap().indent >= indent {
                scopes.pop();
            }

            if let Some((key, column, has_value, is_block)) = parse_key_at(content, indent, 0) {
                let parent_path = scopes.last().map(|s| s.path.clone()).unwrap_or_default();
                let full_path = if parent_path.is_empty() {
                    key
                } else {
                    format!("{}.{}", parent_path, key)
                };
                self.insert_location(&full_path, line_number, column);
                if !has_value || is_block {
                    scopes.push(Scope {
                        indent,
                        path: full_path,
                    });
                }
            }
        }
    }

    fn insert_location(&mut self, path: &str, line: usize, column: usize) {
        self.locations
            .entry(path.to_string())
            .or_insert(YamlLocation { line, column });
    }
}

#[derive(Debug, Clone)]
struct Scope {
    indent: usize,
    path: String,
}

fn parse_key_at(
    content: &str,
    indent: usize,
    offset: usize,
) -> Option<(String, usize, bool, bool)> {
    let (key, key_start, has_value, is_block) = parse_key(content)?;
    let column = indent + offset + key_start + 1;
    Some((key, column, has_value, is_block))
}

fn parse_key(content: &str) -> Option<(String, usize, bool, bool)> {
    let mut in_single = false;
    let mut in_double = false;
    let mut colon_index = None;

    for (index, ch) in content.char_indices() {
        match ch {
            '\'' if !in_double => in_single = !in_single,
            '"' if !in_single => in_double = !in_double,
            ':' if !in_single && !in_double => {
                colon_index = Some(index);
                break;
            }
            _ => {}
        }
    }

    let colon_index = colon_index?;
    let key_part = &content[..colon_index];
    let key = key_part.trim();
    if key.is_empty() {
        return None;
    }

    let key_start = key_part.find(|c: char| !c.is_whitespace())?;
    let rest = content[colon_index + 1..].trim();
    let has_value = !rest.is_empty();
    let is_block = rest.starts_with('|') || rest.starts_with('>');

    Some((key.to_string(), key_start, has_value, is_block))
}
