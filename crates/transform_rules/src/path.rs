use serde_json::Value as JsonValue;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PathToken {
    Key(String),
    Index(usize),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathError {
    Empty,
    InvalidSyntax,
    InvalidEscape,
    EmptyKey,
}

impl PathError {
    pub fn message(&self) -> &'static str {
        match self {
            PathError::Empty => "path is empty",
            PathError::InvalidSyntax => "path syntax is invalid",
            PathError::InvalidEscape => "path escape is invalid",
            PathError::EmptyKey => "path segment is empty",
        }
    }
}

pub fn parse_path(path: &str) -> Result<Vec<PathToken>, PathError> {
    if path.is_empty() {
        return Err(PathError::Empty);
    }

    let chars: Vec<char> = path.chars().collect();
    let mut tokens = Vec::new();
    let mut index = 0;

    while index < chars.len() {
        if chars[index] == '.' {
            return Err(PathError::EmptyKey);
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
                return Err(PathError::EmptyKey);
            }
            let key: String = chars[start..index].iter().collect();
            if key.is_empty() {
                return Err(PathError::EmptyKey);
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
                    return Err(PathError::InvalidSyntax);
                }
            } else {
                return Err(PathError::InvalidSyntax);
            }
        }
    }

    Ok(tokens)
}

fn parse_bracket(chars: &[char], start: usize) -> Result<(PathToken, usize), PathError> {
    if chars.get(start) != Some(&'[') {
        return Err(PathError::InvalidSyntax);
    }
    let index = start + 1;
    if index >= chars.len() {
        return Err(PathError::InvalidSyntax);
    }

    match chars[index] {
        '"' | '\'' => parse_quoted(chars, index),
        c if c.is_ascii_digit() => parse_index(chars, index),
        _ => Err(PathError::InvalidSyntax),
    }
}

fn parse_index(chars: &[char], start: usize) -> Result<(PathToken, usize), PathError> {
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
        return Err(PathError::InvalidSyntax);
    }
    if chars.get(index) != Some(&']') {
        return Err(PathError::InvalidSyntax);
    }
    index += 1;
    Ok((PathToken::Index(value), index))
}

fn parse_quoted(chars: &[char], start: usize) -> Result<(PathToken, usize), PathError> {
    let quote = chars[start];
    let mut index = start + 1;
    let mut value = String::new();

    while index < chars.len() {
        let ch = chars[index];
        if ch == '\\' {
            index += 1;
            if index >= chars.len() {
                return Err(PathError::InvalidEscape);
            }
            let escaped = chars[index];
            if escaped == '\\' || escaped == quote {
                value.push(escaped);
                index += 1;
                continue;
            }
            return Err(PathError::InvalidEscape);
        }

        if ch == '[' || ch == ']' {
            return Err(PathError::InvalidSyntax);
        }

        if ch == quote {
            index += 1;
            break;
        }

        value.push(ch);
        index += 1;
    }

    if value.is_empty() {
        return Err(PathError::EmptyKey);
    }
    if chars.get(index - 1) != Some(&quote) {
        return Err(PathError::InvalidSyntax);
    }
    if chars.get(index) != Some(&']') {
        return Err(PathError::InvalidSyntax);
    }
    index += 1;
    Ok((PathToken::Key(value), index))
}

pub fn get_path<'a>(value: &'a JsonValue, tokens: &[PathToken]) -> Option<&'a JsonValue> {
    let mut current = value;
    for token in tokens {
        match token {
            PathToken::Key(key) => match current {
                JsonValue::Object(map) => current = map.get(key)?,
                _ => return None,
            },
            PathToken::Index(index) => match current {
                JsonValue::Array(items) => current = items.get(*index)?,
                _ => return None,
            },
        }
    }
    Some(current)
}
