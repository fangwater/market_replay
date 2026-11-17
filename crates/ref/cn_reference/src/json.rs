//! Insertion-ordered JSON value with Python `json.dumps(ensure_ascii=False)`
//! output, so responses can match the Python service byte for byte.
use serde::de::{Deserialize, Deserializer, MapAccess, SeqAccess, Visitor};
use std::fmt;

#[derive(Clone, Debug, PartialEq)]
pub enum J {
    Null,
    Bool(bool),
    Int(i64),
    /// A number kept as its source text (non-i64 numbers from jsonb).
    Num(String),
    Str(String),
    Arr(Vec<J>),
    Obj(Vec<(String, J)>),
}

impl J {
    pub fn s(v: impl Into<String>) -> J {
        J::Str(v.into())
    }
    pub fn opt(v: Option<String>) -> J {
        v.map_or(J::Null, J::Str)
    }
    pub fn obj(items: Vec<(&str, J)>) -> J {
        J::Obj(items.into_iter().map(|(k, v)| (k.to_string(), v)).collect())
    }

    pub fn get(&self, key: &str) -> Option<&J> {
        match self {
            J::Obj(items) => items.iter().find(|(k, _)| k == key).map(|(_, v)| v),
            _ => None,
        }
    }
    /// String value, `None` for null/missing/non-strings.
    pub fn str_of(&self, key: &str) -> Option<&str> {
        match self.get(key) {
            Some(J::Str(s)) => Some(s),
            _ => None,
        }
    }
    /// `dict[key] = value`: replace in place or append.
    pub fn set(&mut self, key: &str, value: J) {
        if let J::Obj(items) = self {
            match items.iter_mut().find(|(k, _)| k == key) {
                Some(slot) => slot.1 = value,
                None => items.push((key.to_string(), value)),
            }
        }
    }

    /// `separators=(",", ":")` when `spaced` is false, Python's defaults
    /// (`", "`, `": "`) when true.
    pub fn dumps(&self, spaced: bool) -> String {
        let mut out = String::new();
        self.write(&mut out, spaced);
        out
    }

    fn write(&self, out: &mut String, spaced: bool) {
        match self {
            J::Null => out.push_str("null"),
            J::Bool(true) => out.push_str("true"),
            J::Bool(false) => out.push_str("false"),
            J::Int(i) => out.push_str(&i.to_string()),
            J::Num(n) => out.push_str(n),
            J::Str(s) => escape(s, out),
            J::Arr(items) => {
                out.push('[');
                for (i, v) in items.iter().enumerate() {
                    if i > 0 {
                        out.push_str(if spaced { ", " } else { "," });
                    }
                    v.write(out, spaced);
                }
                out.push(']');
            }
            J::Obj(items) => {
                out.push('{');
                for (i, (k, v)) in items.iter().enumerate() {
                    if i > 0 {
                        out.push_str(if spaced { ", " } else { "," });
                    }
                    escape(k, out);
                    out.push_str(if spaced { ": " } else { ":" });
                    v.write(out, spaced);
                }
                out.push('}');
            }
        }
    }
}

fn escape(s: &str, out: &mut String) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
}

impl<'de> Deserialize<'de> for J {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = J;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("any JSON value")
            }
            fn visit_unit<E>(self) -> Result<J, E> {
                Ok(J::Null)
            }
            fn visit_none<E>(self) -> Result<J, E> {
                Ok(J::Null)
            }
            fn visit_bool<E>(self, v: bool) -> Result<J, E> {
                Ok(J::Bool(v))
            }
            fn visit_i64<E>(self, v: i64) -> Result<J, E> {
                Ok(J::Int(v))
            }
            fn visit_u64<E>(self, v: u64) -> Result<J, E> {
                Ok(i64::try_from(v).map_or_else(|_| J::Num(v.to_string()), J::Int))
            }
            fn visit_f64<E>(self, v: f64) -> Result<J, E> {
                Ok(J::Num(format!("{v:?}")))
            }
            fn visit_str<E>(self, v: &str) -> Result<J, E> {
                Ok(J::Str(v.to_string()))
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut a: A) -> Result<J, A::Error> {
                let mut v = Vec::new();
                while let Some(x) = a.next_element()? {
                    v.push(x);
                }
                Ok(J::Arr(v))
            }
            fn visit_map<A: MapAccess<'de>>(self, mut a: A) -> Result<J, A::Error> {
                let mut v = Vec::new();
                while let Some((k, x)) = a.next_entry::<String, J>()? {
                    v.push((k, x));
                }
                Ok(J::Obj(v))
            }
        }
        d.deserialize_any(V)
    }
}

/// Parses JSON text keeping object key order (PostgreSQL `jsonb::text`).
pub fn parse_ordered(text: &str) -> anyhow::Result<J> {
    Ok(serde_json::from_str::<J>(text)?)
}

/// Python `repr()` of a str, for error messages that echo user input.
pub fn py_repr(s: &str) -> String {
    let quote = if s.contains('\'') && !s.contains('"') {
        '"'
    } else {
        '\''
    };
    let mut out = String::new();
    out.push(quote);
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c == quote => {
                out.push('\\');
                out.push(c);
            }
            c if (c as u32) < 0x20 || c as u32 == 0x7f => {
                out.push_str(&format!("\\x{:02x}", c as u32))
            }
            c => out.push(c),
        }
    }
    out.push(quote);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dumps_like_python() {
        let v = J::obj(vec![
            ("b", J::Int(1)),
            (
                "a",
                J::Arr(vec![J::s("锡\n\"x\u{1}"), J::Null, J::Bool(true)]),
            ),
        ]);
        assert_eq!(
            v.dumps(false),
            "{\"b\":1,\"a\":[\"锡\\n\\\"x\\u0001\",null,true]}"
        );
        assert_eq!(
            v.dumps(true),
            "{\"b\": 1, \"a\": [\"锡\\n\\\"x\\u0001\", null, true]}"
        );
    }

    #[test]
    fn jsonb_text_keeps_order() {
        let j = parse_ordered("{\"Area\": \"cn\", \"TimeEnd\": \"01:00:00\", \"SegmentNo\": 1}")
            .unwrap();
        assert_eq!(
            j.dumps(false),
            "{\"Area\":\"cn\",\"TimeEnd\":\"01:00:00\",\"SegmentNo\":1}"
        );
    }

    #[test]
    fn repr_matches_python() {
        assert_eq!(py_repr("x"), "'x'");
        assert_eq!(py_repr("it's"), "\"it's\"");
        assert_eq!(py_repr("a'b\"c"), "'a\\'b\"c'");
    }
}
