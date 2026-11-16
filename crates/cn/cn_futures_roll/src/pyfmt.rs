//! Byte-compatible re-implementations of the Python formatting the roll tables
//! depend on: `repr(float)`, `csv.DictWriter` (QUOTE_MINIMAL, `\r\n`) and
//! `json.dumps(..., indent=2, sort_keys=True)` string escaping.

use std::io::{self, Write};

/// `repr(x)` / `str(x)` for a Python `float`.
///
/// Python prints the shortest round-trip digits, switching to exponent form
/// when the decimal point position `decpt` is `<= -4` or `> 16`, and always
/// keeps a `.0` on integral fixed-point values.
pub fn py_float(value: f64) -> String {
    if value.is_nan() {
        return "nan".to_owned();
    }
    if value.is_infinite() {
        return if value > 0.0 { "inf" } else { "-inf" }.to_owned();
    }
    // Rust's `{:e}` emits the shortest round-trip digits: `-1.2345e3`.
    let exp_form = format!("{value:e}");
    let (negative, body) = match exp_form.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, exp_form.as_str()),
    };
    let (mantissa, exponent) = body.split_once('e').expect("LowerExp has an exponent");
    let exponent: i32 = exponent.parse().expect("LowerExp exponent is an integer");
    let digits: String = mantissa.chars().filter(|c| *c != '.').collect();
    let ndigits = digits.len() as i32;
    let decpt = exponent + 1;

    let mut out = String::with_capacity(24);
    if negative {
        out.push('-');
    }
    if decpt <= -4 || decpt > 16 {
        out.push_str(&digits[..1]);
        if ndigits > 1 {
            out.push('.');
            out.push_str(&digits[1..]);
        }
        let exp = decpt - 1;
        out.push('e');
        out.push(if exp < 0 { '-' } else { '+' });
        out.push_str(&format!("{:02}", exp.abs()));
    } else if decpt <= 0 {
        out.push_str("0.");
        for _ in 0..(-decpt) {
            out.push('0');
        }
        out.push_str(&digits);
    } else if decpt >= ndigits {
        out.push_str(&digits);
        for _ in 0..(decpt - ndigits) {
            out.push('0');
        }
        out.push_str(".0");
    } else {
        let split = decpt as usize;
        out.push_str(&digits[..split]);
        out.push('.');
        out.push_str(&digits[split..]);
    }
    out
}

/// Python `csv` writes `None` as an empty field and floats via `repr`.
pub fn py_opt_float(value: Option<f64>) -> String {
    value.map(py_float).unwrap_or_default()
}

/// `csv.writer` default dialect (`excel`): QUOTE_MINIMAL, `\r\n` terminator.
pub struct PyCsvWriter<W: Write> {
    inner: W,
}

impl<W: Write> PyCsvWriter<W> {
    pub fn new(inner: W) -> Self {
        Self { inner }
    }

    pub fn write_row<S: AsRef<str>>(&mut self, fields: &[S]) -> io::Result<()> {
        let single_empty = fields.len() == 1 && fields[0].as_ref().is_empty();
        for (index, field) in fields.iter().enumerate() {
            if index > 0 {
                self.inner.write_all(b",")?;
            }
            let field = field.as_ref();
            let needs_quote = single_empty
                || field
                    .bytes()
                    .any(|b| matches!(b, b',' | b'"' | b'\r' | b'\n'));
            if needs_quote {
                self.inner.write_all(b"\"")?;
                self.inner
                    .write_all(field.replace('"', "\"\"").as_bytes())?;
                self.inner.write_all(b"\"")?;
            } else {
                self.inner.write_all(field.as_bytes())?;
            }
        }
        self.inner.write_all(b"\r\n")
    }

    pub fn into_inner(self) -> W {
        self.inner
    }
}

/// `json.dumps(str)` with the default `ensure_ascii=True`.
pub fn py_json_str(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0c}' => out.push_str("\\f"),
            c if (c as u32) < 0x20 || (c as u32) > 0x7e => {
                let mut units = [0u16; 2];
                for unit in c.encode_utf16(&mut units) {
                    out.push_str(&format!("\\u{unit:04x}"));
                }
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn float_repr_matches_python() {
        let cases: &[(f64, &str)] = &[
            (3877.0, "3877.0"),
            (11245.0, "11245.0"),
            (0.0, "0.0"),
            (-0.0, "-0.0"),
            (-4268.0, "-4268.0"),
            (0.1, "0.1"),
            (0.1 + 0.2, "0.30000000000000004"),
            (1234.5, "1234.5"),
            (-0.5, "-0.5"),
            (0.0001, "0.0001"),
            (0.00001, "1e-05"),
            (0.000012345, "1.2345e-05"),
            (1e16, "1e+16"),
            (1234567890123456.0, "1234567890123456.0"),
            (12345678901234567.0, "1.2345678901234568e+16"),
            (1e22, "1e+22"),
            (1.5e300, "1.5e+300"),
            (5e-324, "5e-324"),
            (f64::MAX, "1.7976931348623157e+308"),
            (f64::INFINITY, "inf"),
            (f64::NEG_INFINITY, "-inf"),
            (f64::NAN, "nan"),
            (3600.0 - 3590.2, "9.800000000000182"),
            (115163.0 * 1.1, "126679.30000000002"),
        ];
        for (value, expected) in cases {
            assert_eq!(py_float(*value), *expected, "value {value:?}");
        }
        assert_eq!(py_opt_float(None), "");
    }

    #[test]
    fn csv_quotes_minimally_with_crlf() {
        let mut writer = PyCsvWriter::new(Vec::new());
        writer.write_row(&["a", "", "b,c", "d\"e", "x\ny"]).unwrap();
        writer.write_row(&[""]).unwrap();
        assert_eq!(
            String::from_utf8(writer.into_inner()).unwrap(),
            "a,,\"b,c\",\"d\"\"e\",\"x\ny\"\r\n\"\"\r\n"
        );
    }

    #[test]
    fn json_string_is_ascii_escaped() {
        assert_eq!(py_json_str("AP"), "\"AP\"");
        assert_eq!(py_json_str("a\"b\\c\n"), "\"a\\\"b\\\\c\\n\"");
        assert_eq!(py_json_str("\u{1}"), "\"\\u0001\"");
        assert_eq!(py_json_str("主力"), "\"\\u4e3b\\u529b\"");
        assert_eq!(py_json_str("\u{1f600}"), "\"\\ud83d\\ude00\"");
        assert_eq!(py_json_str("\u{7f}"), "\"\\u007f\"");
    }
}
