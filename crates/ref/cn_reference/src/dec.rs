//! Exact decimals with Python `decimal.Decimal` semantics for the operations
//! the reference service uses: parse, `normalize()`, `format(d, "f")` and
//! multiplication (coefficients and exponents are kept as Python keeps them).

#[derive(Clone, Debug, PartialEq)]
pub struct Dec {
    neg: bool,
    /// Decimal digits, no leading zeros ("0" for zero).
    coef: String,
    exp: i32,
}

fn trim_zeros(digits: &str) -> String {
    let t = digits.trim_start_matches('0');
    if t.is_empty() {
        "0".to_string()
    } else {
        t.to_string()
    }
}

impl Dec {
    /// `Decimal(text)` for finite values; `None` when Python would raise.
    /// Underscores between digits are accepted, as in Python.
    pub fn parse(text: &str) -> Option<Dec> {
        let t = text.trim();
        let (neg, rest) = match t.as_bytes().first()? {
            b'-' => (true, &t[1..]),
            b'+' => (false, &t[1..]),
            _ => (false, t),
        };
        let (mant, exp_part) = match rest.find(['e', 'E']) {
            Some(i) => (&rest[..i], Some(&rest[i + 1..])),
            None => (rest, None),
        };
        let valid_digits = |s: &str| {
            !s.is_empty()
                && !s.starts_with('_')
                && !s.ends_with('_')
                && !s.contains("__")
                && s.bytes().all(|b| b.is_ascii_digit() || b == b'_')
        };
        let (int_part, frac_part) = match mant.split_once('.') {
            Some((i, f)) => (i, f),
            None => (mant, ""),
        };
        if int_part.is_empty() && frac_part.is_empty() {
            return None;
        }
        if !int_part.is_empty() && !valid_digits(int_part) {
            return None;
        }
        if !frac_part.is_empty() && !valid_digits(frac_part) {
            return None;
        }
        let mut exp: i32 = 0;
        if let Some(e) = exp_part {
            let (eneg, edigits) = match e.as_bytes().first()? {
                b'-' => (true, &e[1..]),
                b'+' => (false, &e[1..]),
                _ => (false, e),
            };
            if !valid_digits(edigits) {
                return None;
            }
            let v: i32 = edigits.replace('_', "").parse().ok()?;
            exp = if eneg { -v } else { v };
        }
        let digits: String = format!("{int_part}{frac_part}").replace('_', "");
        exp -= frac_part.replace('_', "").len() as i32;
        Some(Dec {
            neg,
            coef: trim_zeros(&digits),
            exp,
        })
    }

    pub fn is_zero(&self) -> bool {
        self.coef == "0"
    }

    /// Divides by 100 exactly (the value of `Decimal(x) / Decimal("100")`).
    pub fn div100(mut self) -> Dec {
        self.exp -= 2;
        self
    }

    /// `Decimal.normalize()`: trailing zeros removed (zero becomes `0`).
    pub fn normalize(mut self) -> Dec {
        if self.is_zero() {
            self.exp = 0;
            return self;
        }
        while self.coef.ends_with('0') {
            self.coef.pop();
            self.exp += 1;
        }
        self
    }

    /// `Decimal * Decimal` (exact; Python rounds to 28 digits, which these
    /// inputs never reach).
    pub fn mul(&self, other: &Dec) -> Dec {
        let a: Vec<u32> = self.coef.bytes().rev().map(|b| (b - b'0') as u32).collect();
        let b: Vec<u32> = other
            .coef
            .bytes()
            .rev()
            .map(|b| (b - b'0') as u32)
            .collect();
        let mut r = vec![0u32; a.len() + b.len() + 1];
        for (i, x) in a.iter().enumerate() {
            for (j, y) in b.iter().enumerate() {
                r[i + j] += x * y;
            }
        }
        let mut carry = 0;
        for d in r.iter_mut() {
            let v = *d + carry;
            *d = v % 10;
            carry = v / 10;
        }
        let digits: String = r
            .iter()
            .rev()
            .map(|d| char::from(b'0' + *d as u8))
            .collect();
        Dec {
            neg: self.neg != other.neg,
            coef: trim_zeros(&digits),
            exp: self.exp + other.exp,
        }
    }

    /// `format(d, "f")`.
    pub fn plain(&self) -> String {
        let sign = if self.neg { "-" } else { "" };
        if self.exp >= 0 {
            let zeros = if self.is_zero() { 0 } else { self.exp as usize };
            return format!("{sign}{}{}", self.coef, "0".repeat(zeros));
        }
        let frac = (-self.exp) as usize;
        let mut digits = self.coef.clone();
        if digits.len() <= frac {
            digits = format!("{}{digits}", "0".repeat(frac - digits.len() + 1));
        }
        let split = digits.len() - frac;
        format!("{sign}{}.{}", &digits[..split], &digits[split..])
    }
}

/// `models.canonical_decimal`.
pub fn canonical_decimal(value: Option<&str>) -> Option<String> {
    let text = value?.trim().replace(',', "");
    if matches!(
        text.to_lowercase().as_str(),
        "" | "-" | "--" | "none" | "null" | "nan" | "n/a"
    ) {
        return None;
    }
    let (body, percent) = match text.strip_suffix('%') {
        Some(b) => (b.trim(), true),
        None => (text.as_str(), false),
    };
    let mut d = Dec::parse(body)?;
    if percent {
        d = d.div100();
    }
    if d.is_zero() {
        return Some("0".to_string());
    }
    Some(d.normalize().plain())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn c(s: &str) -> Option<String> {
        canonical_decimal(Some(s))
    }

    #[test]
    fn canonical_matches_python_examples() {
        assert_eq!(c("10.0"), Some("10".into()));
        assert_eq!(c("0.000001"), Some("0.000001".into()));
        assert_eq!(c("1,234.500"), Some("1234.5".into()));
        assert_eq!(c("5%"), Some("0.05".into()));
        assert_eq!(c("0.00"), Some("0".into()));
        assert_eq!(c("1e3"), Some("1000".into()));
        assert_eq!(c("100"), Some("100".into()));
        assert_eq!(c("-0.50"), Some("-0.5".into()));
        assert_eq!(c("1_000"), Some("1000".into()));
        assert_eq!(c("n/a"), None);
        assert_eq!(c("abc"), None);
        assert_eq!(c(""), None);
    }

    #[test]
    fn product_keeps_python_exponent() {
        let m = |a: &str, b: &str| Dec::parse(a).unwrap().mul(&Dec::parse(b).unwrap()).plain();
        assert_eq!(m("10", "1"), "10");
        assert_eq!(m("0.5", "10"), "5.0");
        assert_eq!(m("0.01", "10"), "0.10");
        assert_eq!(m("2", "5"), "10");
        assert_eq!(m("0.2", "0.3"), "0.06");
    }
}
