//! Product index and query matching (code / contract / RIC / Chinese / English).
use serde::Serialize;
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, PartialOrd, Ord)]
#[serde(rename_all = "lowercase")]
pub enum Market {
    Cn,
    Lseg,
    Ib,
}

impl Market {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "cn" => Some(Self::Cn),
            "lseg" => Some(Self::Lseg),
            "ib" => Some(Self::Ib),
            _ => None,
        }
    }
    pub fn label(self) -> &'static str {
        match self {
            Self::Cn => "国内期货",
            Self::Lseg => "海外期货 LSEG",
            Self::Ib => "IB",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Product {
    pub market: Market,
    pub code: String,
    /// Exchange codes (several for LSEG products split across directories).
    pub exchanges: Vec<String>,
    pub exchange_cn: String,
    pub name_cn: Option<String>,
    /// Where the Chinese name came from (shown in the UI).
    pub name_cn_source: Option<String>,
    pub category: Option<String>,
    pub name_en: Option<String>,
    pub name_en_source: Option<String>,
    pub multiplier: Option<String>,
    pub tick: Option<String>,
    /// Market-specific availability summary (counts, ranges, dominant...).
    pub summary: serde_json::Value,
}

#[derive(Debug, Clone, Serialize)]
pub struct Hit {
    pub score: u32,
    /// Why it matched: 代码 / 合约 / 中文名 / 英文名 / 类别.
    pub matched: &'static str,
    /// Contract the query named (rb2701, ESZ26), if any.
    pub contract: Option<String>,
    pub product: Product,
}

#[derive(Default)]
pub struct Index {
    pub products: Vec<Product>,
    by_key: HashMap<(Market, String), usize>,
    /// upper-case contract code -> (product idx, original contract code)
    contracts: HashMap<String, Vec<(usize, String)>>,
}

impl Index {
    pub fn new(products: Vec<Product>) -> Self {
        let by_key = products
            .iter()
            .enumerate()
            .map(|(i, p)| ((p.market, p.code.clone()), i))
            .collect();
        Self {
            products,
            by_key,
            contracts: HashMap::new(),
        }
    }

    /// Registers an exact contract code (LSEG RIC, IB local symbol...).
    pub fn add_contract(&mut self, market: Market, product: &str, contract: &str) {
        if let Some(&i) = self.by_key.get(&(market, product.to_string())) {
            let e = self
                .contracts
                .entry(contract.to_ascii_uppercase())
                .or_default();
            if !e.iter().any(|(j, _)| *j == i) {
                e.push((i, contract.to_string()));
            }
        }
    }

    pub fn get(&self, market: Market, code: &str) -> Option<&Product> {
        self.by_key
            .get(&(market, code.to_string()))
            .map(|&i| &self.products[i])
    }

    pub fn search(&self, query: &str, per_market: usize) -> Vec<Hit> {
        let q = query.trim();
        if q.is_empty() {
            return Vec::new();
        }
        let qu = q.to_ascii_uppercase();
        let ql = q.to_lowercase();
        let ascii = q.is_ascii();
        let mut best: HashMap<usize, (u32, &'static str, Option<String>)> = HashMap::new();
        let mut offer = |i: usize, score: u32, why: &'static str, c: Option<String>| {
            let e = best.entry(i).or_insert((0, why, None));
            if score > e.0 {
                *e = (score, why, c);
            }
        };
        if let Some(list) = self.contracts.get(&qu) {
            for (i, c) in list {
                offer(*i, 95, "合约", Some(c.clone()));
            }
        }
        // CN contract pattern: letters + 3/4 digits (rb2701, AP601, IF2612).
        if let Some((root, digits)) = split_cn_contract(q) {
            if let Some(&i) = self.by_key.get(&(Market::Cn, root.to_ascii_uppercase())) {
                offer(
                    i,
                    95,
                    "合约",
                    Some(cn_contract_code(&self.products[i], &root, digits)),
                );
            }
        }
        for (i, p) in self.products.iter().enumerate() {
            if ascii {
                let code = p.code.to_ascii_uppercase();
                if code == qu {
                    offer(i, 100, "代码", None);
                } else if code.starts_with(&qu) {
                    let extra = (code.len() - qu.len()).min(20) as u32;
                    offer(i, 60 - extra, "代码", None);
                }
                if let Some(en) = &p.name_en {
                    if english_match(en, &ql) {
                        offer(i, 50, "英文名", None);
                    }
                }
            }
            if let Some(cn) = &p.name_cn {
                if cn.starts_with(q) {
                    offer(i, 88, "中文名", None);
                } else if cn.contains(q) {
                    offer(i, 80, "中文名", None);
                }
            }
            if !ascii {
                if let Some(cat) = &p.category {
                    if cat.contains(q) {
                        offer(i, 30, "类别", None);
                    }
                }
                if p.exchange_cn.contains(q) {
                    offer(i, 20, "交易所", None);
                }
            }
        }
        let mut hits: Vec<Hit> = best
            .into_iter()
            .map(|(i, (score, matched, contract))| Hit {
                score,
                matched,
                contract,
                product: self.products[i].clone(),
            })
            .collect();
        hits.sort_by(|a, b| {
            (
                a.product.market,
                std::cmp::Reverse(a.score),
                &a.product.code,
            )
                .cmp(&(
                    b.product.market,
                    std::cmp::Reverse(b.score),
                    &b.product.code,
                ))
        });
        let mut counts: HashMap<Market, usize> = HashMap::new();
        hits.retain(|h| {
            let n = counts.entry(h.product.market).or_default();
            *n += 1;
            *n <= per_market
        });
        hits
    }
}

/// English: substring for ≥3 chars, word-prefix for 2 chars.
fn english_match(name: &str, ql: &str) -> bool {
    let n = name.to_lowercase();
    match ql.chars().count() {
        0 | 1 => false,
        2 => n
            .split(|c: char| !c.is_alphanumeric())
            .any(|w| w.starts_with(ql)),
        _ => n.contains(ql),
    }
}

/// `rb2701` -> ("rb", "2701"); `AP601` -> ("AP", "601"). Letters 1–2, digits 3–4.
pub fn split_cn_contract(q: &str) -> Option<(String, &str)> {
    let alpha: String = q.chars().take_while(|c| c.is_ascii_alphabetic()).collect();
    let digits = &q[alpha.len()..];
    let ok = (1..=2).contains(&alpha.len())
        && (3..=4).contains(&digits.len())
        && digits.bytes().all(|b| b.is_ascii_digit());
    ok.then_some((alpha, digits))
}

/// Contract code as the dominant / EOD files spell it: CZCE / CFFEX
/// upper-case, the others lower-case; CZCE three-digit `YMM` codes are
/// normalised to `YYMM` (AP601 -> AP2601), as the roll outputs store them.
fn cn_contract_code(p: &Product, root: &str, digits: &str) -> String {
    let upper = p.exchanges.iter().any(|e| e == "xzce" || e == "ccfx");
    if upper && digits.len() == 3 {
        format!("{}2{}", root.to_ascii_uppercase(), digits)
    } else if upper {
        format!("{}{}", root.to_ascii_uppercase(), digits)
    } else {
        format!("{}{}", root.to_ascii_lowercase(), digits)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(market: Market, code: &str, ex: &str, cn: Option<&str>, en: Option<&str>) -> Product {
        Product {
            market,
            code: code.into(),
            exchanges: vec![ex.into()],
            exchange_cn: String::new(),
            name_cn: cn.map(Into::into),
            name_cn_source: None,
            category: None,
            name_en: en.map(Into::into),
            name_en_source: None,
            multiplier: None,
            tick: None,
            summary: serde_json::Value::Null,
        }
    }

    fn index() -> Index {
        let mut ix = Index::new(vec![
            p(Market::Cn, "RB", "xsge", Some("螺纹钢"), None),
            p(Market::Cn, "SC", "xsie", Some("原油"), None),
            p(Market::Cn, "AU", "xsge", Some("黄金"), None),
            p(Market::Cn, "AP", "xzce", Some("苹果"), None),
            p(
                Market::Lseg,
                "ES",
                "cme",
                Some("E-mini标普500指数"),
                Some("EMINI S&P"),
            ),
            p(
                Market::Lseg,
                "CL",
                "nymex",
                Some("WTI轻质原油"),
                Some("LIGHT CRUDE"),
            ),
            p(
                Market::Lseg,
                "GC",
                "comex",
                Some("标准黄金（100盎司）"),
                Some("GOLD"),
            ),
            p(Market::Lseg, "CLR", "nymex", None, None),
            p(
                Market::Ib,
                "ES",
                "CME",
                Some("E-mini标普500指数"),
                Some("E-mini S&P 500"),
            ),
            p(
                Market::Ib,
                "CL",
                "NYMEX",
                Some("WTI轻质原油"),
                Some("Light Sweet Crude Oil"),
            ),
        ]);
        ix.add_contract(Market::Lseg, "ES", "ESZ26");
        ix.add_contract(Market::Ib, "ES", "ESZ26");
        ix.add_contract(Market::Ib, "ES", "ESZ6");
        ix
    }

    #[test]
    fn chinese_names_group_by_market() {
        let ix = index();
        let hits = ix.search("原油", 10);
        let codes: Vec<_> = hits
            .iter()
            .map(|h| (h.product.market, h.product.code.as_str()))
            .collect();
        assert_eq!(
            codes,
            vec![(Market::Cn, "SC"), (Market::Lseg, "CL"), (Market::Ib, "CL")]
        );
        assert_eq!(hits[0].matched, "中文名");
        let gold = ix.search("黄金", 10);
        assert_eq!(gold.len(), 2);
        assert_eq!(gold[0].product.code, "AU");
        assert!(ix.search("标普", 10).iter().all(|h| h.product.code == "ES"));
    }

    #[test]
    fn code_and_cn_contract() {
        let ix = index();
        let h = ix.search("rb", 10);
        assert_eq!(h[0].product.code, "RB");
        assert_eq!(h[0].score, 100);
        let h = ix.search("rb2701", 10);
        assert_eq!(h.len(), 1);
        assert_eq!(h[0].contract.as_deref(), Some("rb2701"));
        let h = ix.search("ap601", 10);
        assert_eq!(h[0].contract.as_deref(), Some("AP2601"));
        // exact code ranks above prefix match
        let h = ix.search("CL", 10);
        let lseg: Vec<_> = h
            .iter()
            .filter(|h| h.product.market == Market::Lseg)
            .collect();
        assert_eq!(lseg[0].product.code, "CL");
        assert_eq!(lseg[1].product.code, "CLR");
    }

    #[test]
    fn ric_and_local_symbol() {
        let ix = index();
        let h = ix.search("esz26", 10);
        assert_eq!(h.len(), 2);
        assert!(h.iter().all(|h| h.contract.as_deref() == Some("ESZ26")));
        let h = ix.search("ESZ6", 10);
        assert_eq!(h.len(), 1);
        assert_eq!(h[0].product.market, Market::Ib);
    }

    #[test]
    fn english_names() {
        let ix = index();
        let h = ix.search("crude", 10);
        assert_eq!(h.len(), 2);
        assert!(h.iter().all(|h| h.matched == "英文名"));
        // two letters: word prefix only
        assert!(ix.search("ru", 10).iter().all(|h| h.matched != "英文名"));
        assert!(ix.search("", 10).is_empty());
    }

    #[test]
    fn splits_cn_contracts() {
        assert_eq!(split_cn_contract("rb2701"), Some(("rb".into(), "2701")));
        assert_eq!(split_cn_contract("AP601"), Some(("AP".into(), "601")));
        assert_eq!(split_cn_contract("ESZ26"), None);
        assert_eq!(split_cn_contract("rb27"), None);
    }
}
