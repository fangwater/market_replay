use cn_futures_core::mdl::{parse_frame, ParseError, UNKNOWN_QUANTITY};
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

thread_local! {static ON: Cell<bool> = const { Cell::new(false) }; static N: Cell<usize> = const { Cell::new(0) };}
struct Counting;
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        if ON.try_with(Cell::get).unwrap_or(false) {
            let _ = N.try_with(|n| n.set(n.get() + 1));
        }
        System.alloc(l)
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        System.dealloc(p, l)
    }
}
#[global_allocator]
static A: Counting = Counting;

const FRAMES: [(&[u8], &str); 6] = [
    (include_bytes!("fixtures/mdl_21_1.txt"), "CFFEX"),
    (include_bytes!("fixtures/mdl_22_1.txt"), "SHFE"),
    (include_bytes!("fixtures/mdl_22_3.txt"), "INE"),
    (include_bytes!("fixtures/mdl_23_1.txt"), "CZCE"),
    (include_bytes!("fixtures/mdl_24_1.txt"), "DCE"),
    (include_bytes!("fixtures/mdl_26_1.txt"), "GFEX"),
];

fn trim(f: &[u8]) -> &[u8] {
    f.strip_suffix(b"\n").unwrap_or(f)
}

#[test]
fn six_live_fixtures_parse_with_native_symbols() {
    for (frame, venue) in FRAMES {
        let q = parse_frame(trim(frame)).unwrap();
        assert_eq!(q.venue, venue);
        assert!(!q.symbol.is_empty() && q.trading_day > 20_000_000 && q.exchange_ts_us > 0);
        assert!(q.last_price > 0.0 && q.volume != UNKNOWN_QUANTITY && q.turnover >= 0.0);
        assert!(q.bid[0].price > 0.0 && q.ask[0].price >= q.bid[0].price);
    }
}

#[test]
fn rejects_unknown_topic_and_truncation() {
    assert_eq!(
        parse_frame(b"mdl.24.3.* || x").unwrap_err(),
        ParseError("not a supported futures L2 topic")
    );
    assert_eq!(
        parse_frame(b"mdl.22.1.*|x").unwrap_err(),
        ParseError("invalid single-frame delimiter")
    );
    assert_eq!(
        parse_frame(b"mdl.22.1.* || rb2601,2026-11-13").unwrap_err(),
        ParseError("truncated futures quote")
    );
}

#[test]
fn parser_allocates_nothing() {
    N.with(|n| n.set(0));
    ON.with(|b| b.set(true));
    for _ in 0..1000 {
        for (frame, _) in FRAMES {
            std::hint::black_box(parse_frame(std::hint::black_box(trim(frame))).unwrap());
        }
    }
    ON.with(|b| b.set(false));
    assert_eq!(N.with(Cell::get), 0);
}
