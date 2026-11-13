//! Scalar math, quantile and date helpers shared by the evaluator.

use crate::ast::*;

pub(super) fn math_fn(name: &str, x: f64) -> f64 {
    match name {
        "abs" => x.abs(),
        "ceil" => x.ceil(),
        "floor" => x.floor(),
        "sqrt" => x.sqrt(),
        "exp" => x.exp(),
        "ln" => x.ln(),
        "log2" => x.log2(),
        "log10" => x.log10(),
        // Trigonometric family (radians, PromQL semantics).
        "sin" => x.sin(),
        "cos" => x.cos(),
        "tan" => x.tan(),
        "asin" => x.asin(),
        "acos" => x.acos(),
        "atan" => x.atan(),
        "sinh" => x.sinh(),
        "cosh" => x.cosh(),
        "tanh" => x.tanh(),
        "asinh" => x.asinh(),
        "acosh" => x.acosh(),
        "atanh" => x.atanh(),
        // Angle conversion.
        "deg" => x.to_degrees(),
        "rad" => x.to_radians(),
        "sgn" => {
            if x > 0.0 {
                1.0
            } else if x < 0.0 {
                -1.0
            } else {
                0.0
            }
        }
        _ => x,
    }
}

pub(super) fn map_values(v: InstantVector, f: impl Fn(f64) -> f64) -> InstantVector {
    // Prometheus drops series whose function result is NaN (e.g.
    // ln(-1), asin(2)) rather than emitting a NaN sample.
    InstantVector {
        series: v
            .series
            .into_iter()
            .filter_map(|(l, x)| {
                let y = f(x);
                if y.is_nan() {
                    None
                } else {
                    Some((l, y))
                }
            })
            .collect(),
    }
}

pub(super) fn quantile_of(q: f64, sorted: &[f64]) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    let pos = q * (sorted.len() - 1) as f64;
    let lo = pos.floor() as usize;
    let hi = pos.ceil() as usize;
    if lo == hi {
        sorted[lo]
    } else {
        let frac = pos - lo as f64;
        sorted[lo] * (1.0 - frac) + sorted[hi] * frac
    }
}

pub(super) fn quantile_from_buckets(q: f64, buckets: &[(f64, f64)]) -> Option<f64> {
    // buckets: (le, count), sorted ascending; last should be +Inf.
    if buckets.len() < 2 {
        return None;
    }
    let total = buckets.last().map(|b| b.1).unwrap_or(0.0);
    if total <= 0.0 {
        return None;
    }
    let target = q * total;
    let mut prev_count = 0.0;
    let mut prev_bound = f64::NEG_INFINITY;
    for (bound, count) in buckets.iter().copied() {
        if count >= target {
            if bound == f64::INFINITY {
                return Some(prev_bound);
            }
            let frac = (target - prev_count) / (count - prev_count);
            return Some(prev_bound + (bound - prev_bound) * frac);
        }
        prev_count = count;
        prev_bound = bound;
    }
    buckets.last().map(|b| b.0)
}

/// Extracts a calendar component (minute/hour/day/…) from epoch seconds.
/// Mirrors Prometheus: components are in UTC.
pub(super) fn date_component(name: &str, secs: f64) -> f64 {
    use chrono::{Datelike, Timelike};
    let dt = chrono::DateTime::from_timestamp(secs as i64, 0).unwrap_or_default();
    match name {
        "minute" => dt.minute() as f64,
        "hour" => dt.hour() as f64,
        "day_of_week" => dt.weekday().num_days_from_sunday() as f64,
        "day_of_month" | "day_of_month_iso" => dt.day() as f64,
        "day_of_year" => dt.ordinal() as f64,
        "days_in_month" => days_in_month(dt.year(), dt.month()) as f64,
        "month" => dt.month() as f64,
        _ => dt.year() as f64,
    }
}

pub(super) fn days_in_month(year: i32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            let leap = (year % 4 == 0 && year % 100 != 0) || year % 400 == 0;
            if leap {
                29
            } else {
                28
            }
        }
        _ => 30,
    }
}

pub(super) fn fmt_num(v: f64) -> String {
    if v.fract() == 0.0 {
        format!("{}", v as i64)
    } else {
        v.to_string()
    }
}
