//! Cadence specifications: `instant`, `every(<duration>)` and
//! `cron("<expr>")`, and the next tick of each.

use std::fmt;

/// One minute in milliseconds.
const MINUTE_MS: u64 = 60_000;
/// One day in minutes.
const DAY_MINUTES: u64 = 24 * 60;
/// How far ahead a cron search looks before giving up: five years.
const CRON_HORIZON_DAYS: u64 = 5 * 366;

/// When a cadence releases work.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Cadence {
    /// As soon as it is ready.
    Instant,
    /// On ticks every `period` milliseconds, aligned to the Unix epoch.
    Every(u64),
    /// On the minutes a cron expression names, in UTC.
    Cron(Cron),
}

/// Why a cadence string did not parse.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CadenceError(String);

impl fmt::Display for CadenceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for CadenceError {}

fn error(message: impl Into<String>) -> CadenceError {
    CadenceError(message.into())
}

impl Cadence {
    /// Parse `instant`, `every(10m)` (units `ms`, `s`, `m`, `h`, `d`) or
    /// `cron("*/10 * * * *")`.
    ///
    /// # Errors
    ///
    /// A [`CadenceError`] saying what is wrong.
    pub fn parse(text: &str) -> Result<Self, CadenceError> {
        let text = text.trim();
        if text == "instant" {
            return Ok(Self::Instant);
        }
        if let Some(inner) = text
            .strip_prefix("every(")
            .and_then(|t| t.strip_suffix(')'))
        {
            return parse_duration(inner.trim()).map(Self::Every);
        }
        if let Some(inner) = text.strip_prefix("cron(").and_then(|t| t.strip_suffix(')')) {
            let inner = inner.trim();
            let expr = inner
                .strip_prefix('"')
                .and_then(|t| t.strip_suffix('"'))
                .unwrap_or(inner);
            return Cron::parse(expr).map(Self::Cron);
        }
        Err(error(
            "expected `instant`, `every(<n><ms|s|m|h|d>)` or `cron(\"<expr>\")`",
        ))
    }

    /// A stable description of the schedule, to notice when it changes.
    #[must_use]
    pub fn fingerprint(&self) -> String {
        match self {
            Self::Instant => "instant".to_owned(),
            Self::Every(period) => format!("every:{period}"),
            Self::Cron(cron) => format!("cron:{}", cron.expr),
        }
    }

    /// The first tick strictly after `after_ms`, or `None` for
    /// [`Instant`](Self::Instant) and for a cron that never fires.
    #[must_use]
    pub fn next_tick(&self, after_ms: u64) -> Option<u64> {
        match self {
            Self::Instant => None,
            Self::Every(period) => (after_ms / period)
                .checked_add(1)
                .and_then(|n| n.checked_mul(*period)),
            Self::Cron(cron) => cron.next_after(after_ms),
        }
    }
}

fn parse_duration(text: &str) -> Result<u64, CadenceError> {
    let split = text
        .find(|c: char| !c.is_ascii_digit())
        .ok_or_else(|| error("a duration needs a unit: ms, s, m, h or d"))?;
    let (number, unit) = text.split_at(split);
    let number: u64 = number
        .parse()
        .map_err(|_| error(format!("{number:?} is not a number")))?;
    let scale = match unit {
        "ms" => 1,
        "s" => 1_000,
        "m" => MINUTE_MS,
        "h" => 60 * MINUTE_MS,
        "d" => DAY_MINUTES * MINUTE_MS,
        other => return Err(error(format!("unknown unit {other:?}"))),
    };
    match number.checked_mul(scale) {
        Some(0) => Err(error("a period must be positive")),
        Some(period) => Ok(period),
        None => Err(error("the period is too long")),
    }
}

/// A five-field cron expression: minute, hour, day of month, month, day of
/// week (0 or 7 is Sunday). Each field is `*`, a number, a range `a-b`, any
/// of those with a step `/n`, or a comma list of them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cron {
    expr: String,
    minutes: Vec<bool>,
    hours: Vec<bool>,
    days: Vec<bool>,
    months: Vec<bool>,
    weekdays: Vec<bool>,
    any_day: bool,
    any_weekday: bool,
}

fn parse_field(text: &str, low: u32, high: u32, name: &str) -> Result<Vec<bool>, CadenceError> {
    let mut set = vec![false; usize::try_from(high + 1).unwrap_or(0)];
    let number = |part: &str| -> Result<u32, CadenceError> {
        let value: u32 = part
            .parse()
            .map_err(|_| error(format!("{name}: {part:?} is not a number")))?;
        if (low..=high).contains(&value) {
            Ok(value)
        } else {
            Err(error(format!("{name}: {value} is outside {low}-{high}")))
        }
    };
    for item in text.split(',') {
        let (range, step) = match item.split_once('/') {
            Some((range, step)) => {
                let step: u32 = step
                    .parse()
                    .map_err(|_| error(format!("{name}: bad step {step:?}")))?;
                if step == 0 {
                    return Err(error(format!("{name}: a step must be positive")));
                }
                (range, step)
            }
            None => (item, 1),
        };
        let (from, to) = match range {
            "*" => (low, high),
            _ => {
                if let Some((from, to)) = range.split_once('-') {
                    (number(from)?, number(to)?)
                } else {
                    let value = number(range)?;
                    (value, if step > 1 { high } else { value })
                }
            }
        };
        if from > to {
            return Err(error(format!("{name}: {from}-{to} is empty")));
        }
        for value in (from..=to).step_by(usize::try_from(step).unwrap_or(1)) {
            set[usize::try_from(value).unwrap_or(0)] = true;
        }
    }
    Ok(set)
}

/// (year, month 1-12, day 1-31) of a day count since the Unix epoch.
fn civil(days: u64) -> (u64, u32, u32) {
    let z = days + 719_468;
    let era = z / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = u32::try_from(doy - (153 * mp + 2) / 5 + 1).unwrap_or(1);
    let month = u32::try_from(if mp < 10 { mp + 3 } else { mp - 9 }).unwrap_or(1);
    let year = yoe + era * 400 + u64::from(month <= 2);
    (year, month, day)
}

fn at(set: &[bool], index: u64) -> bool {
    usize::try_from(index)
        .ok()
        .and_then(|i| set.get(i))
        .copied()
        .unwrap_or(false)
}

impl Cron {
    /// Parse a five-field expression.
    ///
    /// # Errors
    ///
    /// A [`CadenceError`] naming the bad field.
    pub fn parse(expr: &str) -> Result<Self, CadenceError> {
        let fields: Vec<&str> = expr.split_whitespace().collect();
        let [minute, hour, day, month, weekday] = fields.as_slice() else {
            return Err(error("a cron expression has five fields"));
        };
        let mut weekdays = parse_field(weekday, 0, 7, "day of week")?;
        if weekdays[7] {
            weekdays[0] = true;
        }
        weekdays.truncate(7);
        Ok(Self {
            expr: fields.join(" "),
            minutes: parse_field(minute, 0, 59, "minute")?,
            hours: parse_field(hour, 0, 23, "hour")?,
            days: parse_field(day, 1, 31, "day of month")?,
            months: parse_field(month, 1, 12, "month")?,
            weekdays,
            any_day: day.starts_with('*'),
            any_weekday: weekday.starts_with('*'),
        })
    }

    fn day_matches(&self, days: u64) -> bool {
        let (_, month, day) = civil(days);
        if !at(&self.months, u64::from(month)) {
            return false;
        }
        let by_day = at(&self.days, u64::from(day));
        let by_weekday = at(&self.weekdays, (days + 4) % 7);
        match (self.any_day, self.any_weekday) {
            (true, true) => true,
            (false, true) => by_day,
            (true, false) => by_weekday,
            (false, false) => by_day || by_weekday,
        }
    }

    /// The first matching minute strictly after `after_ms`.
    #[must_use]
    pub fn next_after(&self, after_ms: u64) -> Option<u64> {
        let mut minute = after_ms / MINUTE_MS + 1;
        let horizon = minute + CRON_HORIZON_DAYS * DAY_MINUTES;
        while minute < horizon {
            let days = minute / DAY_MINUTES;
            if !self.day_matches(days) {
                minute = (days + 1) * DAY_MINUTES;
                continue;
            }
            let of_day = minute % DAY_MINUTES;
            if !at(&self.hours, of_day / 60) {
                minute = minute - of_day % 60 + 60;
                continue;
            }
            if at(&self.minutes, of_day % 60) {
                return minute.checked_mul(MINUTE_MS);
            }
            minute += 1;
        }
        None
    }
}

#[cfg(test)]
#[path = "spec_tests.rs"]
mod tests;
