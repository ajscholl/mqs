use ::time::{error::ComponentRange, Date, Month, PrimitiveDateTime, Time};
#[cfg(feature = "diesel")]
use byteorder::{NetworkEndian, WriteBytesExt};
use cached::once_cell::sync::Lazy;
#[cfg(feature = "chrono")]
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
#[cfg(feature = "diesel")]
use diesel::{
    data_types::{PgInterval, PgTimestamp},
    deserialize::FromSql,
    expression::ValidGrouping,
    pg::Pg,
    pg::PgValue,
    serialize::IsNull,
    serialize::{Output, ToSql},
    sql_types::{Timestamp, Timestamptz},
    AsExpression,
    FromSqlRow,
    QueryId,
};
#[cfg(feature = "diesel")]
use std::num::TryFromIntError;
use std::{
    convert::TryFrom,
    error::Error,
    fmt::{Display, Formatter},
    num::ParseIntError,
    ops::{Add, Sub},
    time::{Duration, SystemTime},
};

/// A `UtcTime` represents a timestamp in the UTC timezone.
#[derive(Clone, Copy, Hash, Eq, PartialEq, Ord, PartialOrd, Debug, Deserialize, Serialize)]
#[cfg_attr(feature = "diesel", derive(AsExpression, FromSqlRow, QueryId, ValidGrouping))]
#[cfg_attr(feature = "diesel", diesel(sql_type = Timestamp))]
pub struct UtcTime {
    time: PrimitiveDateTime,
}

/// A `UtcTimeParseError` is returned when parsing a `UtcTime` fails.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum UtcTimeParseError {
    /// The given string had an incorrect length.
    InvalidLengthError(usize),
    /// The given string contained an unexpected character.
    UnexpectedCharacter(char, char, usize),
    /// The given string contained something not parsable as a number.
    InvalidCharactersError(ParseIntError),
    /// The given string did not represent a valid date.
    InvalidTimestampError(ComponentRange),
}

impl From<ParseIntError> for UtcTimeParseError {
    fn from(err: ParseIntError) -> Self {
        Self::InvalidCharactersError(err)
    }
}

impl From<ComponentRange> for UtcTimeParseError {
    fn from(err: ComponentRange) -> Self {
        Self::InvalidTimestampError(err)
    }
}

impl Display for UtcTimeParseError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        use UtcTimeParseError::{
            InvalidCharactersError,
            InvalidLengthError,
            InvalidTimestampError,
            UnexpectedCharacter,
        };
        match self {
            InvalidLengthError(actual) => write!(f, "Invalid length {}", actual),
            UnexpectedCharacter(c, expected, position) => write!(
                f,
                "Unexpected character '{}' at position {}, expected '{}'",
                c, position, expected
            ),
            InvalidCharactersError(err) => write!(f, "Unexpected characters when parsing: {}", err),
            InvalidTimestampError(err) => write!(f, "Does not represent a valid date: {}", err),
        }
    }
}

impl Error for UtcTimeParseError {}

static UNIX_EPOCH: Lazy<PrimitiveDateTime> = Lazy::new(|| {
    PrimitiveDateTime::new(
        Date::from_calendar_date(1970, Month::January, 1).expect("1970-01-01 should be a valid date"),
        Time::MIDNIGHT,
    )
});

impl UtcTime {
    /// now returns the current time.
    ///
    /// ```
    /// use mqs_common::UtcTime;
    ///
    /// let current_time = UtcTime::now();
    /// ```
    #[must_use]
    pub fn now() -> Self {
        match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
            Ok(since_unix) => Self {
                time: UNIX_EPOCH.add(since_unix),
            },
            Err(until_unix) => Self {
                time: UNIX_EPOCH.sub(until_unix.duration()),
            },
        }
    }

    /// `from_timestamp` returns the time corresponding to the given unix timestamp.
    /// The timestamp is given in seconds.
    ///
    /// ```
    /// use mqs_common::UtcTime;
    ///
    /// let time = UtcTime::from_timestamp(0);
    /// assert_eq!(time.to_rfc3339(), "1970-01-01T00:00:00Z");
    /// let time = UtcTime::from_timestamp(30);
    /// assert_eq!(time.to_rfc3339(), "1970-01-01T00:00:30Z");
    /// let time = UtcTime::from_timestamp(-30);
    /// assert_eq!(time.to_rfc3339(), "1969-12-31T23:59:30Z");
    /// ```
    #[must_use]
    pub fn from_timestamp(epoch: i64) -> Self {
        u64::try_from(epoch).map_or_else(
            |_| Self {
                time: UNIX_EPOCH.sub(Duration::from_secs(u64::try_from(epoch.abs()).unwrap())),
            },
            |epoch_seconds| Self {
                time: UNIX_EPOCH.add(Duration::from_secs(epoch_seconds)),
            },
        )
    }

    /// `parse_from_rfc3339` is the inverse of `to_rfc3339` and `to_rfc3339_nanos`. It parses a UTC timestamp
    /// with second, millisecond, or nanosecond precision.
    ///
    /// ```
    /// use mqs_common::UtcTime;
    ///
    /// let time = UtcTime::now();
    ///
    /// assert_eq!(
    ///     Ok(time),
    ///     UtcTime::parse_from_rfc3339(&time.to_rfc3339_nanos())
    /// );
    /// ```
    pub fn parse_from_rfc3339(s: &str) -> Result<Self, UtcTimeParseError> {
        const SECOND_PRECISION: usize = "YYYY-MM-DDTHH:ii:ssZ".len();
        const MILLISECOND_PRECISION: usize = "YYYY-MM-DDTHH:ii:ss.mmmZ".len();
        const NANOSECOND_PRECISION: usize = "YYYY-MM-DDTHH:ii:ss.nnnnnnnnnZ".len();
        if s.len() < SECOND_PRECISION {
            return Err(UtcTimeParseError::InvalidLengthError(s.len()));
        }
        let yyyy = s[0..4].parse()?;
        Self::expect_char(s, 4, '-')?;
        let mm = Month::try_from(s[5..7].parse::<u8>()?)?;
        Self::expect_char(s, 7, '-')?;
        let dd = s[8..10].parse()?;
        Self::expect_char(s, 10, 'T')?;
        let hh = s[11..13].parse()?;
        Self::expect_char(s, 13, ':')?;
        let ii = s[14..16].parse()?;
        Self::expect_char(s, 16, ':')?;
        let ss = s[17..19].parse()?;
        Self::expect_char(s, s.len() - 1, 'Z')?;
        match s.len() {
            SECOND_PRECISION => Ok(Self {
                time: PrimitiveDateTime::new(Date::from_calendar_date(yyyy, mm, dd)?, Time::from_hms(hh, ii, ss)?),
            }),
            MILLISECOND_PRECISION => {
                Self::expect_char(s, 19, '.')?;
                let millis = s[20..23].parse()?;

                Ok(Self {
                    time: PrimitiveDateTime::new(
                        Date::from_calendar_date(yyyy, mm, dd)?,
                        Time::from_hms_milli(hh, ii, ss, millis)?,
                    ),
                })
            },
            NANOSECOND_PRECISION => {
                Self::expect_char(s, 19, '.')?;
                let nanos = s[20..29].parse()?;

                Ok(Self {
                    time: PrimitiveDateTime::new(
                        Date::from_calendar_date(yyyy, mm, dd)?,
                        Time::from_hms_nano(hh, ii, ss, nanos)?,
                    ),
                })
            },
            _ => Err(UtcTimeParseError::InvalidLengthError(s.len())),
        }
    }

    fn expect_char(s: &str, position: usize, expected: char) -> Result<(), UtcTimeParseError> {
        s[position..]
            .chars()
            .next()
            .map_or(Err(UtcTimeParseError::InvalidLengthError(s.len())), |c| {
                if c == expected {
                    Ok(())
                } else {
                    Err(UtcTimeParseError::UnexpectedCharacter(c, expected, position))
                }
            })
    }

    /// Format a timestamp according to RFC3339 with second precision.
    ///
    /// ```
    /// use mqs_common::UtcTime;
    ///
    /// let time = UtcTime::from_timestamp(1200);
    ///
    /// assert_eq!("1970-01-01T00:20:00Z", &time.to_rfc3339());
    /// ```
    #[must_use]
    pub fn to_rfc3339(&self) -> String {
        let yyyy = self.time.year();
        let mm = self.time.month() as u8;
        let dd = self.time.day();
        let hh = self.time.hour();
        let ii = self.time.minute();
        let ss = self.time.second();

        format!("{:0>4}-{:0>2}-{:0>2}T{:0>2}:{:0>2}:{:0>2}Z", yyyy, mm, dd, hh, ii, ss)
    }

    /// Format a timestamp according to RFC3339 with nanosecond precision.
    ///
    /// ```
    /// use mqs_common::UtcTime;
    /// use std::time::Duration;
    ///
    /// let time = UtcTime::from_timestamp(1200).add(Duration::from_nanos(123_456_789));
    ///
    /// assert_eq!("1970-01-01T00:20:00.123456789Z", &time.to_rfc3339_nanos());
    /// ```
    #[must_use]
    pub fn to_rfc3339_nanos(&self) -> String {
        let yyyy = self.time.year();
        let mm = self.time.month() as u8;
        let dd = self.time.day();
        let hh = self.time.hour();
        let ii = self.time.minute();
        let ss = self.time.second();
        let nanos = self.time.nanosecond();

        format!(
            "{:0>4}-{:0>2}-{:0>2}T{:0>2}:{:0>2}:{:0>2}.{:0>9}Z",
            yyyy, mm, dd, hh, ii, ss, nanos
        )
    }

    /// Add the given duration from a timestamp.
    ///
    /// ```
    /// use mqs_common::UtcTime;
    /// use std::time::Duration;
    ///
    /// let time = UtcTime::from_timestamp(1000);
    /// assert_eq!(
    ///     UtcTime::from_timestamp(1500),
    ///     time.add(Duration::from_secs(500))
    /// );
    /// ```
    #[must_use]
    pub fn add(&self, d: Duration) -> Self {
        Self { time: self.time.add(d) }
    }

    /// Subtract the given duration from a timestamp.
    ///
    /// ```
    /// use mqs_common::UtcTime;
    /// use std::time::Duration;
    ///
    /// let time = UtcTime::from_timestamp(1000);
    /// assert_eq!(
    ///     UtcTime::from_timestamp(500),
    ///     time.sub(Duration::from_secs(500))
    /// );
    /// ```
    #[must_use]
    pub fn sub(&self, d: Duration) -> Self {
        Self { time: self.time.sub(d) }
    }

    /// Compute the time which this timestamp is later than the given time. If this timestamp is after
    /// the given time, the duration is returned as an error.
    ///
    /// ```
    /// use mqs_common::UtcTime;
    /// use std::time::Duration;
    ///
    /// let earlier = UtcTime::from_timestamp(1000);
    /// let later = UtcTime::from_timestamp(1500);
    ///
    /// assert_eq!(Ok(Duration::from_secs(500)), later.since(&earlier));
    /// assert_eq!(Err(Duration::from_secs(500)), earlier.since(&later));
    /// ```
    pub fn since(&self, other: &Self) -> Result<Duration, Duration> {
        let diff = Duration::from_nanos(
            u64::try_from(
                (self.time.assume_utc().unix_timestamp_nanos() - other.time.assume_utc().unix_timestamp_nanos()).abs(),
            )
            .unwrap_or(u64::MAX),
        );
        if self >= other {
            Ok(diff)
        } else {
            Err(diff)
        }
    }
}

#[cfg(feature = "chrono")]
impl Into<DateTime<Utc>> for UtcTime {
    fn into(self) -> DateTime<Utc> {
        let date = {
            let year = self.time.year();
            let month = self.time.month() as u8;
            let day = self.time.day();

            NaiveDate::from_ymd_opt(year, u32::from(month), u32::from(day))
                .expect("Failed to convert between different dates")
        };
        let time = {
            let (h, m, s, nano) = self.time.time().as_hms_nano();

            NaiveTime::from_hms_nano_opt(u32::from(h), u32::from(m), u32::from(s), nano)
                .expect("Failed to convert between different times")
        };
        DateTime::from_utc(NaiveDateTime::new(date, time), Utc)
    }
}

#[cfg(feature = "diesel")]
#[derive(Debug, Clone, Eq, PartialEq)]
struct UtcTimeSqlConversionError(TryFromIntError);

#[cfg(feature = "diesel")]
impl Display for UtcTimeSqlConversionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "time conversion failed: {}", self.0)
    }
}

#[cfg(feature = "diesel")]
impl Error for UtcTimeSqlConversionError {}

#[cfg(feature = "diesel")]
impl FromSql<Timestamp, Pg> for UtcTime {
    fn from_sql(bytes: PgValue<'_>) -> diesel::deserialize::Result<Self> {
        let PgTimestamp(offset) = FromSql::<Timestamp, Pg>::from_sql(bytes)?;
        match u64::try_from(offset) {
            Err(_) => {
                let offset = u64::try_from(offset.abs()).map_err(UtcTimeSqlConversionError)?;
                Ok(PG_EPOCH.sub(Duration::from_micros(offset)))
            },
            Ok(offset) => Ok(PG_EPOCH.add(Duration::from_micros(offset))),
        }
    }
}

#[cfg(feature = "diesel")]
impl ToSql<Timestamp, Pg> for UtcTime {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Pg>) -> diesel::serialize::Result {
        let time = self.since_pg_epoch_micros().map_err(UtcTimeSqlConversionError)?;

        out.write_i64::<NetworkEndian>(time)
            .map(|_| IsNull::No)
            .map_err(|e| Box::new(e) as Box<_>)
    }
}

#[cfg(feature = "diesel")]
impl FromSql<Timestamptz, Pg> for UtcTime {
    fn from_sql(bytes: PgValue<'_>) -> diesel::deserialize::Result<Self> {
        FromSql::<Timestamp, Pg>::from_sql(bytes)
    }
}

#[cfg(feature = "diesel")]
impl ToSql<Timestamptz, Pg> for UtcTime {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Pg>) -> diesel::serialize::Result {
        ToSql::<Timestamp, Pg>::to_sql(self, out)
    }
}

// Postgres timestamps start from January 1st 2000.
#[cfg(feature = "diesel")]
static PG_EPOCH: Lazy<UtcTime> = Lazy::new(|| UtcTime {
    time: PrimitiveDateTime::new(
        Date::from_calendar_date(2000, Month::January, 1).expect("2000-01-01 should be a valid date"),
        Time::MIDNIGHT,
    ),
});

#[cfg(feature = "diesel")]
impl UtcTime {
    /// Add a `PgInterval` to a `UtcTime`.
    ///
    /// Example:
    /// ```
    /// use diesel::data_types::PgInterval;
    /// use mqs_common::UtcTime;
    ///
    /// let interval = PgInterval::new(1000000, 1, 1);
    /// let time =
    ///     UtcTime::parse_from_rfc3339("2020-01-01T00:00:00Z").expect("Should parse for this test");
    /// let result = time.add_pg_interval(&interval);
    ///
    /// assert_eq!(result.to_rfc3339(), "2020-02-01T00:00:01Z");
    /// ```
    #[must_use]
    pub fn add_pg_interval(&self, offset: &PgInterval) -> Self {
        let micros = offset.microseconds + 1_000_000 * 3600 * 24 * i64::from(offset.days + offset.months * 30);
        u64::try_from(micros).map_or_else(
            |_| self.sub(Duration::from_micros(u64::try_from(micros.abs()).unwrap())),
            |micros| self.add(Duration::from_micros(micros)),
        )
    }

    fn since_pg_epoch_micros(&self) -> Result<i64, TryFromIntError> {
        match self.since(&PG_EPOCH) {
            Ok(d) => i64::try_from(d.as_micros()),
            Err(d) => i64::try_from(d.as_micros()).map(|v| -v),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::thread::sleep;

    #[test]
    async fn current_time_ticks_forward() {
        let old = UtcTime::now();
        sleep(Duration::from_millis(10));
        let new = UtcTime::now();
        assert!(new > old);
        assert!(new.since(&old).unwrap() >= Duration::from_millis(10));
    }

    #[test]
    async fn parse() {
        assert!(UtcTime::parse_from_rfc3339("").is_err());
        assert!(UtcTime::parse_from_rfc3339("2000").is_err());
        assert!(UtcTime::parse_from_rfc3339("2000-01").is_err());
        assert!(UtcTime::parse_from_rfc3339("2000-01-01").is_err());
        assert!(UtcTime::parse_from_rfc3339("2000-01-01T").is_err());
        assert!(UtcTime::parse_from_rfc3339("2000-01-01T00").is_err());
        assert!(UtcTime::parse_from_rfc3339("2000-01-01T00:00").is_err());
        assert!(UtcTime::parse_from_rfc3339("2000-01-01T00:00:00").is_err());
        assert_eq!(
            UtcTime::parse_from_rfc3339("2000-01-01T00:00:00Z"),
            Ok(UtcTime::from_timestamp((30 * 365 + 7) * 24 * 3600))
        );
        assert_eq!(
            UtcTime::parse_from_rfc3339("2000-01-01T00:00:00.123Z"),
            Ok(UtcTime::from_timestamp((30 * 365 + 7) * 24 * 3600).add(Duration::from_millis(123)))
        );
        assert_eq!(
            UtcTime::parse_from_rfc3339("2000-01-01T00:00:00.123456789Z"),
            Ok(UtcTime::from_timestamp((30 * 365 + 7) * 24 * 3600).add(Duration::from_nanos(123456789)))
        );
    }

    #[test]
    async fn format() {
        assert_eq!(
            UtcTime::from_timestamp((30 * 365 + 7) * 24 * 3600).to_rfc3339(),
            "2000-01-01T00:00:00Z"
        );
        assert_eq!(
            UtcTime::from_timestamp((30 * 365 + 7) * 24 * 3600)
                .add(Duration::from_millis(100))
                .to_rfc3339(),
            "2000-01-01T00:00:00Z"
        );
        assert_eq!(
            UtcTime::from_timestamp((30 * 365 + 7) * 24 * 3600).to_rfc3339_nanos(),
            "2000-01-01T00:00:00.000000000Z"
        );
        assert_eq!(
            UtcTime::from_timestamp((30 * 365 + 7) * 24 * 3600)
                .add(Duration::from_millis(100))
                .to_rfc3339_nanos(),
            "2000-01-01T00:00:00.100000000Z"
        );
    }

    #[cfg(feature = "chrono")]
    #[test]
    async fn to_chrono() {
        use chrono::{DateTime, Utc};

        let now = UtcTime::now();
        let chrono_now: DateTime<Utc> = now.into();

        assert_eq!(chrono_now.to_rfc3339().replace("+00:00", "Z"), now.to_rfc3339_nanos());
    }
}
