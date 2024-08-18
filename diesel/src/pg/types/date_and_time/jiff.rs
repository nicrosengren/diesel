#![allow(unused_imports)]

extern crate jiff;

use self::jiff::{
    civil,
    tz::TimeZone,
    Span,
    Unit,
    // civil::DateTime, // Naive DateTime
    // Timestamp,       // UTC Unix TZ
    //civil::Date, // Naive Date
    //civil::Time, // Naive Time
    Zoned, // Time zoned DateTime
};

use super::{PgDate, PgTime, PgTimestamp};
use crate::deserialize::{self, FromSql};
use crate::pg::{Pg, PgValue};
use crate::serialize::{self, Output, ToSql};
use crate::sql_types::{Date, Time, Timestamp, Timestamptz};

// Postgres timestamps start from January 1st 2000.
const PG_EPOCH_DATE: civil::Date = civil::Date::constant(2000, 1, 1);
const PG_EPOCH: civil::DateTime = PG_EPOCH_DATE.at(0, 0, 0, 0);

// Timestamp <-> civil::DateTime
//
#[cfg(all(feature = "jiff", feature = "postgres_backend"))]
impl FromSql<Timestamp, Pg> for civil::DateTime {
    fn from_sql(bytes: PgValue<'_>) -> deserialize::Result<Self> {
        let PgTimestamp(offset) = FromSql::<Timestamp, Pg>::from_sql(bytes)?;

        match PG_EPOCH.checked_add(Span::new().microseconds(offset)) {
            Ok(v) => Ok(v),
            Err(err) => {
                let message = format!(
                    "Tried to deserialize a timestamp to large for jiff::civil::DateTime: {err}"
                );
                Err(message.into())
            }
        }
    }
}

#[cfg(all(feature = "jiff", feature = "postgres_backend"))]
impl ToSql<Timestamp, Pg> for civil::DateTime {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Pg>) -> serialize::Result {
        let micros = (*self - PG_EPOCH).get_microseconds();
        ToSql::<Timestamp, Pg>::to_sql(&PgTimestamp(micros), &mut out.reborrow())
    }
}

// Timestamptz <-> civil::DateTime
//
#[cfg(all(feature = "jiff", feature = "postgres_backend"))]
impl FromSql<Timestamptz, Pg> for civil::DateTime {
    fn from_sql(bytes: PgValue<'_>) -> deserialize::Result<Self> {
        FromSql::<Timestamp, Pg>::from_sql(bytes)
    }
}

#[cfg(all(feature = "jiff", feature = "postgres_backend"))]
impl ToSql<Timestamptz, Pg> for civil::DateTime {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Pg>) -> serialize::Result {
        ToSql::<Timestamp, Pg>::to_sql(self, out)
    }
}

// Timestamptz <-> Zoned
//
#[cfg(all(feature = "jiff", feature = "postgres_backend"))]
impl FromSql<Timestamptz, Pg> for Zoned {
    fn from_sql(bytes: PgValue<'_>) -> deserialize::Result<Self> {
        let date_time = <civil::DateTime as FromSql<Timestamptz, Pg>>::from_sql(bytes)?;
        match date_time.to_zoned(TimeZone::UTC) {
            Ok(zoned) => Ok(zoned),
            Err(err) => {
                let message = format!("Pg Timestamptz could not be expressed in UTC: {err}");
                Err(message.into())
            }
        }
    }
}

#[cfg(all(feature = "jiff", feature = "postgres_backend"))]
impl ToSql<Timestamptz, Pg> for Zoned {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Pg>) -> serialize::Result {
        let as_utc = self.with_time_zone(TimeZone::UTC).datetime();
        ToSql::<Timestamptz, Pg>::to_sql(&as_utc, &mut out.reborrow())
    }
}

// Time <-> civil::Time
//
#[cfg(all(feature = "jiff", feature = "postgres_backend"))]
impl ToSql<Time, Pg> for civil::Time {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Pg>) -> serialize::Result {
        let span = *self - civil::Time::MIN;
        match span.total(Unit::Microsecond) {
            Ok(microseconds_f64) => {
                ToSql::<Time, Pg>::to_sql(&PgTime(microseconds_f64 as i64), &mut out.reborrow())
            }
            Err(err) => {
                let message = format!("Getting total duration of {self:?} as microseconds: {err}");
                Err(message.into())
            }
        }
    }
}

#[cfg(all(feature = "jiff", feature = "postgres_backend"))]
impl FromSql<Time, Pg> for civil::Time {
    fn from_sql(bytes: PgValue<'_>) -> deserialize::Result<Self> {
        let PgTime(offset_microseconds) = FromSql::<Time, Pg>::from_sql(bytes)?;
        let duration_since_midnight = Span::new().microseconds(offset_microseconds);
        Ok(civil::Time::MIN + duration_since_midnight)
    }
}

// Time <-> civil::Time
//
#[cfg(all(feature = "jiff", feature = "postgres_backend"))]
impl ToSql<Date, Pg> for civil::Date {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Pg>) -> serialize::Result {
        match (*self - PG_EPOCH_DATE).total(Unit::Day) {
            Ok(days_since_epoch_f64) => {
                let whole_days_since_epoch = days_since_epoch_f64 as i32;
                ToSql::<Date, Pg>::to_sql(&PgDate(whole_days_since_epoch), &mut out.reborrow())
            }

            Err(err) => {
                let message = format!("Converting {self:?} to pg::Date: {err}");
                Err(message.into())
            }
        }
    }
}

#[cfg(all(feature = "jiff", feature = "postgres_backend"))]
impl FromSql<Date, Pg> for civil::Date {
    fn from_sql(bytes: PgValue<'_>) -> deserialize::Result<Self> {
        let PgDate(offset) = FromSql::<Date, Pg>::from_sql(bytes)?;

        match PG_EPOCH_DATE.checked_add(Span::new().days(i64::from(offset))) {
            Ok(date) => Ok(date),
            Err(err) => {
                let message =
                    format!("could not add {offset} days to civil::Date 2001-01-01: {err}");
                Err(message.into())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    extern crate dotenvy;

    use crate::dsl::{now, sql};
    use crate::prelude::*;
    use crate::select;
    use crate::sql_types::{Date, Time, Timestamp, Timestamptz};
    use crate::test_helpers::connection;

    use jiff::{civil, tz, Span, Zoned};

    fn now_local() -> Zoned {
        Zoned::now()
    }

    fn now_utc() -> Zoned {
        Zoned::now().with_time_zone(tz::TimeZone::UTC)
    }

    #[test]
    fn unix_epoch_encodes_correctly() {
        let connection = &mut connection();
        let date_time = civil::date(1970, 1, 1).at(0, 0, 0, 0);
        let query = select(sql::<Timestamp>("'1970-01-01'").eq(date_time));

        assert!(query.get_result::<bool>(connection).unwrap());
    }

    /*

    #[test]
    fn unix_epoch_encodes_correctly_with_utc_timezone() {
        let connection = &mut connection();

        let zoned = civil::date(1970, 1, 1)
            .at(0, 0, 0, 0)
            .to_zoned(tz::TimeZone::UTC);

        let query = select(sql::<Timestamptz>("'1970-01-01Z'::timestamptz").eq(zoned));

        assert!(query.get_result::<bool>(connection).unwrap());
    }

    #[test]
    fn unix_epoch_encodes_correctly_with_timezone() {
        let connection = &mut connection();

        let zoned = civil::date(1970, 1, 1)
            .at(0, 0, 0, 0)
            .to_zoned(tz::TimeZone::fixed(tz::offset(1)));

        let query = select(sql::<Timestamptz>("'1970-01-01 01:00:00Z'::timestamptz").eq(zoned));

        assert!(query.get_result::<bool>(connection).unwrap());
    }

    #[test]
    fn unix_epoch_decodes_correctly() {
        let connection = &mut connection();
        let date_time = civil::date(1970, 1, 1).at(0, 0, 0, 0);
        let epoch_from_sql =
            select(sql::<Timestamp>("'1970-01-01'::timestamp")).get_result(connection);

        assert_eq!(Ok(date_time), epoch_from_sql);
    }

    #[test]
    fn unix_epoch_decodes_correctly_with_timezone() {
        let connection = &mut connection();
        let zoned = civil::date(1970, 1, 1)
            .at(0, 0, 0, 0)
            .to_zoned(tz::TimeZone::UTC)
            .expect("valid time");

        let epoch_from_sql = select(sql::<Timestamptz>("'1970-01-01Z'::timestamptz"))
            .get_result(connection)
            .expect("valid");

        assert_eq!(zoned, epoch_from_sql);
    }

    #[test]
    fn times_relative_to_now_encode_correctly() {
        let connection = &mut connection();

        let span = Span::new().seconds(60);

        let time = now_utc() + span;
        let query = select(now.at_time_zone("utc").lt(time));
        assert!(query.get_result::<bool>(connection).unwrap());

        let time = now_utc() - span;
        let query = select(now.at_time_zone("utc").gt(time));
        assert!(query.get_result::<bool>(connection).unwrap());
    }

        #[test]
        fn times_with_timezones_round_trip_after_conversion() {
            let connection = &mut connection();
            let time = FixedOffset::east_opt(3600)
                .unwrap()
                .with_ymd_and_hms(2016, 1, 2, 1, 0, 0)
                .unwrap();
            let expected = NaiveDate::from_ymd_opt(2016, 1, 1)
                .unwrap()
                .and_hms_opt(20, 0, 0)
                .unwrap();
            let query = select(time.into_sql::<Timestamptz>().at_time_zone("EDT"));
            assert_eq!(Ok(expected), query.get_result(connection));
        }

        #[test]
        fn times_of_day_encode_correctly() {
            let connection = &mut connection();

            let midnight = NaiveTime::from_hms_opt(0, 0, 0).unwrap();
            let query = select(sql::<Time>("'00:00:00'::time").eq(midnight));
            assert!(query.get_result::<bool>(connection).unwrap());

            let noon = NaiveTime::from_hms_opt(12, 0, 0).unwrap();
            let query = select(sql::<Time>("'12:00:00'::time").eq(noon));
            assert!(query.get_result::<bool>(connection).unwrap());

            let roughly_half_past_eleven = NaiveTime::from_hms_micro_opt(23, 37, 4, 2200).unwrap();
            let query = select(sql::<Time>("'23:37:04.002200'::time").eq(roughly_half_past_eleven));
            assert!(query.get_result::<bool>(connection).unwrap());
        }

        #[test]
        fn times_of_day_decode_correctly() {
            let connection = &mut connection();
            let midnight = NaiveTime::from_hms_opt(0, 0, 0).unwrap();
            let query = select(sql::<Time>("'00:00:00'::time"));
            assert_eq!(Ok(midnight), query.get_result::<NaiveTime>(connection));

            let noon = NaiveTime::from_hms_opt(12, 0, 0).unwrap();
            let query = select(sql::<Time>("'12:00:00'::time"));
            assert_eq!(Ok(noon), query.get_result::<NaiveTime>(connection));

            let roughly_half_past_eleven = NaiveTime::from_hms_micro_opt(23, 37, 4, 2200).unwrap();
            let query = select(sql::<Time>("'23:37:04.002200'::time"));
            assert_eq!(
                Ok(roughly_half_past_eleven),
                query.get_result::<NaiveTime>(connection)
            );
        }

        #[test]
        fn dates_encode_correctly() {
            let connection = &mut connection();
            let january_first_2000 = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
            let query = select(sql::<Date>("'2000-1-1'").eq(january_first_2000));
            assert!(query.get_result::<bool>(connection).unwrap());

            let distant_past = NaiveDate::from_ymd_opt(-398, 4, 11).unwrap(); // year 0 is 1 BC in this function
            let query = select(sql::<Date>("'399-4-11 BC'").eq(distant_past));
            assert!(query.get_result::<bool>(connection).unwrap());

            let julian_epoch = NaiveDate::from_ymd_opt(-4713, 11, 24).unwrap();
            let query = select(sql::<Date>("'J0'::date").eq(julian_epoch));
            assert!(query.get_result::<bool>(connection).unwrap());

            let max_date = NaiveDate::from_ymd_opt(262142, 12, 31).unwrap();
            let query = select(sql::<Date>("'262142-12-31'::date").eq(max_date));
            assert!(query.get_result::<bool>(connection).unwrap());

            let january_first_2018 = NaiveDate::from_ymd_opt(2018, 1, 1).unwrap();
            let query = select(sql::<Date>("'2018-1-1'::date").eq(january_first_2018));
            assert!(query.get_result::<bool>(connection).unwrap());

            let distant_future = NaiveDate::from_ymd_opt(72_400, 1, 8).unwrap();
            let query = select(sql::<Date>("'72400-1-8'::date").eq(distant_future));
            assert!(query.get_result::<bool>(connection).unwrap());
        }

        #[test]
        fn dates_decode_correctly() {
            let connection = &mut connection();
            let january_first_2000 = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
            let query = select(sql::<Date>("'2000-1-1'::date"));
            assert_eq!(
                Ok(january_first_2000),
                query.get_result::<NaiveDate>(connection)
            );

            let distant_past = NaiveDate::from_ymd_opt(-398, 4, 11).unwrap();
            let query = select(sql::<Date>("'399-4-11 BC'::date"));
            assert_eq!(Ok(distant_past), query.get_result::<NaiveDate>(connection));

            let julian_epoch = NaiveDate::from_ymd_opt(-4713, 11, 24).unwrap();
            let query = select(sql::<Date>("'J0'::date"));
            assert_eq!(Ok(julian_epoch), query.get_result::<NaiveDate>(connection));

            let max_date = NaiveDate::from_ymd_opt(262142, 12, 31).unwrap();
            let query = select(sql::<Date>("'262142-12-31'::date"));
            assert_eq!(Ok(max_date), query.get_result::<NaiveDate>(connection));

            let january_first_2018 = NaiveDate::from_ymd_opt(2018, 1, 1).unwrap();
            let query = select(sql::<Date>("'2018-1-1'::date"));
            assert_eq!(
                Ok(january_first_2018),
                query.get_result::<NaiveDate>(connection)
            );

            let distant_future = NaiveDate::from_ymd_opt(72_400, 1, 8).unwrap();
            let query = select(sql::<Date>("'72400-1-8'::date"));
            assert_eq!(
                Ok(distant_future),
                query.get_result::<NaiveDate>(connection)
            );
        }

        /// Get test duration and corresponding literal SQL strings.
        fn get_test_duration_and_literal_strings() -> (Duration, Vec<&'static str>) {
            (
                Duration::days(60) + Duration::minutes(1) + Duration::microseconds(123456),
                vec![
                    "60 days 1 minute 123456 microseconds",
                    "2 months 1 minute 123456 microseconds",
                    "5184060 seconds 123456 microseconds",
                    "60 days 60123456 microseconds",
                    "59 days 24 hours 60.123456 seconds",
                    "60 0:01:00.123456",
                    "58 48:01:00.123456",
                    "P0Y2M0DT0H1M0.123456S",
                    "0-2 0:01:00.123456",
                    "P0000-02-00T00:01:00.123456",
                    "1440:01:00.123456",
                    "1 month 30 days 0.5 minutes 30.123456 seconds",
                ],
            )
        }

        #[test]
        fn duration_encode_correctly() {
            let connection = &mut connection();
            let (duration, literal_strings) = get_test_duration_and_literal_strings();
            for literal in literal_strings {
                let query = select(sql::<Interval>(&format!("'{}'::interval", literal)).eq(duration));
                assert!(query.get_result::<bool>(connection).unwrap());
            }
        }

        #[test]
        fn duration_decode_correctly() {
            let connection = &mut connection();
            let (duration, literal_strings) = get_test_duration_and_literal_strings();
            for literal in literal_strings {
                let query = select(sql::<Interval>(&format!("'{}'::interval", literal)));
                assert_eq!(Ok(duration), query.get_result::<Duration>(connection));
            }
    }

        */
}
