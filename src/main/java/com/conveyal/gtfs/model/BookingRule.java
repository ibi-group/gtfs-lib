package com.conveyal.gtfs.model;

import com.conveyal.gtfs.GTFSFeed;

import java.io.IOException;
import java.net.URL;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Objects;

public class BookingRule extends Entity {

    private static final long serialVersionUID = -3961639608144161095L;

    public String booking_rule_id;
    public int booking_type = INT_MISSING;
    public int prior_notice_duration_min;
    public int prior_notice_duration_max;
    public int prior_notice_last_day;
    public String prior_notice_last_time;
    public int prior_notice_start_day;
    public String prior_notice_start_time;
    public String prior_notice_service_id;
    public String message;
    public String pickup_message;
    public String drop_off_message;
    public String phone_number;
    public URL info_url;
    public URL booking_url;

    public static final String TABLE_NAME = "booking_rules";
    public static final String BOOKING_RULE_ID_NAME = "booking_rule_id";
    public static final String BOOKING_TYPE_NAME = "booking_type";
    public static final String PRIOR_NOTICE_DURATION_MIN_NAME = "prior_notice_duration_min";
    public static final String PRIOR_NOTICE_DURATION_MAX_NAME = "prior_notice_duration_max";
    public static final String PRIOR_NOTICE_LAST_DAY_NAME = "prior_notice_last_day";
    public static final String PRIOR_NOTICE_LAST_TIME_NAME = "prior_notice_last_time";
    public static final String PRIOR_NOTICE_START_DAY_NAME = "prior_notice_start_day";
    public static final String PRIOR_NOTICE_START_TIME_NAME = "prior_notice_start_time";
    public static final String PRIOR_NOTICE_SERVICE_ID_NAME = "prior_notice_service_id";
    public static final String MESSAGE_NAME = "message";
    public static final String PICKUP_MESSAGE_NAME = "pickup_message";
    public static final String DROP_OFF_MESSAGE_NAME = "drop_off_message";
    public static final String PHONE_NUMBER_NAME = "phone_number";
    public static final String INFO_URL_NAME = "info_url";
    public static final String BOOKING_URL_NAME = "booking_url";

    @Override
    public String getId() {
        return booking_rule_id;
    }

    /**
     * Sets the parameters for a prepared statement following the parameter order defined in
     * {@link com.conveyal.gtfs.loader.Table#BOOKING_RULES}. JDBC prepared statement parameters use a one-based index.
     */
    @Override
    public void setStatementParameters(PreparedStatement statement, boolean setDefaultId) throws SQLException {
        int oneBasedIndex = 1;
        if (!setDefaultId) statement.setInt(oneBasedIndex++, id);
        statement.setString(oneBasedIndex++, booking_rule_id);
        setIntParameter(statement, oneBasedIndex++, booking_type);
        setIntParameter(statement, oneBasedIndex++, prior_notice_duration_min);
        setIntParameter(statement, oneBasedIndex++, prior_notice_duration_max);
        setIntParameter(statement, oneBasedIndex++, prior_notice_last_day);
        statement.setString(oneBasedIndex++, prior_notice_last_time);
        setIntParameter(statement, oneBasedIndex++, prior_notice_start_day);
        statement.setString(oneBasedIndex++, prior_notice_start_time);
        statement.setString(oneBasedIndex++, prior_notice_service_id);
        statement.setString(oneBasedIndex++, message);
        statement.setString(oneBasedIndex++, pickup_message);
        statement.setString(oneBasedIndex++, drop_off_message);
        statement.setString(oneBasedIndex++, phone_number);
        statement.setString(oneBasedIndex++, info_url != null ? info_url.toString() : null);
        statement.setString(oneBasedIndex, booking_url != null ? booking_url.toString() : null);
    }

    public static class Loader extends Entity.Loader<BookingRule> {

        public Loader(GTFSFeed feed) {
            super(feed, TABLE_NAME);
        }

        @Override
        protected boolean isRequired() {
            return false;
        }

        @Override
        public void loadOneRow() throws IOException {
            BookingRule bookingRule = new BookingRule();
            bookingRule.id = row + 1; // offset line number by 1 to account for 0-based row index
            bookingRule.booking_rule_id = getStringField(BOOKING_RULE_ID_NAME, true);
            bookingRule.booking_type = getIntField(BOOKING_TYPE_NAME, true, 0, 2);
            bookingRule.prior_notice_duration_min = getIntField(PRIOR_NOTICE_DURATION_MIN_NAME, false, 0, Integer.MAX_VALUE);
            bookingRule.prior_notice_duration_max = getIntField(PRIOR_NOTICE_DURATION_MAX_NAME, false, 0, Integer.MAX_VALUE);
            bookingRule.prior_notice_last_day = getIntField(PRIOR_NOTICE_LAST_DAY_NAME, false, 0, Integer.MAX_VALUE);
            bookingRule.prior_notice_last_time = getStringField(PRIOR_NOTICE_LAST_TIME_NAME, false);
            bookingRule.prior_notice_start_day = getIntField(PRIOR_NOTICE_START_DAY_NAME, false, 0, Integer.MAX_VALUE);
            bookingRule.prior_notice_start_time = getStringField(PRIOR_NOTICE_START_TIME_NAME, false);
            bookingRule.prior_notice_service_id = getStringField(PRIOR_NOTICE_SERVICE_ID_NAME, false);
            bookingRule.message = getStringField(MESSAGE_NAME, false);
            bookingRule.pickup_message = getStringField(PICKUP_MESSAGE_NAME, false);
            bookingRule.drop_off_message = getStringField(DROP_OFF_MESSAGE_NAME, false);
            bookingRule.phone_number = getStringField(PHONE_NUMBER_NAME, false);
            bookingRule.info_url = getUrlField(INFO_URL_NAME, false);
            bookingRule.booking_url = getUrlField(BOOKING_URL_NAME, false);

            // Attempting to put a null key or value will cause an NPE in BTreeMap
            if (bookingRule.booking_rule_id != null) {
                feed.bookingRules.put(bookingRule.booking_rule_id, bookingRule);
            }

            /*
              Check referential integrity without storing references. BookingRule cannot directly reference Calendars
              because they would be serialized into the MapDB.
             */
            getRefField(PRIOR_NOTICE_SERVICE_ID_NAME, bookingRule.prior_notice_service_id != null, feed.calendars);
        }
    }

    public static class Writer extends Entity.Writer<BookingRule> {
        public Writer(GTFSFeed feed) {
            super(feed, TABLE_NAME);
        }

        @Override
        public void writeHeaders() throws IOException {
            writer.writeRecord(new String[]{
                BOOKING_RULE_ID_NAME,
                BOOKING_TYPE_NAME,
                PRIOR_NOTICE_DURATION_MIN_NAME,
                PRIOR_NOTICE_DURATION_MAX_NAME,
                PRIOR_NOTICE_LAST_DAY_NAME,
                PRIOR_NOTICE_LAST_TIME_NAME,
                PRIOR_NOTICE_START_DAY_NAME,
                PRIOR_NOTICE_START_TIME_NAME,
                PRIOR_NOTICE_SERVICE_ID_NAME,
                MESSAGE_NAME,
                PICKUP_MESSAGE_NAME,
                DROP_OFF_MESSAGE_NAME,
                PHONE_NUMBER_NAME,
                INFO_URL_NAME,
                BOOKING_URL_NAME
            });
        }

        @Override
        public void writeOneRow(BookingRule bookingRule) throws IOException {
            writeStringField(bookingRule.booking_rule_id);
            writeIntField(bookingRule.booking_type);
            writeIntField(bookingRule.prior_notice_duration_min);
            writeIntField(bookingRule.prior_notice_duration_max);
            writeIntField(bookingRule.prior_notice_last_day);
            writeStringField(bookingRule.prior_notice_last_time);
            writeIntField(bookingRule.prior_notice_start_day);
            writeStringField(bookingRule.prior_notice_start_time);
            writeStringField(bookingRule.prior_notice_service_id);
            writeStringField(bookingRule.message);
            writeStringField(bookingRule.pickup_message);
            writeStringField(bookingRule.drop_off_message);
            writeStringField(bookingRule.phone_number);
            writeUrlField(bookingRule.info_url);
            writeUrlField(bookingRule.booking_url);
            endRecord();
        }

        @Override
        public Iterator<BookingRule> iterator() {
            return this.feed.bookingRules.values().iterator();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BookingRule that = (BookingRule) o;
        return booking_type == that.booking_type &&
            prior_notice_duration_min == that.prior_notice_duration_min &&
            prior_notice_duration_max == that.prior_notice_duration_max &&
            prior_notice_last_day == that.prior_notice_last_day &&
            Objects.equals(prior_notice_last_time, that.prior_notice_last_time) &&
            prior_notice_start_day == that.prior_notice_start_day &&
            Objects.equals(prior_notice_start_time, that.prior_notice_start_time) &&
            Objects.equals(booking_rule_id, that.booking_rule_id) &&
            Objects.equals(prior_notice_service_id, that.prior_notice_service_id) &&
            Objects.equals(message, that.message) &&
            Objects.equals(pickup_message, that.pickup_message) &&
            Objects.equals(drop_off_message, that.drop_off_message) &&
            Objects.equals(phone_number, that.phone_number) &&
            Objects.equals(info_url, that.info_url) &&
            Objects.equals(booking_url, that.booking_url);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            booking_rule_id,
            booking_type,
            prior_notice_duration_min,
            prior_notice_duration_max,
            prior_notice_last_day,
            prior_notice_last_time,
            prior_notice_start_day,
            prior_notice_start_time,
            prior_notice_service_id,
            message,
            pickup_message,
            drop_off_message,
            phone_number,
            info_url,
            booking_url
        );
    }
}
