package com.conveyal.gtfs.model;

import com.conveyal.gtfs.GTFSFeed;
import org.mapdb.Fun;
import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * Represents a GTFS StopTime. Note that once created and saved in a feed, stop times are by convention immutable
 * because they are in a MapDB.
 */
public class StopTime extends Entity implements Cloneable, Serializable {

    private static final long serialVersionUID = -8883780047901081832L;
    /* StopTime cannot directly reference Trips or Stops because they would be serialized into the MapDB. */
    public String trip_id;
    public int    arrival_time = INT_MISSING;
    public int    departure_time = INT_MISSING;
    public String stop_id;
    public int    stop_sequence;
    public String stop_headsign;
    public int    pickup_type;
    public int    drop_off_type;
    public int    continuous_pickup = INT_MISSING;
    public int    continuous_drop_off = INT_MISSING;
    public double shape_dist_traveled = DOUBLE_MISSING;
    public int    timepoint = INT_MISSING;

    // Additional GTFS Flex fields.
    public String location_group_id;
    public String location_id;
    public String pickup_booking_rule_id;
    public String drop_off_booking_rule_id;
    public int start_pickup_drop_off_window = INT_MISSING;
    public int end_pickup_drop_off_window = INT_MISSING;

    public static final String TABLE_NAME = "stop_times";
    public static final String TRIP_ID_NAME = "trip_id";
    public static final String ARRIVAL_TIME_NAME = "arrival_time";
    public static final String DEPARTURE_TIME_NAME = "departure_time";
    public static final String STOP_ID_NAME = "stop_id";
    public static final String STOP_SEQUENCE_NAME = "stop_sequence";
    public static final String STOP_HEADSIGN_NAME = "stop_headsign";
    public static final String PICKUP_TYPE_NAME = "pickup_type";
    public static final String DROP_OFF_TYPE_NAME = "drop_off_type";
    public static final String CONTINUOUS_PICK_UP_NAME = "continuous_pickup";
    public static final String CONTINUOUS_DROP_OFF_NAME = "continuous_drop_off";
    public static final String SHAPE_DIST_TRAVELED_NAME = "shape_dist_traveled";
    public static final String TIMEPOINT_NAME = "timepoint";
    public static final String LOCATION_GROUP_ID_NAME = "location_group_id";
    public static final String LOCATION_ID_NAME = "location_id";
    public static final String PICKUP_BOOKING_RULE_ID_NAME = "pickup_booking_rule_id";
    public static final String DROP_OFF_BOOKING_RULE_ID_NAME = "drop_off_booking_rule_id";
    public static final String START_PICKUP_DROP_OFF_WINDOW_NAME = "start_pickup_drop_off_window";
    public static final String END_PICKUP_DROP_OFF_WINDOW_NAME = "end_pickup_drop_off_window";


    @Override
    public String getId() {
        return trip_id; // Needs sequence number to be unique
    }

    @Override
    public Integer getSequenceNumber() {
        return stop_sequence; // Compound key of StopTime is (trip_id, stop_sequence)
    }

    /**
     * Sets the parameters for a prepared statement following the parameter order defined in
     * {@link com.conveyal.gtfs.loader.Table#STOP_TIMES}. JDBC prepared statement parameters use a one-based index.
     */
    @Override
    public void setStatementParameters(PreparedStatement statement, boolean setDefaultId) throws SQLException {
        int oneBasedIndex = 1;
        if (!setDefaultId) statement.setInt(oneBasedIndex++, id);
        statement.setString(oneBasedIndex++, trip_id);
        setIntParameter(statement, oneBasedIndex++, stop_sequence);
        statement.setString(oneBasedIndex++, stop_id);
        statement.setString(oneBasedIndex++, location_group_id);
        statement.setString(oneBasedIndex++, location_id);
        setIntParameter(statement, oneBasedIndex++, arrival_time);
        setIntParameter(statement, oneBasedIndex++, departure_time);
        statement.setString(oneBasedIndex++, stop_headsign);
        setIntParameter(statement, oneBasedIndex++, pickup_type);
        setIntParameter(statement, oneBasedIndex++, drop_off_type);
        setIntParameter(statement, oneBasedIndex++, continuous_pickup);
        setIntParameter(statement, oneBasedIndex++, continuous_drop_off);
        statement.setDouble(oneBasedIndex++, shape_dist_traveled);
        setIntParameter(statement, oneBasedIndex++, timepoint);
        statement.setString(oneBasedIndex++, pickup_booking_rule_id);
        statement.setString(oneBasedIndex++, drop_off_booking_rule_id);
        setIntParameter(statement, oneBasedIndex++, start_pickup_drop_off_window);
        setIntParameter(statement, oneBasedIndex, end_pickup_drop_off_window);
    }

    public static class Loader extends Entity.Loader<StopTime> {

        public Loader(GTFSFeed feed) {
            super(feed, TABLE_NAME);
        }

        @Override
        protected boolean isRequired() {
            return true;
        }

        @Override
        public void loadOneRow() throws IOException {
            StopTime st = new StopTime();
            st.id = row + 1; // offset line number by 1 to account for 0-based row index
            st.trip_id = getStringField(TRIP_ID_NAME, true);
            // TODO: arrival_time and departure time are not required, but if one is present the other should be
            // also, if this is the first or last stop, they are both required
            st.arrival_time = getTimeField(ARRIVAL_TIME_NAME, false);
            st.departure_time = getTimeField(DEPARTURE_TIME_NAME, false);
            st.stop_id = getStringField(STOP_ID_NAME, false);
            st.stop_sequence = getIntField(STOP_SEQUENCE_NAME, true, 0, Integer.MAX_VALUE);
            st.stop_headsign = getStringField(STOP_HEADSIGN_NAME, false);
            st.pickup_type = getIntField(PICKUP_TYPE_NAME, false, 0, 3); // TODO add ranges as parameters
            st.drop_off_type = getIntField(DROP_OFF_TYPE_NAME, false, 0, 3);
            st.continuous_pickup = getIntField(CONTINUOUS_PICK_UP_NAME, false, 0, 3, INT_MISSING);
            st.continuous_drop_off = getIntField(CONTINUOUS_DROP_OFF_NAME, false, 0, 3, INT_MISSING);
            st.shape_dist_traveled = getDoubleField(SHAPE_DIST_TRAVELED_NAME, false, 0D, Double.MAX_VALUE); // FIXME using both 0 and NaN for "missing", define DOUBLE_MISSING
            st.timepoint = getIntField(TIMEPOINT_NAME, false, 0, 1, INT_MISSING);
            if (feed.isGTFSFlexFeed()) {
                st.location_group_id = getStringField(LOCATION_GROUP_ID_NAME, false);
                st.location_id = getStringField(LOCATION_ID_NAME, false);
                st.pickup_booking_rule_id = getStringField(PICKUP_BOOKING_RULE_ID_NAME, false);
                st.drop_off_booking_rule_id = getStringField(DROP_OFF_BOOKING_RULE_ID_NAME, false);
                st.start_pickup_drop_off_window = getTimeField(START_PICKUP_DROP_OFF_WINDOW_NAME, false);
                st.end_pickup_drop_off_window = getTimeField(END_PICKUP_DROP_OFF_WINDOW_NAME, false);
            }
            st.feed = null; // this could circular-serialize the whole feed
            feed.stop_times.put(new Fun.Tuple2(st.trip_id, st.stop_sequence), st);

            /*
              Check referential integrity without storing references. StopTime cannot directly reference foreign tables
              because they would be serialized into the MapDB.
             */
            getRefField(TRIP_ID_NAME, true, feed.trips);
            getRefField(STOP_ID_NAME, st.stop_id != null, feed.stops);
            getRefField(LOCATION_GROUP_ID_NAME, st.location_group_id != null, feed.locationGroup);
            getRefField(LOCATION_ID_NAME, st.location_id != null, feed.locations);
        }

    }

    public static class Writer extends Entity.Writer<StopTime> {
        public Writer (GTFSFeed feed) {
            super(feed, TABLE_NAME);
        }

        /**
         * This is the only table which has a mixture of original GTFS values and GTFS Flex values. If the feed does not
         * include GTFS Flex data, the additional headers are not required.
         */
        @Override
        protected void writeHeaders() throws IOException {
            if (feed.isGTFSFlexFeed()) {
                writer.writeRecord(new String[] {
                    TRIP_ID_NAME,
                    ARRIVAL_TIME_NAME,
                    DEPARTURE_TIME_NAME,
                    STOP_ID_NAME,
                    LOCATION_GROUP_ID_NAME,
                    LOCATION_ID_NAME,
                    STOP_SEQUENCE_NAME,
                    STOP_HEADSIGN_NAME,
                    START_PICKUP_DROP_OFF_WINDOW_NAME,
                    END_PICKUP_DROP_OFF_WINDOW_NAME,
                    PICKUP_TYPE_NAME,
                    DROP_OFF_TYPE_NAME,
                    CONTINUOUS_PICK_UP_NAME,
                    CONTINUOUS_DROP_OFF_NAME,
                    SHAPE_DIST_TRAVELED_NAME,
                    TIMEPOINT_NAME,
                    PICKUP_BOOKING_RULE_ID_NAME,
                    DROP_OFF_BOOKING_RULE_ID_NAME
                });
            } else {
                writer.writeRecord(new String[] {
                    TRIP_ID_NAME,
                    ARRIVAL_TIME_NAME,
                    DEPARTURE_TIME_NAME,
                    STOP_ID_NAME,
                    STOP_SEQUENCE_NAME,
                    STOP_HEADSIGN_NAME,
                    PICKUP_TYPE_NAME,
                    DROP_OFF_TYPE_NAME,
                    CONTINUOUS_PICK_UP_NAME,
                    CONTINUOUS_DROP_OFF_NAME,
                    SHAPE_DIST_TRAVELED_NAME,
                    TIMEPOINT_NAME
                });
            }
        }

        /**
         * Only include the flex fields if this is a GTFS Flex feed.
         */
        @Override
        protected void writeOneRow(StopTime st) throws IOException {
            writeStringField(st.trip_id);
            writeTimeField(st.arrival_time);
            writeTimeField(st.departure_time);
            writeStringField(st.stop_id);
            if (feed.isGTFSFlexFeed()) {
                writeStringField(st.location_group_id);
                writeStringField(st.location_id);
            }
            writeIntField(st.stop_sequence);
            writeStringField(st.stop_headsign);
            if (feed.isGTFSFlexFeed()) {
                writeTimeField(st.start_pickup_drop_off_window);
                writeTimeField(st.end_pickup_drop_off_window);
            }
            writeIntField(st.pickup_type);
            writeIntField(st.drop_off_type);
            writeIntField(st.continuous_pickup);
            writeIntField(st.continuous_drop_off);
            writeDoubleField(st.shape_dist_traveled);
            writeIntField(st.timepoint);
            if (feed.isGTFSFlexFeed()) {
                writeStringField(st.pickup_booking_rule_id);
                writeStringField(st.drop_off_booking_rule_id);
            }
            endRecord();
        }

        @Override
        protected Iterator<StopTime> iterator() {
            return feed.stop_times.values().iterator();
        }

    }

    /**
     * Check that the flex columns exist. This is to guard against cases where booking rules, location
     * group stops or locations are defined in a feed but flex specific stop time columns are not.
     */
    private static boolean flexColumnsExist(Connection connection, String tablePrefix) throws SQLException {
        boolean exists = false;
        String sql = String.format(
            "SELECT EXISTS (SELECT 1 " +
            "FROM information_schema.columns " +
            "WHERE table_schema='%s' " +
            "AND table_name='%s' " +
            "AND column_name IN ('%s', '%s', '%s', '%s', '%s', '%s'))",
            tablePrefix.replace(".", ""),
            TABLE_NAME,
            LOCATION_GROUP_ID_NAME,
            LOCATION_ID_NAME,
            PICKUP_BOOKING_RULE_ID_NAME,
            DROP_OFF_BOOKING_RULE_ID_NAME,
            START_PICKUP_DROP_OFF_WINDOW_NAME,
            END_PICKUP_DROP_OFF_WINDOW_NAME
        );
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                exists = resultSet.getBoolean(1);
            }
        }
        return exists;
    }

    /**
     * Extract stop times which are required for flex validation. To qualify, one of the flex fields must not be null.
     * To match the expected import values, where applicable, integers and doubles that are null are set to INT_MISSING
     * and DOUBLE_MISSING respectively.
     */
    public static List<StopTime> getFlexStopTimesForValidation(Connection connection, String tablePrefix) throws SQLException {
        List<StopTime> stopTimes = new ArrayList<>();
        if (!flexColumnsExist(connection, tablePrefix)) {
            return stopTimes;
        }
        String sql = String.format(
            "select id, %s, %s, %s, %s, %s, %s, %s, " +
            "%s, %s, %s " +
            "from %s%s where " +
            "%s IS NOT NULL " +
            "or %s IS NOT NULL ",
            TRIP_ID_NAME,
            STOP_ID_NAME,
            LOCATION_GROUP_ID_NAME,
            LOCATION_ID_NAME,
            ARRIVAL_TIME_NAME,
            DEPARTURE_TIME_NAME,
            PICKUP_TYPE_NAME,
            DROP_OFF_TYPE_NAME,
            START_PICKUP_DROP_OFF_WINDOW_NAME,
            END_PICKUP_DROP_OFF_WINDOW_NAME,
            tablePrefix,
            TABLE_NAME,
            START_PICKUP_DROP_OFF_WINDOW_NAME,
            END_PICKUP_DROP_OFF_WINDOW_NAME
        );
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                StopTime stopTime = new StopTime();
                stopTime.id = resultSet.getInt(1);
                stopTime.trip_id = resultSet.getString(2);
                stopTime.stop_id = resultSet.getString(3);
                stopTime.location_group_id = resultSet.getString(4);
                stopTime.location_id = resultSet.getString(5);
                stopTime.arrival_time = getIntValue(resultSet.getString(6));
                stopTime.departure_time = getIntValue(resultSet.getString(7));
                stopTime.pickup_type = resultSet.getInt(8);
                stopTime.drop_off_type = resultSet.getInt(9);
                stopTime.start_pickup_drop_off_window = getIntValue(resultSet.getString(10));
                stopTime.end_pickup_drop_off_window = getIntValue(resultSet.getString(11));
                stopTimes.add(stopTime);
            }
        }
        return stopTimes;
    }

    @Override
    public StopTime clone () {
        try {
            return (StopTime) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StopTime stopTime = (StopTime) o;
        return
            arrival_time == stopTime.arrival_time &&
            departure_time == stopTime.departure_time &&
            stop_sequence == stopTime.stop_sequence &&
            pickup_type == stopTime.pickup_type &&
            drop_off_type == stopTime.drop_off_type &&
            continuous_pickup == stopTime.continuous_pickup &&
            continuous_drop_off == stopTime.continuous_drop_off &&
            Double.compare(stopTime.shape_dist_traveled, shape_dist_traveled) == 0 &&
            timepoint == stopTime.timepoint &&
            start_pickup_drop_off_window == stopTime.start_pickup_drop_off_window &&
            end_pickup_drop_off_window == stopTime.end_pickup_drop_off_window &&
            Objects.equals(trip_id, stopTime.trip_id) &&
            Objects.equals(stop_id, stopTime.stop_id) &&
            Objects.equals(location_group_id, stopTime.location_group_id) &&
            Objects.equals(location_id, stopTime.location_id) &&
            Objects.equals(stop_headsign, stopTime.stop_headsign) &&
            Objects.equals(pickup_booking_rule_id, stopTime.pickup_booking_rule_id) &&
            Objects.equals(drop_off_booking_rule_id, stopTime.drop_off_booking_rule_id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            trip_id,
            arrival_time,
            departure_time,
            stop_id,
            location_group_id,
            location_id,
            stop_sequence,
            stop_headsign,
            pickup_type,
            drop_off_type,
            continuous_pickup,
            continuous_drop_off,
            shape_dist_traveled,
            timepoint,
            pickup_booking_rule_id,
            drop_off_booking_rule_id,
            start_pickup_drop_off_window,
            end_pickup_drop_off_window
        );
    }
}
