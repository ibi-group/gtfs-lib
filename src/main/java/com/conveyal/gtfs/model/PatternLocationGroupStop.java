package com.conveyal.gtfs.model;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * A pattern location group stop represents generalized information about a location or stop visited by a pattern, i.e. a
 * collection of trips that all visit the same locations/stops in the same sequence. Some of these characteristics,
 * e.g. stop area ID and stop sequence help determine a unique pattern.
 */
public class PatternLocationGroupStop extends PatternHalt {
    private static final long serialVersionUID = 1L;

    public String location_group_id;

    public int pickup_type;
    public int drop_off_type;
    public int timepoint;
    public String stop_headsign;
    public int continuous_pickup = INT_MISSING;
    public int continuous_drop_off = INT_MISSING;

    // Flex additions.
    public String pickup_booking_rule_id;
    public String drop_off_booking_rule_id;

    // Additional GTFS Flex stop area and location fields
    public int flex_default_travel_time = INT_MISSING;
    public int flex_default_zone_time = INT_MISSING;

    public PatternLocationGroupStop() {}

    @Override
    public void setStatementParameters(PreparedStatement statement, boolean setDefaultId) throws SQLException {
        int oneBasedIndex = 1;
        if (!setDefaultId) statement.setInt(oneBasedIndex++, id);
        statement.setString(oneBasedIndex++, pattern_id);
        // Stop sequence is zero-based.
        setIntParameter(statement, oneBasedIndex++, stop_sequence);
        statement.setString(oneBasedIndex++, location_group_id);
        setIntParameter(statement, oneBasedIndex++, drop_off_type);
        setIntParameter(statement, oneBasedIndex++, pickup_type);
        setIntParameter(statement, oneBasedIndex++, timepoint);
        statement.setString(oneBasedIndex++, stop_headsign);
        setIntParameter(statement, oneBasedIndex++, continuous_pickup);
        setIntParameter(statement, oneBasedIndex++, continuous_drop_off);
        statement.setString(oneBasedIndex++, pickup_booking_rule_id);
        statement.setString(oneBasedIndex++, drop_off_booking_rule_id);

        // the derived fields
        setIntParameter(statement, oneBasedIndex++, flex_default_travel_time);
        setIntParameter(statement, oneBasedIndex, flex_default_zone_time);
    }

    public int getTravelTime() {
        return flex_default_travel_time == Entity.INT_MISSING ? 0 : flex_default_travel_time;
    }

    public int getDwellTime() {
        return flex_default_zone_time == Entity.INT_MISSING ? 0 : flex_default_zone_time;
    }

    public boolean isFlex() {
        return true;
    }
}

