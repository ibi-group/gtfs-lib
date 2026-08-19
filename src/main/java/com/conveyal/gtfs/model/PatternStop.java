package com.conveyal.gtfs.model;

import com.conveyal.gtfs.TripPatternKey;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * A pattern stop represents generalized information about a stop visited by a pattern, i.e. a collection of trips that
 * all visit the same stops in the same sequence. Some of these characteristics, e.g., stop ID, stop sequence, pickup
 * type, and drop off type, help determine a unique pattern. Others (default dwell/travel time, timepoint, and shape dist
 * traveled) are specific to the editor and usually based on values from the first trip encountered in a feed for a
 * given pattern.
 */
public class PatternStop extends PatternHalt {
    private static final long serialVersionUID = 1L;

    public String stop_id;
    // FIXME: Should we be storing default travel and dwell times here?
    public int default_travel_time;
    public int default_dwell_time;
    public double shape_dist_traveled;
    public int pickup_type;
    public int drop_off_type;
    public int timepoint;
    public String stop_headsign;
    public int continuous_pickup = INT_MISSING;
    public int continuous_drop_off = INT_MISSING;

    // Flex additions.
    public String location_group_id;
    public String location_id;
    public String pickup_booking_rule_id;
    public String drop_off_booking_rule_id;
    public int start_pickup_drop_off_window = INT_MISSING;
    public int end_pickup_drop_off_window = INT_MISSING;

    public PatternStop () {}
    public PatternStop (String patternId, int stopSequence, TripPatternKey tripPattern, int travelTime, int timeInLocation) {
        pattern_id = patternId;
        stop_sequence = stopSequence;
        stop_id = tripPattern.stops.get(stopSequence);
        location_group_id = tripPattern.locationGroupIds.get(stopSequence);
        location_id = tripPattern.locationIds.get(stopSequence);
        stop_headsign = tripPattern.stopHeadsigns.get(stopSequence);
        default_travel_time = travelTime;
        default_dwell_time = timeInLocation;
        drop_off_type = tripPattern.dropoffTypes.get(stopSequence);
        pickup_type = tripPattern.pickupTypes.get(stopSequence);
        shape_dist_traveled = tripPattern.shapeDistances.get(stopSequence);
        timepoint = tripPattern.timepoints.get(stopSequence);
        continuous_pickup = tripPattern.continuous_pickup.get(stopSequence);
        continuous_drop_off = tripPattern.continuous_drop_off.get(stopSequence);
        pickup_booking_rule_id = tripPattern.pickup_booking_rule_id.get(stopSequence);
        drop_off_booking_rule_id = tripPattern.drop_off_booking_rule_id.get(stopSequence);
        start_pickup_drop_off_window = tripPattern.start_pickup_drop_off_window.get(stopSequence);
        end_pickup_drop_off_window = tripPattern.end_pickup_drop_off_window.get(stopSequence);
    }

    @Override
    public void setStatementParameters(PreparedStatement statement, boolean setDefaultId) throws SQLException {
        int oneBasedIndex = 1;
        if (!setDefaultId) statement.setInt(oneBasedIndex++, id);
        statement.setString(oneBasedIndex++, pattern_id);
        // Stop sequence is zero-based.
        setIntParameter(statement, oneBasedIndex++, stop_sequence);
        statement.setString(oneBasedIndex++, stop_id);
        statement.setString(oneBasedIndex++, location_group_id);
        statement.setString(oneBasedIndex++, location_id);
        statement.setString(oneBasedIndex++, stop_headsign);
        setIntParameter(statement, oneBasedIndex++, default_travel_time);
        setIntParameter(statement, oneBasedIndex++, default_dwell_time);
        setIntParameter(statement, oneBasedIndex++, drop_off_type);
        setIntParameter(statement, oneBasedIndex++, pickup_type);
        setDoubleParameter(statement, oneBasedIndex++, shape_dist_traveled);
        setIntParameter(statement, oneBasedIndex++, timepoint);
        setIntParameter(statement, oneBasedIndex++, continuous_pickup);
        setIntParameter(statement, oneBasedIndex++, continuous_drop_off);
        statement.setString(oneBasedIndex++, pickup_booking_rule_id);
        statement.setString(oneBasedIndex++, drop_off_booking_rule_id);
        setIntParameter(statement, oneBasedIndex++, start_pickup_drop_off_window);
        setIntParameter(statement, oneBasedIndex, end_pickup_drop_off_window);
    }

    public int getTravelTime() {
        return default_travel_time == Entity.INT_MISSING ? 0 : default_travel_time;
    }

    public int getDwellTime() {
        return default_dwell_time == Entity.INT_MISSING ? 0 : default_dwell_time;
    }


    /**
     * As part of the flex spec, either stop id, location group id or location id can be defined. If one of the latter
     * two are defined, this is a flex pattern stop.
     */
    public boolean isFlex() {
        return location_group_id != null || location_id != null;
    }
}
