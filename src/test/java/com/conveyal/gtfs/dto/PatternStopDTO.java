package com.conveyal.gtfs.dto;

public class PatternStopDTO {
    public Integer id;
    public String pattern_id;
    public String stop_id;
    public Integer default_travel_time;
    public Integer default_dwell_time;
    public Double shape_dist_traveled;
    public Integer drop_off_type;
    public Integer pickup_type;
    public String stop_headsign;
    public Integer stop_sequence;
    public Integer timepoint;
    public Integer continuous_pickup;
    public Integer continuous_drop_off;

    // Flex additions.
    public String location_group_id;
    public String location_id;
    public Integer start_pickup_drop_off_window;
    public Integer end_pickup_drop_off_window;
    public String pickup_booking_rule_id;
    public String drop_off_booking_rule_id;

    /** Empty constructor for deserialization */
    public PatternStopDTO() {}

    public PatternStopDTO (String patternId, String stopId, int defaultTravelTime, int stopSequence) {
        pattern_id = patternId;
        stop_id = stopId;
        stop_sequence = stopSequence;
        default_travel_time = defaultTravelTime;
    }

    public PatternStopDTO (String patternId, String stopId, int stopSequence) {
        pattern_id = patternId;
        stop_id = stopId;
        stop_sequence = stopSequence;
    }
    public PatternStopDTO (String patternId, String stopId, int stopSequence, int defaultTravelTime, int defaultDwellTime) {
        pattern_id = patternId;
        stop_id = stopId;
        stop_sequence = stopSequence;
        default_travel_time = defaultTravelTime;
        default_dwell_time = defaultDwellTime;
    }

    public PatternStopDTO (String patternId, String stopId, int stopSequence, int timePoint, double shape_dist_traveledValue) {
        timepoint = timePoint;
        pattern_id = patternId;
        stop_id = stopId;
        stop_sequence = stopSequence;
        shape_dist_traveled = shape_dist_traveledValue;
    }

    /**
     * Used to create a location group or location pattern stop.
     */
    public static PatternStopDTO createFlexPatternStop(
        String patternId,
        String locationGroupId,
        String locationId,
        int stopSequence,
        int flexDefaultTravelTime,
        int flexDefaultZoneTime
    ) {
        PatternStopDTO patternStopDTO = new PatternStopDTO();
        patternStopDTO.pattern_id = patternId;
        patternStopDTO.location_group_id = locationGroupId;
        patternStopDTO.location_id = locationId;
        patternStopDTO.stop_sequence = stopSequence;
        patternStopDTO.default_travel_time = flexDefaultTravelTime;
        patternStopDTO.default_dwell_time = flexDefaultZoneTime;
        return patternStopDTO;
    }

    /**
     * Used to create a location group or location pattern stop.
     */
    public static PatternStopDTO createFlexPatternStop(
        String patternId,
        String locationGroupId,
        String locationId,
        int stopSequence,
        int flexDefaultTravelTime
    ) {
        PatternStopDTO patternStopDTO = new PatternStopDTO();
        patternStopDTO.pattern_id = patternId;
        patternStopDTO.location_group_id = locationGroupId;
        patternStopDTO.location_id = locationId;
        patternStopDTO.stop_sequence = stopSequence;
        patternStopDTO.default_travel_time = flexDefaultTravelTime;
        return patternStopDTO;
    }
}
