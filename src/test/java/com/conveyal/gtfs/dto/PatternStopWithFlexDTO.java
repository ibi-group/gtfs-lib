package com.conveyal.gtfs.dto;

public class PatternStopWithFlexDTO extends PatternStopDTO {

    // Flex additions.
    public String location_group_id;
    public String location_id;
    public Integer start_pickup_drop_off_window;
    public Integer end_pickup_drop_off_window;
    public String pickup_booking_rule_id;
    public String drop_off_booking_rule_id;

    /** Empty constructor for deserialization */
    public PatternStopWithFlexDTO() {}

    public PatternStopWithFlexDTO(String patternId, String stopId, int defaultTravelTime, int stopSequence) {
        pattern_id = patternId;
        stop_id = stopId;
        stop_sequence = stopSequence;
        default_travel_time = defaultTravelTime;
    }

    public PatternStopWithFlexDTO(String patternId, String stopId, int stopSequence) {
        pattern_id = patternId;
        stop_id = stopId;
        stop_sequence = stopSequence;
    }

    public PatternStopWithFlexDTO(String patternId, String stopId, int stopSequence, int defaultTravelTime, int defaultDwellTime) {
        this(patternId, stopId, defaultTravelTime, stopSequence);
        default_dwell_time = defaultDwellTime;
    }

    public PatternStopWithFlexDTO(String patternId, String stopId, int stopSequence, int timePoint, double shapeDistTraveled) {
        timepoint = timePoint;
        pattern_id = patternId;
        stop_id = stopId;
        stop_sequence = stopSequence;
        shape_dist_traveled = shapeDistTraveled;
    }

    /**
     * Used to create a location group or location pattern stop.
     */
    public static PatternStopWithFlexDTO createFlexPatternStop(
        String patternId,
        String locationGroupId,
        String locationId,
        int stopSequence,
        int flexDefaultTravelTime,
        int flexDefaultZoneTime
    ) {
        PatternStopWithFlexDTO patternStopWithFlexDTO = new PatternStopWithFlexDTO();
        patternStopWithFlexDTO.pattern_id = patternId;
        patternStopWithFlexDTO.location_group_id = locationGroupId;
        patternStopWithFlexDTO.location_id = locationId;
        patternStopWithFlexDTO.stop_sequence = stopSequence;
        patternStopWithFlexDTO.default_travel_time = flexDefaultTravelTime;
        patternStopWithFlexDTO.default_dwell_time = flexDefaultZoneTime;
        return patternStopWithFlexDTO;
    }

    /**
     * Used to create a location group or location pattern stop.
     */
    public static PatternStopWithFlexDTO createFlexPatternStop(
        String patternId,
        String locationGroupId,
        String locationId,
        int stopSequence,
        int flexDefaultTravelTime
    ) {
        return createFlexPatternStop(patternId, locationGroupId, locationId, stopSequence, flexDefaultTravelTime, 0);
    }
}
