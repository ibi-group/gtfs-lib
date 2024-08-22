package com.conveyal.gtfs.dto;

public class PatternStopDTO {

    // Unique row id
    public Integer id;

    // Pattern Halt
    public String pattern_id;
    public Integer stop_sequence;

    // Pattern Stop
    public String stop_id;
    public int default_travel_time;
    public int default_dwell_time;
    public double shape_dist_traveled;
    public int pickup_type;
    public int drop_off_type;
    public int timepoint;
    public String stop_headsign;
    public int continuous_pickup;
    public int continuous_drop_off;

    /** Empty constructor for deserialization */
    public PatternStopDTO() {}

    public PatternStopDTO(String patternId, String stopId, int defaultTravelTime, int stopSequence) {
        pattern_id = patternId;
        stop_id = stopId;
        stop_sequence = stopSequence;
        default_travel_time = defaultTravelTime;
    }
}
