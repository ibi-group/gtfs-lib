package com.conveyal.gtfs.dto;

import com.conveyal.gtfs.model.PatternLocationGroupStop;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * DTO used to model expected {@link PatternLocationGroupStop} JSON structure for the editor. NOTE:
 * reference types (e.g., Integer and Double) are used here in order to model null/empty values in JSON object.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class PatternLocationGroupStopDTO {
    public int id;

    // PatternHalt params
    public String pattern_id;
    public int stop_sequence;

    // PatternLocationGroupStop params
    public String location_group_id;
    public int pickup_type;
    public int drop_off_type;
    public int timepoint;
    public String stop_headsign;
    public int continuous_pickup;
    public int continuous_drop_off;

    public String pickup_booking_rule_id;
    public String drop_off_booking_rule_id;

    public int flex_default_travel_time;
    public int flex_default_zone_time;

    /** Empty constructor for deserialization */
    public PatternLocationGroupStopDTO() {}

    public PatternLocationGroupStopDTO(
        String patternId,
        String locationGroupId,
        int stopSequence,
        int flexDefaultTravelTime,
        int flexDefaultZoneTime
    ) {
        this.pattern_id = patternId;
        this.location_group_id = locationGroupId;
        this.stop_sequence = stopSequence;
        this.flex_default_travel_time = flexDefaultTravelTime;
        this.flex_default_zone_time = flexDefaultZoneTime;
    }

    public PatternLocationGroupStopDTO(String patternId, String locationGroupId, int stopSequence) {
        this.pattern_id = patternId;
        this.location_group_id = locationGroupId;
        this.stop_sequence = stopSequence;
    }
}
