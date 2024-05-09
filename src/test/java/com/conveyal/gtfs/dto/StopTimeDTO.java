package com.conveyal.gtfs.dto;

public class StopTimeDTO {
    public int id;
    public String trip_id;
    public String stop_id;
    public Integer stop_sequence;
    public Integer arrival_time;
    public Integer departure_time;
    public String stop_headsign;
    public Integer timepoint;
    public Integer drop_off_type;
    public Integer pickup_type;
    public Double shape_dist_traveled;
    public int continuous_pickup;
    public int continuous_drop_off;

    // Additional GTFS Flex booking rule fields.
    public String pickup_booking_rule_id;
    public String drop_off_booking_rule_id;

    // Additional GTFS Flex location groups and locations fields
    public Integer start_pickup_drop_off_window;
    public Integer end_pickup_drop_off_window;

    /**
     * Empty constructor for deserialization
     */
    public StopTimeDTO() {
    }

    public StopTimeDTO(String stopId, Integer arrivalTime, Integer departureTime, Integer stopSequence) {
        stop_id = stopId;
        arrival_time = arrivalTime;
        departure_time = departureTime;
        stop_sequence = stopSequence;
    }

    public static StopTimeDTO flexStopTime(String stopId, Integer startPickupDropOffWindow, Integer endPickupDropOffWindow, Integer stopSequence) {
        StopTimeDTO stopTimeDTO = new StopTimeDTO();
        stopTimeDTO.stop_id = stopId;
        stopTimeDTO.start_pickup_drop_off_window = startPickupDropOffWindow;
        stopTimeDTO.end_pickup_drop_off_window = endPickupDropOffWindow;
        stopTimeDTO.stop_sequence = stopSequence;
        return stopTimeDTO;
    }
}
