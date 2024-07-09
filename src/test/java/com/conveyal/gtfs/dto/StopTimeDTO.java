package com.conveyal.gtfs.dto;

public class StopTimeDTO {
    public int id;
    public String trip_id;
    public String stop_id;
    public String location_group_id;
    public String location_id;
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

    /**
     * Empty constructor for deserialization
     */
    public StopTimeDTO() {
    }

    /**
     * Used to create a stop time which references a stop.
     */
    public StopTimeDTO(String stopId, Integer arrivalTime, Integer departureTime, Integer stopSequence) {
        stop_id = stopId;
        arrival_time = arrivalTime;
        departure_time = departureTime;
        stop_sequence = stopSequence;
    }
}
