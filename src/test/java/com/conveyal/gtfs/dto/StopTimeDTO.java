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

    public static StopTimeDTO create() {
        StopTimeDTO stopTimeDTO = new StopTimeDTO();
        stopTimeDTO.stop_id = "stop-id-1";
        stopTimeDTO.arrival_time = 0;
        stopTimeDTO.departure_time = 0;
        stopTimeDTO.stop_sequence = 0;
        return stopTimeDTO;
    }
}
