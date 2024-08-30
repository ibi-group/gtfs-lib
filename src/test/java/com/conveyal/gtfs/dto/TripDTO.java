package com.conveyal.gtfs.dto;

public class TripDTO {
    public Integer id;
    public String trip_id;
    public String trip_headsign;
    public String trip_short_name;
    public String block_id;
    public Integer direction_id;
    public String route_id;
    public String service_id;
    public Integer wheelchair_accessible;
    public Integer bikes_allowed;
    public String shape_id;
    public String pattern_id;
    public StopTimeWithFlexDTO[] stop_times;
    public FrequencyDTO[] frequencies;

    public static TripDTO create(String patternId, String routeId, StopTimeWithFlexDTO[] stopTimes) {
        TripDTO tripDTO = new TripDTO();
        tripDTO.pattern_id = patternId;
        tripDTO.route_id = routeId;
        tripDTO.service_id = "1";
        tripDTO.stop_times = stopTimes;
        tripDTO.frequencies = new FrequencyDTO[] {};
        return tripDTO;
    }

    public static TripDTO create(String patternId, String routeId, int startTime) {
        TripDTO tripInput = new TripDTO();
        tripInput.pattern_id = patternId;
        tripInput.route_id = routeId;
        tripInput.service_id = "1";
        tripInput.stop_times = new StopTimeWithFlexDTO[] {
            new StopTimeWithFlexDTO("1", 0, 0, 0),
            new StopTimeWithFlexDTO("2", 60, 60, 1)
        };
        FrequencyDTO frequency = new FrequencyDTO();
        frequency.start_time = startTime;
        frequency.end_time = 9 * 60 * 60;
        frequency.headway_secs = 15 * 60;
        tripInput.frequencies = new FrequencyDTO[] {frequency};
        return tripInput;
    }
}
