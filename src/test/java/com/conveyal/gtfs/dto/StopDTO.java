package com.conveyal.gtfs.dto;

public class StopDTO {
    public Integer id;
    public String stop_id;
    public String location_group_id;
    public String location_id;
    public String stop_name;
    public String stop_code;
    public String stop_desc;
    public Double stop_lon;
    public Double stop_lat;
    public String zone_id;
    public String stop_url;
    public String stop_timezone;
    public String parent_station;
    public Integer location_type;
    public Integer wheelchair_boarding;
    public String platform_code;

    public static StopDTO create(String stopId, String stopName, double latitude, double longitude) {
        StopDTO stopDTO = new StopDTO();
        stopDTO.stop_id = stopId;
        stopDTO.stop_name = stopName;
        stopDTO.stop_lat = latitude;
        stopDTO.stop_lon = longitude;
        return stopDTO;
    }
}
