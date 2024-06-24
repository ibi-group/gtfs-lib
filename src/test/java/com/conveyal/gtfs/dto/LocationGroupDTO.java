package com.conveyal.gtfs.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class LocationGroupDTO {
    public int id;
    public String location_group_id;
    public String location_group_name;
}

