package com.conveyal.gtfs.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class LocationGroupDTO {
    public int id;
    public String location_group_id;
    public String location_group_name;

    public static LocationGroupDTO create(String locationGroupId){
        LocationGroupDTO locationGroup = new LocationGroupDTO();
        locationGroup.location_group_id = locationGroupId;
        locationGroup.location_group_name = "location-group-name";
        return locationGroup;
    }
}

