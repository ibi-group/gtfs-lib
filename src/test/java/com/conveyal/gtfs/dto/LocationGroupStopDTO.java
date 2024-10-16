package com.conveyal.gtfs.dto;

import com.conveyal.gtfs.model.LocationGroupStop;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * DTO used to model expected {@link LocationGroupStop} JSON structure for the editor. NOTE: reference types
 * (e.g., Integer and Double) are used here in order to model null/empty values in JSON object.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class LocationGroupStopDTO {
    public int id;
    public String location_group_id;
    public String stop_id;

    public static LocationGroupStopDTO create() {
        LocationGroupStopDTO locationGroupStop = new LocationGroupStopDTO();
        locationGroupStop.location_group_id = "location-group-id-1";
        locationGroupStop.stop_id = "1";
        return locationGroupStop;
    }
}

