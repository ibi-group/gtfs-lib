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
}

