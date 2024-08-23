package com.conveyal.gtfs.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * DTO used to model expected {@link com.conveyal.gtfs.model.TimeFrame} JSON structure for the editor. NOTE: reference types
 * (e.g., Integer and Double) are used here in order to model null/empty values in JSON object.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class TimeFrameDTO {
    public Integer id;
    public String timeframe_group_id;
    public Integer start_time;
    public Integer end_time;
    public String service_id;
}
