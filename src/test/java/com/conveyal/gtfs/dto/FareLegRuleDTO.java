package com.conveyal.gtfs.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * DTO used to model expected {@link com.conveyal.gtfs.model.FareLegRule} JSON structure for the editor. NOTE: reference types
 * (e.g., Integer and Double) are used here in order to model null/empty values in JSON object.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class FareLegRuleDTO {
    public Integer id;
    public String leg_group_id;
    public String network_id;
    public String from_area_id;
    public String to_area_id;
    public String from_timeframe_group_id;
    public String to_timeframe_group_id;
    public String fare_product_id;
    public int rule_priority;
}
