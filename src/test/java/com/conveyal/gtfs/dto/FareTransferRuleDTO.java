package com.conveyal.gtfs.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * DTO used to model expected {@link com.conveyal.gtfs.model.FareTransferRule} JSON structure for the editor. NOTE: reference types
 * (e.g., Integer and Double) are used here in order to model null/empty values in JSON object.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class FareTransferRuleDTO {
    public Integer id;
    public String from_leg_group_id;
    public String to_leg_group_id;
    public int transfer_count;
    public int duration_limit;
    public int duration_limit_type;
    public int fare_transfer_type;
    public String fare_product_id;
}
