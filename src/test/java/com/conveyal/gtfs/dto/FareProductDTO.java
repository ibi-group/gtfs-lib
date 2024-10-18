package com.conveyal.gtfs.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * DTO used to model expected {@link com.conveyal.gtfs.model.FareProduct} JSON structure for the editor. NOTE: reference types
 * (e.g., Integer and Double) are used here in order to model null/empty values in JSON object.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class FareProductDTO {
    public Integer id;
    public String fare_product_id;
    public String fare_product_name;
    public String fare_media_id;
    public double amount;
    public String currency;
}
