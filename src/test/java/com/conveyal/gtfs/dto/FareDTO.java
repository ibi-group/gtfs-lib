package com.conveyal.gtfs.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * DTO used to model expected {@link com.conveyal.gtfs.model.Fare} JSON structure. NOTE: reference types (e.g., Integer
 * and Double) are used here in order to model null/empty values in JSON object.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class FareDTO {
    public int id;
    public String fare_id;
    public Double price;
    public String currency_type;
    public Integer payment_method;
    // transfers is a string because we need to be able to pass empty strings to the JdbcTableWriter
    public String transfers;
    public String agency_id;
    public Integer transfer_duration;
    public FareRuleDTO[] fare_rules;

    public static FareDTO create() {
        // create new object to be saved
        FareDTO fareDTO = new FareDTO();
        fareDTO.fare_id = "2A";
        fareDTO.currency_type = "USD";
        fareDTO.price = 2.50;
        fareDTO.agency_id = "RTA";
        fareDTO.payment_method = 0;
        // Empty string value or null should be permitted for transfers and transfer_duration
        fareDTO.transfers = "";
        fareDTO.transfer_duration = null;
        FareRuleDTO fareRuleInput = new FareRuleDTO();
        // Fare ID should be assigned to "child entity" by editor automatically.
        fareRuleInput.fare_id = null;
        fareRuleInput.route_id = null;
        fareRuleInput.contains_id = "any";
        fareRuleInput.origin_id = "value";
        fareRuleInput.destination_id = "permitted";
        fareDTO.fare_rules = new FareRuleDTO[] {fareRuleInput};
        return fareDTO;
    }
}
