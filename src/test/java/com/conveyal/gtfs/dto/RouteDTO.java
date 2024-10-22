package com.conveyal.gtfs.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * DTO used to model expected {@link com.conveyal.gtfs.model.Route} JSON structure for the editor. NOTE: reference types
 * (e.g., Integer and Double) are used here in order to model null/empty values in JSON object.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class RouteDTO {
    public int id;
    public String route_id;
    public String agency_id;
    public String route_short_name;
    public String route_long_name;
    public String route_desc;
    public Integer route_type;
    public String route_url;
    public String route_branding_url;
    public String route_color;
    public String route_text_color;
    public Integer publicly_visible;
    public Integer wheelchair_accessible;
    /** This field is incorrectly set to String in order to test how empty string literals are persisted to the database. */
    public String route_sort_order;
    public Integer status;
    public int continuous_pickup;
    public int continuous_drop_off;

    public static RouteDTO create() {
        return create("500");
    }

    public static RouteDTO create(String routeId) {
        RouteDTO routeDTO = new RouteDTO();
        routeDTO.route_id = routeId;
        routeDTO.agency_id = "RTA";
        // Empty value should be permitted for transfers and transfer_duration
        routeDTO.route_short_name = "500";
        routeDTO.route_long_name = "Hollingsworth";
        routeDTO.route_type = 3;
        // Set values to empty strings/null to later verify that they are set to null in the database.
        routeDTO.route_color = "";
        routeDTO.route_sort_order = "";
        return routeDTO;
    }
}
