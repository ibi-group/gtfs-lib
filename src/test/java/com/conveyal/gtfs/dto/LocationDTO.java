package com.conveyal.gtfs.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.net.MalformedURLException;
import java.net.URL;

/**
 * DTO used to model expected {@link com.conveyal.gtfs.model.Location} JSON structure for the editor. NOTE:
 * reference types (e.g., Integer and Double) are used here in order to model null/empty values in JSON object.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class LocationDTO {
    public int id;
    public String location_id;
    public String stop_name;
    public String stop_desc;
    public String zone_id;
    public URL stop_url;
    public String geometry_type;
    public LocationShapeDTO[] location_shapes;

    public static LocationDTO create() throws MalformedURLException {
        return create("location-id-1", 4, true);
    }

    public static LocationDTO create(String locationId) throws MalformedURLException {
        return create(locationId, 4, true);
    }

    public static LocationDTO create(String locationId, int numberOfShapes, boolean firstAndLastMatch) throws MalformedURLException {
        LocationDTO location = new LocationDTO();
        location.location_id = locationId;
        location.geometry_type = "polygon";
        location.stop_name = "Templeboy to Ballisodare";
        location.stop_desc = "Templeboy to Ballisodare Door-to-door pickup area";
        location.zone_id = "1";
        location.stop_url = new URL("https://www.Teststopsite.com");
        location.location_shapes = LocationShapeDTO.getLocationShapes(location.location_id, numberOfShapes, firstAndLastMatch);
        return location;
    }

}
