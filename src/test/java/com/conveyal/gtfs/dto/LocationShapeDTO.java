package com.conveyal.gtfs.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * DTO used to model expected {@link com.conveyal.gtfs.model.LocationShape} JSON structure for the editor. NOTE:
 * reference types (e.g., Integer and Double) are used here in order to model null/empty values in JSON object.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class LocationShapeDTO {
    public int id;
    public String location_id;
    public String geometry_id;
    public Double geometry_pt_lat;
    public Double geometry_pt_lon;

    public static LocationShapeDTO[] getLocationShapes(String locationId, int numberOfShapes, boolean firstAndLastMatch) {
        LocationShapeDTO[] locationShapes = new LocationShapeDTO[numberOfShapes];
        for (int i = 0; i < numberOfShapes; i++) {
            if (i == numberOfShapes - 1 && !firstAndLastMatch) {
                locationShapes[i] = createLocationShape(locationId, i, 89.243334, -10.74333);
            } else {
                locationShapes[i] = createLocationShape(locationId, i, 45.1111111, -80.432222);
            }
        }
        return locationShapes;
    }

    public static LocationShapeDTO create() {
        LocationShapeDTO locationShape = new LocationShapeDTO();
        locationShape.location_id = "location-shape-id-1";
        locationShape.geometry_id = "1";
        locationShape.geometry_pt_lat = 89.243334;
        locationShape.geometry_pt_lon = -10.74333;
        return locationShape;
    }

    public static LocationShapeDTO createLocationShape(String locationId, int id, Double lat, Double lon) {
        LocationShapeDTO locationShape = new LocationShapeDTO();
        locationShape.id = id;
        locationShape.location_id = locationId;
        locationShape.geometry_id = "1";
        locationShape.geometry_pt_lat = lat;
        locationShape.geometry_pt_lon = lon;
        return locationShape;
    }

}
