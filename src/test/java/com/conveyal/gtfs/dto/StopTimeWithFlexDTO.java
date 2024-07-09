package com.conveyal.gtfs.dto;

public class StopTimeWithFlexDTO extends StopTimeDTO{
    // Additional GTFS Flex booking rule fields.
    public String pickup_booking_rule_id;
    public String drop_off_booking_rule_id;

    // Additional GTFS Flex location groups and locations fields
    public Integer start_pickup_drop_off_window;
    public Integer end_pickup_drop_off_window;

    /**
     * Empty constructor for deserialization
     */
    public StopTimeWithFlexDTO() {
    }

    /**
     * Used to create a stop time which references a stop.
     */
    public StopTimeWithFlexDTO(String stopId, Integer arrivalTime, Integer departureTime, Integer stopSequence) {
        stop_id = stopId;
        arrival_time = arrivalTime;
        departure_time = departureTime;
        stop_sequence = stopSequence;
    }

    /**
     * Used to create a stop time which references a location group or location.
     */
    public static StopTimeWithFlexDTO createFlexStopTime(
        String locationGroupId,
        String locationId,
        Integer start_pickup_drop_off_window,
        Integer end_pickup_drop_off_window,
        Integer stopSequence
    ) {
        StopTimeWithFlexDTO stopTimeWithFlexDTO = new StopTimeWithFlexDTO();
        stopTimeWithFlexDTO.location_group_id = locationGroupId;
        stopTimeWithFlexDTO.location_id = locationId;
        stopTimeWithFlexDTO.start_pickup_drop_off_window = start_pickup_drop_off_window;
        stopTimeWithFlexDTO.end_pickup_drop_off_window = end_pickup_drop_off_window;
        stopTimeWithFlexDTO.stop_sequence = stopSequence;
        return stopTimeWithFlexDTO;
    }
}
