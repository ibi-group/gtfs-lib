package com.conveyal.gtfs.error;

import com.conveyal.gtfs.validator.model.Priority;

/**
 * This enum defines the GTFS error types that can be encountered when validating GTFS table data. Each error type has a
 * severity level and related error message.
 */
public enum NewGTFSErrorType {
    // Standard errors.
    AGENCY_ID_REQUIRED_FOR_MULTI_AGENCY_FEEDS(Priority.HIGH, "For GTFS feeds with more than one agency, agency_id is required."),
    BOOLEAN_FORMAT(Priority.MEDIUM, "A GTFS boolean field must contain the value 1 or 0."),
    COLOR_FORMAT(Priority.MEDIUM, "A color should be specified with six-characters (three two-digit hexadecimal numbers)."),
    COLUMN_NAME_UNSAFE(Priority.HIGH, "Column header contains characters not safe in SQL, it was renamed."),
    CONDITIONALLY_REQUIRED(Priority.HIGH, "A conditionally required field was missing in a particular row."),
    CURRENCY_UNKNOWN(Priority.MEDIUM, "The currency code was not recognized."),
    DATE_FORMAT(Priority.MEDIUM, "Date format should be YYYYMMDD."),
    DATE_NO_SERVICE(Priority.MEDIUM, "No service_ids were active on a date within the range of dates with defined service."),
    DATE_RANGE(Priority.MEDIUM, "Date should is extremely far in the future or past."),
    DEPARTURE_BEFORE_ARRIVAL(Priority.MEDIUM, "The vehicle departs from this stop before it arrives."),
    DUPLICATE_HEADER(Priority.MEDIUM, "More than one column in a table had the same name in the header row."),
    DUPLICATE_ID(Priority.MEDIUM, "More than one entity in a table had the same ID."),
    DUPLICATE_STOP(Priority.MEDIUM, "More than one stop was located in exactly the same place."),
    DUPLICATE_TRIP(Priority.MEDIUM, "More than one trip had an identical schedule and stops."),
    FARE_TRANSFER_MISMATCH(Priority.MEDIUM, "A fare that does not permit transfers has a non-zero transfer duration."),
    FEED_TRAVEL_TIMES_ROUNDED(Priority.LOW, "All travel times in the feed are rounded to the minute, which may cause unexpected results in routing applications where travel times are zero."),
    FLEX_FORBIDDEN_ARRIVAL_TIME(Priority.HIGH, "arrival_time is not allow when either start_pickup_drop_off_window or end_pickup_drop_off_window is defined."),
    FLEX_FORBIDDEN_CONTINUOUS_DROP_OFF(Priority.HIGH, "continuous_drop_off is not allow when either start_pickup_drop_off_window or end_pickup_drop_off_window is defined."),
    FLEX_FORBIDDEN_CONTINUOUS_PICKUP(Priority.HIGH, "continuous_pickup is not allow when either start_pickup_drop_off_window or end_pickup_drop_off_window is defined."),
    FLEX_FORBIDDEN_DEPARTURE_TIME(Priority.HIGH, "departure_time is not allow when either start_pickup_drop_off_window or end_pickup_drop_off_window is defined."),
    FLEX_FORBIDDEN_DROP_OFF_TYPE(Priority.HIGH, "drop_off_type of 0 (Regularly scheduled pickup) is not allow when either start_pickup_drop_off_window or end_pickup_drop_off_window is defined."),
    FLEX_FORBIDDEN_DUPLICATE_LOCATION_GROUP_ID(Priority.HIGH, "location_group_id that matches a stop_id or location_id is not allow."),
    FLEX_FORBIDDEN_LOCATION_GROUP_ID(Priority.HIGH, "location_group_id is not allow if a stop_id or location_id is defined."),
    FLEX_FORBIDDEN_DUPLICATE_LOCATION_ID(Priority.HIGH, "location_id that matches a stop_id or location_group_id is not allow."),
    FLEX_FORBIDDEN_LOCATION_ID(Priority.HIGH, "location_id is not allow if a stop_id or location_group_id is defined."),
    FLEX_FORBIDDEN_START_PICKUP_DROP_OFF_WINDOW(Priority.HIGH, "start_pickup_drop_off_window is not allow when either an arrive_time or departure_time is defined."),
    FLEX_FORBIDDEN_END_PICKUP_DROP_OFF_WINDOW(Priority.HIGH, "end_pickup_drop_off_window is not allow when either an arrive_time or departure_time is defined."),
    FLEX_FORBIDDEN_PICKUP_TYPE(Priority.HIGH, "pickup_type of 0 (Regularly scheduled pickup) or 3 (Must coordinate with driver to arrange pickup) is not allow when either start_pickup_drop_off_window or end_pickup_drop_off_window is defined."),
    FLEX_FORBIDDEN_PICKUP_TYPE_FOR_LOCATION(Priority.HIGH, "pickup_type of 3 (Must coordinate with driver to arrange pickup) is not allow when a stop_id refers to a location."),
    FLEX_FORBIDDEN_PRIOR_NOTICE_DURATION_MIN(Priority.HIGH, "prior_notice_duration_min is not allow unless booking_type is 1 (Up to same-day booking with advance notice)."),
    FLEX_FORBIDDEN_PRIOR_NOTICE_DURATION_MAX(Priority.HIGH, "prior_notice_duration_max is not allow for booking_type 0 (Real time booking) or 2 (Up to prior day(s) booking)."),
    FLEX_FORBIDDEN_PRIOR_NOTICE_LAST_DAY(Priority.HIGH, "prior_notice_last_day is not allow for booking_type 2 (Up to prior day(s) booking)."),
    FLEX_FORBIDDEN_PRIOR_NOTICE_SERVICE_ID(Priority.HIGH, "prior_notice_service_id is not allow if prior_notice_start_day is not defined."),
    FLEX_FORBIDDEN_PRIOR_NOTICE_START_DAY_FOR_BOOKING_TYPE(Priority.HIGH, "prior_notice_start_day is not allow for booking_type 0 (Real time booking)."),
    FLEX_FORBIDDEN_PRIOR_NOTICE_START_DAY(Priority.HIGH, "prior_notice_start_day is not allow for booking_type 1 (Up to same-day booking with advance notice) if prior_notice_duration_max is defined."),
    FLEX_FORBIDDEN_PRIOR_START_TIME(Priority.HIGH, "prior_notice_start_time is not allow if prior_notice_start_day is not defined."),
    FLEX_FORBIDDEN_STOP_ID(Priority.HIGH, "stop_id is not allow if a location_group_id or location_id is defined."),
    FLEX_FORBIDDEN_ROUTE_CONTINUOUS_DROP_OFF(Priority.HIGH, "continuous_drop_off is not allow when either start_pickup_drop_off_window or end_pickup_drop_off_window is defined for any trip on this route."),
    FLEX_FORBIDDEN_ROUTE_CONTINUOUS_PICKUP(Priority.HIGH, "continuous_pick_up is not allow when either start_pickup_drop_off_window or end_pickup_drop_off_window is defined for any trip on this route."),
    FLEX_REQUIRED_END_PICKUP_DROP_OFF_WINDOW(Priority.HIGH, "end_pickup_drop_off_window is required if a location_group_id, location_id or start_pickup_drop_off_window is defined."),
    FLEX_REQUIRED_PRIOR_NOTICE_DURATION_MIN(Priority.HIGH, "prior_notice_duration_min is required for booking_type 1 (Up to same-day booking with advance notice)."),
    FLEX_REQUIRED_PRIOR_NOTICE_LAST_DAY(Priority.HIGH, "prior_notice_last_day is required if a stop_id refers to a location group or location."),
    FLEX_REQUIRED_PRIOR_NOTICE_START_TIME(Priority.HIGH, "prior_notice_start_time is required if prior_notice_start_day is defined."),
    FLEX_REQUIRED_STOP_ID(Priority.HIGH, "A stop_id is required if both location_group_id and location_id are not defined."),
    FLEX_REQUIRED_START_PICKUP_DROP_OFF_WINDOW(Priority.HIGH, "start_pickup_drop_off_window is required if a location_group_id, location_id or end_pickup_drop_off_window is defined."),
    FLEX_MISSING_FARE_RULE(Priority.HIGH, "A location zone_id must reference a fare rule (contains_id, destination_id or origin_id)."),
    FLOATING_FORMAT(Priority.MEDIUM, "Incorrect floating point number format."),
    FREQUENCY_PERIOD_OVERLAP(Priority.MEDIUM, "A frequency for a trip overlaps with another frequency defined for the same trip."),
    GEO_JSON_PARSING(Priority.HIGH, "Unable to parse the locations.geojson file. Make sure the file conforms to the GeoJSON standard and supported geometry types are used."),
    ILLEGAL_FIELD_VALUE(Priority.MEDIUM, "Fields may not contain tabs, carriage returns or new lines."),
    INTEGER_FORMAT(Priority.MEDIUM, "Incorrect integer format."),
    LANGUAGE_FORMAT(Priority.LOW, "Language should be specified with a valid BCP47 tag."),
    LOCATION_GROUP_UNUSED(Priority.MEDIUM, "This location group is not referenced by any trips."),
    LOCATION_UNUSED(Priority.MEDIUM, "This location is not referenced by any trips."),
    STOP_AREA_PARSING(Priority.HIGH, "Unable to parse the stop_areas.txt file. Make sure the file conforms to the GTFS Flex standard."),
    MISSING_ARRIVAL_OR_DEPARTURE(Priority.MEDIUM, "First and last stop times are required to have both an arrival and departure time."),
    MISSING_COLUMN(Priority.MEDIUM, "A required column was missing from a table."),
    MISSING_FIELD(Priority.MEDIUM, "A required field was missing or empty in a particular row."),
    MISSING_FOREIGN_TABLE_REFERENCE(Priority.HIGH, "This line references an ID that must exist in a single foreign table."),
    MISSING_SHAPE(Priority.MEDIUM, "???"),
    MISSING_TABLE(Priority.MEDIUM, "This table is required by the GTFS specification but is missing."),
    MULTIPLE_SHAPES_FOR_PATTERN(Priority.MEDIUM, "Multiple shapes found for a single unique sequence of stops (i.e, trip pattern)."),
    NO_SERVICE(Priority.HIGH, "There is no service defined on any day in this feed."),
    NUMBER_NEGATIVE(Priority.MEDIUM, "Number was expected to be non-negative."),
    NUMBER_PARSING(Priority.MEDIUM, "Unable to parse number from value."),
    NUMBER_TOO_LARGE(Priority.MEDIUM, "Number was above the allowed range."),
    NUMBER_TOO_SMALL(Priority.MEDIUM, "Number was below the allowed range."),
    OVERLAPPING_TRIP(Priority.MEDIUM, "Blocks?"),
    REFERENTIAL_INTEGRITY(Priority.HIGH, "This line references an ID that does not exist in the target table."),
    REQUIRED_TABLE_EMPTY(Priority.MEDIUM, "This table is required by the GTFS specification but is empty."),
    ROUTE_DESCRIPTION_SAME_AS_NAME(Priority.LOW, "The description of a route is identical to its name, so does not add any information."),
    ROUTE_LONG_NAME_CONTAINS_SHORT_NAME(Priority.LOW, "The long name of a route should complement the short name, not include it."),
    ROUTE_SHORT_AND_LONG_NAME_MISSING(Priority.MEDIUM, "A route has neither a long nor a short name."),
    ROUTE_SHORT_NAME_TOO_LONG(Priority.MEDIUM, "The short name of a route is too long for display in standard GTFS consumer applications."),
    ROUTE_TYPE_INVALID(Priority.MEDIUM, "The route type is not valid."),
    ROUTE_UNUSED(Priority.MEDIUM, "This route is defined but has no trips."),
    SERVICE_NEVER_ACTIVE(Priority.MEDIUM, "A service code was defined, but is never active on any date."),
    SERVICE_UNUSED(Priority.MEDIUM, "A service code was defined, but is never referenced by any trips."),
    SERVICE_WITHOUT_DAYS_OF_WEEK(Priority.MEDIUM, "A service defined in calendar.txt should be active on at least one day of the week. Otherwise, it should be omitted from this file."),
    SHAPE_DIST_TRAVELED_NOT_INCREASING(Priority.MEDIUM, "Shape distance traveled must increase with stop times."),
    SHAPE_MISSING_COORDINATE(Priority.MEDIUM, "???"),
    SHAPE_REVERSED(Priority.MEDIUM, "A shape appears to be intended for vehicles running the opposite direction on the route."),
    STOP_DESCRIPTION_SAME_AS_NAME(Priority.LOW, "The description of a stop is identical to its name, so does not add any information."),
    STOP_GEOGRAPHIC_OUTLIER(Priority.MEDIUM, "This stop is located very far from the middle 90% of stops in this feed."),
    STOP_LOW_POPULATION_DENSITY(Priority.MEDIUM, "A stop is located in a geographic area with very low human population density."),
    STOP_NAME_MISSING(Priority.MEDIUM, "A stop does not have a name."),
    STOP_TIME_UNUSED(Priority.LOW, "This stop time allows neither pickup nor drop off and is not a timepoint, so it serves no purpose and should be removed from trip."),
    STOP_UNUSED(Priority.MEDIUM, "This stop is not referenced by any trips."),
    TABLE_IN_SUBDIRECTORY(Priority.HIGH, "Rather than being at the root of the zip file, a table was nested in a subdirectory."),
    TABLE_MISSING_COLUMN_HEADERS(Priority.HIGH, "Table is missing column headers."),
    TABLE_TOO_LONG(Priority.MEDIUM, "Table is too long to record line numbers with a 32-bit integer, overflow will occur."),
    TIME_FORMAT(Priority.MEDIUM, "Time format should be HH:MM:SS."),
    TIME_ZONE_FORMAT(Priority.MEDIUM, "Time zone format should match value from the Time Zone Database https://en.wikipedia.org/wiki/List_of_tz_database_time_zones."),
    TIMEPOINT_MISSING_TIMES(Priority.MEDIUM, "This stop time is marked as a timepoint, but is missing both arrival and departure times."),
    TRAVEL_DISTANCE_ZERO(Priority.MEDIUM, "The vehicle does not cover any distance between the last stop and this one."),
    TRAVEL_TIME_NEGATIVE(Priority.HIGH, "The vehicle arrives at this stop before it departs from the previous one."),
    TRAVEL_TIME_ZERO(Priority.HIGH, "The vehicle arrives at this stop at the same time it departs from the previous stop."),
    TRAVEL_TOO_FAST(Priority.MEDIUM, "The vehicle travels extremely fast to reach this stop from the previous one."),
    TRAVEL_TOO_SLOW(Priority.MEDIUM, "The vehicle is traveling very slowly to reach this stop from the previous one."),
    TRIP_EMPTY(Priority.HIGH, "This trip is defined but has no stop times."),
    TRIP_HEADSIGN_CONTAINS_ROUTE_NAME(Priority.LOW, "A trip headsign contains the route name, but should only contain information to distinguish it from other trips for the route."),
    TRIP_HEADSIGN_SHOULD_DESCRIBE_DESTINATION_OR_WAYPOINTS(Priority.LOW, "A trip headsign begins with 'to' or 'towards', but should begin with destination or direction and optionally include waypoints with 'via'"),
    TRIP_NEVER_ACTIVE(Priority.MEDIUM, "A trip is defined, but its service is never running on any date."),
    TRIP_OVERLAP_IN_BLOCK(Priority.MEDIUM, "A trip overlaps another trip and shares the same block_id."),
    TRIP_TOO_FEW_STOP_TIMES(Priority.MEDIUM, "A trip must have at least two stop times to represent travel."),
    TRIP_SPEED_NOT_VALIDATED(Priority.LOW, "Trip speed not validated because it contains at least one stop which is a location or stop area."),
    URL_FORMAT(Priority.MEDIUM, "URL format should be <scheme>://<authority><path>?<query>#<fragment>"),
    VALIDATOR_FAILED(Priority.HIGH, "The specified validation stage failed due to an error encountered during loading. This is likely due to an error encountered during loading (e.g., a date or number field is formatted incorrectly.)."),
    WRONG_NUMBER_OF_FIELDS(Priority.MEDIUM, "A row did not have the same number of fields as there are headers in its table."),

    // MTC-specific errors.
    FIELD_VALUE_TOO_LONG(Priority.MEDIUM, "Field value has too many characters."),

    // Shared Stops-specifc errors.
    MULTIPLE_SHARED_STOPS_GROUPS(Priority.HIGH, "A GTFS stop belongs to more than one shared-stop group, or belongs to the same shared-stop group twice."),
    SHARED_STOP_GROUP_MULTIPLE_PRIMARY_STOPS(Priority.HIGH, "A shared-stop group has multiple primary stops."),
    SHARED_STOP_GROUP_ENTITY_DOES_NOT_EXIST(Priority.MEDIUM, "The stop referenced by a shared-stop does not exist in the feed it was said to exist in."),

    // Unknown errors.
    OTHER(Priority.LOW, "Other errors.");

    public final Priority priority;
    public final String englishMessage;

    NewGTFSErrorType(Priority priority, String englishMessage) {
        this.priority = priority;
        this.englishMessage = englishMessage;
    }

}


