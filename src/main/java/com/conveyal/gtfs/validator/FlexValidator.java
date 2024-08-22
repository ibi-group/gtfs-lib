package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.error.NewGTFSError;
import com.conveyal.gtfs.error.NewGTFSErrorType;
import com.conveyal.gtfs.error.SQLErrorStorage;
import com.conveyal.gtfs.loader.Feed;
import com.conveyal.gtfs.model.BookingRule;
import com.conveyal.gtfs.model.FareRule;
import com.conveyal.gtfs.model.Location;
import com.conveyal.gtfs.model.LocationGroup;
import com.conveyal.gtfs.model.Route;
import com.conveyal.gtfs.model.Stop;
import com.conveyal.gtfs.model.LocationGroupStop;
import com.conveyal.gtfs.model.StopTime;
import com.conveyal.gtfs.model.Trip;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static com.conveyal.gtfs.error.NewGTFSErrorType.VALIDATOR_FAILED;
import static com.conveyal.gtfs.model.Entity.INT_MISSING;
import static com.conveyal.gtfs.model.StopTime.getFlexStopTimesForValidation;
import static org.apache.commons.lang3.ObjectUtils.isNotEmpty;

/**
 * Spec validation checks for flex additions as defined here:
 * <a href="https://github.com/google/transit/blob/master/gtfs/spec/en/reference.md">GTFS Reference (with Flex v2)</a>
 *
 */
public class FlexValidator extends FeedValidator {

    DataSource dataSource;

    public FlexValidator(Feed feed, SQLErrorStorage errorStorage, DataSource dataSource) {
        super(feed, errorStorage);
        this.dataSource = dataSource;
    }

    @Override
    public void validate() {
        List<BookingRule> bookingRules = Lists.newArrayList(feed.bookingRules);
        List<LocationGroup> locationGroups = Lists.newArrayList(feed.locationGroups);
        List<LocationGroupStop> locationGroupStops = Lists.newArrayList(feed.locationGroupStops);
        List<Location> locations = Lists.newArrayList(feed.locations);
        List<Route> routes = Lists.newArrayList(feed.routes);
        List<Trip> trips = Lists.newArrayList(feed.trips);

        if (isFlexFeed(bookingRules, locationGroups, locationGroupStops, locations)) {
            List<NewGTFSError> errors = new ArrayList<>();
            try (Connection connection = dataSource.getConnection()) {
                List<StopTime> stopTimes = getFlexStopTimesForValidation(connection, feed.databaseSchemaPrefix);
                stopTimes.forEach(stopTime -> errors.addAll(validateStopTime(stopTime)));
                trips.forEach(trip -> errors.addAll(validateTrip(trip, stopTimes)));
                routes.forEach(route -> errors.addAll(validateRoute(route, trips, stopTimes)));
            } catch (SQLException e) {
                String badValue = String.join(":", this.getClass().getSimpleName(), e.toString());
                errorStorage.storeError(NewGTFSError.forFeed(VALIDATOR_FAILED, badValue));
            }
            List<Stop> stops = Lists.newArrayList(feed.stops);
            List<FareRule> fareRules = Lists.newArrayList(feed.fareRules);
            feed.bookingRules.forEach(bookingRule -> errors.addAll(validateBookingRule(bookingRule)));
            feed.locationGroups.forEach(locationGroup -> errors.addAll(validateLocationGroup(locationGroup, stops, locations)));
            feed.locations.forEach(location -> errors.addAll(validateLocation(locationGroups, location, stops, fareRules)));
            // Register errors, if any, once all checks have been completed.
            errors.forEach(this::registerError);
        }
    }

    /**
     * Determine if the feed is flex.
     */
    private static boolean isFlexFeed(
        List<BookingRule> bookingRules,
        List<LocationGroup> locationGroups,
        List<LocationGroupStop> locationGroupStops,
        List<Location> locations
    ) {
        return
            (bookingRules != null && !bookingRules.isEmpty()) ||
            (locationGroups != null && !locationGroups.isEmpty()) ||
            (locationGroupStops != null && !locationGroupStops.isEmpty()) ||
            (locations != null && !locations.isEmpty());
    }

    /**
     * Check if a trip contains a stop that references a location or stop area. A trip's speed can not be validated
     * if at least one stop references a location or stop area.
     */
    public static List<NewGTFSError> validateTrip(Trip trip, List<StopTime> stopTimes) {
        List<NewGTFSError> errors = new ArrayList<>();
        if (hasFlexLocation(trip, stopTimes)) {
            errors.add(NewGTFSError
                .forEntity(trip, NewGTFSErrorType.TRIP_SPEED_NOT_VALIDATED)
                .setBadValue(trip.trip_id)
            );
        }
        return errors;
    }

    /**
     * A route cannot define a continuous_pickup nor continuous_drop_off if these values are defined for one or more
     * stop times for any trip.
     */
    public static List<NewGTFSError> validateRoute(Route route, List<Trip> trips, List<StopTime> stopTimes) {
        List<NewGTFSError> errors = new ArrayList<>();
        if (route.continuous_drop_off == INT_MISSING && route.continuous_pickup == INT_MISSING) {
            return errors;
        }

        trips
            .stream()
            .filter(trip -> trip.route_id.equalsIgnoreCase(route.route_id))
            .forEach(trip -> {
                boolean match = stopTimes
                    .stream()
                    .filter(stopTime -> stopTime.trip_id.equalsIgnoreCase(trip.trip_id))
                    .anyMatch(FlexValidator::hasStartOrEndPickupDropOffWindow);
                if (match) {
                    if (route.continuous_drop_off != INT_MISSING) {
                        errors.add(NewGTFSError
                            .forEntity(route, NewGTFSErrorType.FLEX_FORBIDDEN_ROUTE_CONTINUOUS_DROP_OFF)
                            .setBadValue(String.valueOf(route.continuous_drop_off))
                        );
                    }
                    if (route.continuous_pickup != INT_MISSING) {
                        errors.add(NewGTFSError
                            .forEntity(route, NewGTFSErrorType.FLEX_FORBIDDEN_ROUTE_CONTINUOUS_PICKUP)
                            .setBadValue(String.valueOf(route.continuous_pickup))
                        );
                    }
                }
            });
        return errors;
    }

    /**
     * Check that a location group conforms to flex specification constraints.
     */
    public static List<NewGTFSError> validateLocationGroup(
        LocationGroup locationGroup,
        List<Stop> stops,
        List<Location> locations
    ) {
        List<NewGTFSError> errors = new ArrayList<>();
        if (
            hasMatchOnStopId(stops, locationGroup.location_group_id) ||
            hasMatchOnLocationId(locations, locationGroup.location_group_id)
        ) {
            errors.add(NewGTFSError
                .forEntity(locationGroup,NewGTFSErrorType.FLEX_FORBIDDEN_DUPLICATE_LOCATION_GROUP_ID)
                .setBadValue(locationGroup.location_group_id)
            );
        }
        return errors;
    }

    /**
     * Check location id and zone id conforms to flex specification constraints.
     */
    public static List<NewGTFSError> validateLocation(
        List<LocationGroup> locationGroup,
        Location location,
        List<Stop> stops,
        List<FareRule> fareRules
    ) {
        List<NewGTFSError> errors = new ArrayList<>();
        if (
            hasMatchOnStopId(stops, location.location_id) ||
            hasMatchOnLocationGroupId(locationGroup, location.location_id)
        ) {
            errors.add(NewGTFSError
                .forEntity(location, NewGTFSErrorType.FLEX_FORBIDDEN_DUPLICATE_LOCATION_ID)
                .setBadValue(location.location_id)
            );
        }
        if (hasFareRules(fareRules, location.zone_id)) {
            errors
                .add(NewGTFSError.forEntity(location, NewGTFSErrorType.FLEX_MISSING_FARE_RULE)
                .setBadValue(location.zone_id)
            );
        }
        return errors;
    }

    /**
     * Check that the fields of a stop time object conform to flex specification constraints.
     */
    public static List<NewGTFSError> validateStopTime(StopTime stopTime) {
        List<NewGTFSError> errors = new ArrayList<>();
        validateArrivalTime(stopTime, errors);
        validateDepartureTime(stopTime, errors);
        validateStopId(stopTime, errors);
        validateLocationGroupId(stopTime, errors);
        validateLocationId(stopTime, errors);
        validateStartPickupDropOffWindow(stopTime, errors);
        validateEndPickupDropOffWindow(stopTime, errors);
        validatePickUpType(stopTime, errors);
        validateDropOffType(stopTime, errors);
        validateContinuousPickup(stopTime, errors);
        validateContinuousDropOff(stopTime, errors);
        return errors;
    }

    private static boolean hasStartOrEndPickupDropOffWindow(StopTime stopTime) {
        return stopTime.start_pickup_drop_off_window != INT_MISSING || stopTime.end_pickup_drop_off_window != INT_MISSING;
    }

    /**
     * Conditionally Required:
     * - Forbidden when start_pickup_drop_off_window or end_pickup_drop_off_window are defined.
     * - Optional otherwise.
     */
    public static void validateArrivalTime(StopTime stopTime, List<NewGTFSError> errors) {
        if (stopTime.arrival_time != INT_MISSING && hasStartOrEndPickupDropOffWindow(stopTime)
        ) {
            errors.add(NewGTFSError
                .forEntity(stopTime, NewGTFSErrorType.FLEX_FORBIDDEN_ARRIVAL_TIME)
                .setBadValue(Integer.toString(stopTime.arrival_time))
            );
        }
    }

    /**
     * Conditionally Required:
     * - Forbidden when start_pickup_drop_off_window or end_pickup_drop_off_window are defined.
     * - Optional otherwise.
     */
    public static void validateDepartureTime(StopTime stopTime, List<NewGTFSError> errors) {
        if (
            stopTime.departure_time != INT_MISSING &&
            hasStartOrEndPickupDropOffWindow(stopTime)
        ) {
            // Departure time must not be defined if start/end pickup drop off window is defined.
            errors.add(NewGTFSError
                .forEntity(stopTime, NewGTFSErrorType.FLEX_FORBIDDEN_DEPARTURE_TIME)
                .setBadValue(Integer.toString(stopTime.departure_time))
            );
        }
    }

    /**
     * Conditionally Required:
     * - Required if stop_times.location_group_id AND stop_times.location_id are NOT defined.
     * - Forbidden if stop_times.location_group_id or stop_times.location_id are defined.
     */
    public static void validateStopId(StopTime stopTime, List<NewGTFSError> errors) {
        if (hasNoHalt(stopTime)) {
            // No stop id, location group id or location id defined, a stop id is required.
            errors.add(NewGTFSError
                .forEntity(stopTime, NewGTFSErrorType.FLEX_REQUIRED_STOP_ID)
                .setBadValue(stopTime.stop_id)
            );
        }

        if (isStopIdForbidden(stopTime)) {
            // If a location group id or location id is defined, a stop id is forbidden.
            errors.add(NewGTFSError
                .forEntity(stopTime, NewGTFSErrorType.FLEX_FORBIDDEN_STOP_ID)
                .setBadValue(stopTime.stop_id)
            );
        }
    }

    /**
     * Conditionally Forbidden:
     * - Forbidden if stop_times.stop_id or stop_times.location_id are defined.
     */
    public static void validateLocationGroupId(StopTime stopTime, List<NewGTFSError> errors) {
        if (isLocationGroupIdForbidden(stopTime)) {
            // If a stop id or location id is defined, a location group id is forbidden.
            errors.add(NewGTFSError
                .forEntity(stopTime, NewGTFSErrorType.FLEX_FORBIDDEN_LOCATION_GROUP_ID)
                .setBadValue(stopTime.location_group_id)
            );
        }
    }

    /**
     * Conditionally Forbidden:
     * - Forbidden if stop_times.stop_id or stop_times.location_group_id are defined.
     */
    public static void validateLocationId(StopTime stopTime, List<NewGTFSError> errors) {
        if (isLocationIdForbidden(stopTime)) {
            // If a stop id or location group id is defined, a location id is forbidden.
            errors.add(NewGTFSError
                .forEntity(stopTime, NewGTFSErrorType.FLEX_FORBIDDEN_LOCATION_ID)
                .setBadValue(stopTime.location_group_id)
            );
        }
    }

    /**
     * Conditionally Required:
     * - Required if stop_times.location_group_id or stop_times.location_id is defined.
     * - Required if end_pickup_drop_off_window is defined.
     * - Forbidden if arrival_time or departure_time is defined.
     * - Optional otherwise.
     */
    public static void validateStartPickupDropOffWindow(StopTime stopTime, List<NewGTFSError> errors) {
        boolean isLocationOrLocationGroupDefined = isLocationOrLocationGroupDefined(stopTime);
        boolean isArriveOrDepartureTimeDefined = isArriveOrDepartureTimeDefined(stopTime);

        if (stopTime.start_pickup_drop_off_window == INT_MISSING && isLocationOrLocationGroupDefined) {
            // start_pickup_drop_off_window is required if location group id or location id is defined.
            errors.add(NewGTFSError
                .forEntity(stopTime,NewGTFSErrorType.FLEX_REQUIRED_START_PICKUP_DROP_OFF_WINDOW)
                .setBadValue(Integer.toString(stopTime.start_pickup_drop_off_window))
            );
        }

        if (stopTime.start_pickup_drop_off_window == INT_MISSING && stopTime.end_pickup_drop_off_window != INT_MISSING) {
            // start_pickup_drop_off_window is required if end_pickup_drop_off_window is defined.
            errors.add(NewGTFSError
                .forEntity(stopTime, NewGTFSErrorType.FLEX_REQUIRED_START_PICKUP_DROP_OFF_WINDOW)
                .setBadValue(Integer.toString(stopTime.start_pickup_drop_off_window))
            );
        }

        if (stopTime.start_pickup_drop_off_window != INT_MISSING && isArriveOrDepartureTimeDefined) {
            // start_pickup_drop_off_window is forbidden if arrival_time or departure_time is defined.
            errors.add(NewGTFSError
                .forEntity(stopTime,NewGTFSErrorType.FLEX_FORBIDDEN_START_PICKUP_DROP_OFF_WINDOW)
                .setBadValue(Integer.toString(stopTime.start_pickup_drop_off_window))
            );
        }

    }

    /**
     * Conditionally Required:
     * - Required if stop_times.location_group_id or stop_times.location_id is defined.
     * - Required if start_pickup_drop_off_window is defined.
     * - Forbidden if arrival_time or departure_time is defined.
     * - Optional otherwise.
     */
    public static void validateEndPickupDropOffWindow(StopTime stopTime, List<NewGTFSError> errors) {
        boolean isLocationOrLocationGroupDefined = isLocationOrLocationGroupDefined(stopTime);
        boolean isArriveOrDepartureTimeDefined = isArriveOrDepartureTimeDefined(stopTime);

        if (stopTime.end_pickup_drop_off_window == INT_MISSING && isLocationOrLocationGroupDefined) {
            // end_pickup_drop_off_window is required if location group id or location id is defined.
            errors.add(NewGTFSError
                .forEntity(stopTime,NewGTFSErrorType.FLEX_REQUIRED_END_PICKUP_DROP_OFF_WINDOW)
                .setBadValue(Integer.toString(stopTime.start_pickup_drop_off_window))
            );
        }

        if (stopTime.end_pickup_drop_off_window == INT_MISSING && stopTime.start_pickup_drop_off_window != INT_MISSING) {
            // end_pickup_drop_off_window is required if start_pickup_drop_off_window is defined.
            errors.add(NewGTFSError
                .forEntity(stopTime,NewGTFSErrorType.FLEX_REQUIRED_END_PICKUP_DROP_OFF_WINDOW)
                .setBadValue(Integer.toString(stopTime.end_pickup_drop_off_window))
            );
        }

        if (stopTime.end_pickup_drop_off_window != INT_MISSING && isArriveOrDepartureTimeDefined) {
            // end_pickup_drop_off_window is forbidden if arrival_time or departure_time is defined.
            errors.add(NewGTFSError
                .forEntity(stopTime,NewGTFSErrorType.FLEX_FORBIDDEN_END_PICKUP_DROP_OFF_WINDOW)
                .setBadValue(Integer.toString(stopTime.end_pickup_drop_off_window))
            );
        }
    }

    /**
     * Conditionally Forbidden:
     * - pickup_type=0 forbidden if start_pickup_drop_off_window or end_pickup_drop_off_window are defined.
     * - pickup_type=3 forbidden if start_pickup_drop_off_window or end_pickup_drop_off_window are defined.
     * - Optional otherwise.
     */
    public static void validatePickUpType(StopTime stopTime, List<NewGTFSError> errors) {
        if ((stopTime.pickup_type == 0 || stopTime.pickup_type == 3) && hasStartOrEndPickupDropOffWindow(stopTime)) {
            // pickup_type 0 (Regularly scheduled pickup) and 3 (Must coordinate with driver to arrange pickup) are
            // forbidden if start/end pick up drop off window are defined.
            errors.add(NewGTFSError
                .forEntity(stopTime,NewGTFSErrorType.FLEX_FORBIDDEN_PICKUP_TYPE)
                .setBadValue(Integer.toString(stopTime.pickup_type))
            );
        }
    }

    /**
     * Conditionally Forbidden:
     * - drop_off_type=0 forbidden if start_pickup_drop_off_window or end_pickup_drop_off_window are defined.
     * - Optional otherwise.
     */
    public static void validateDropOffType(StopTime stopTime, List<NewGTFSError> errors) {
        if (stopTime.drop_off_type == 0 && hasStartOrEndPickupDropOffWindow(stopTime)) {
            // drop_off_type 0 (Regularly scheduled pickup) is forbidden if start/end pick up drop off window are defined.
            errors.add(NewGTFSError
                .forEntity(stopTime, NewGTFSErrorType.FLEX_FORBIDDEN_DROP_OFF_TYPE)
                .setBadValue(Integer.toString(stopTime.drop_off_type))
            );
        }
    }

    /**
     * Conditionally Forbidden:
     * - Forbidden if start_pickup_drop_off_window or end_pickup_drop_off_window are defined.
     * - Optional otherwise.
     */
    public static void validateContinuousPickup(StopTime stopTime, List<NewGTFSError> errors) {
        if (hasStartOrEndPickupDropOffWindow(stopTime)) {
            errors.add(NewGTFSError
                .forEntity(stopTime, NewGTFSErrorType.FLEX_FORBIDDEN_CONTINUOUS_PICKUP)
                .setBadValue(Integer.toString(stopTime.drop_off_type))
            );
        }
    }

    /**
     * Conditionally Forbidden:
     * - Forbidden if start_pickup_drop_off_window or end_pickup_drop_off_window are defined.
     * - Optional otherwise.
     */
    public static void validateContinuousDropOff(StopTime stopTime, List<NewGTFSError> errors) {
        if (hasStartOrEndPickupDropOffWindow(stopTime)) {
            errors.add(NewGTFSError
                .forEntity(stopTime, NewGTFSErrorType.FLEX_FORBIDDEN_CONTINUOUS_DROP_OFF)
                .setBadValue(Integer.toString(stopTime.drop_off_type))
            );
        }
    }

    /**
     * Check that a booking rule conforms to flex specification constraints.
     */
    public static List<NewGTFSError> validateBookingRule(BookingRule bookingRule) {
        List<NewGTFSError> errors = new ArrayList<>();

        if (bookingRule.prior_notice_duration_min == INT_MISSING && bookingRule.booking_type == 1) {
            // prior_notice_duration_min is required for booking_type 1 (Up to same-day booking with advance notice).
            errors.add(NewGTFSError.forEntity(
                    bookingRule,
                    NewGTFSErrorType.FLEX_REQUIRED_PRIOR_NOTICE_DURATION_MIN)
                .setBadValue(Integer.toString(bookingRule.prior_notice_duration_min))
            );
        }
        if (bookingRule.prior_notice_duration_min != INT_MISSING && bookingRule.booking_type != 1) {
            // prior_notice_duration_min is forbidden for all but booking_type 1 (Up to same-day booking with advance notice).
            errors.add(NewGTFSError.forEntity(
                    bookingRule,
                    NewGTFSErrorType.FLEX_FORBIDDEN_PRIOR_NOTICE_DURATION_MIN)
                .setBadValue(Integer.toString(bookingRule.prior_notice_duration_min))
            );
        }
        if (bookingRule.prior_notice_duration_max != INT_MISSING &&
            (bookingRule.booking_type == 0 || bookingRule.booking_type == 2)
        ) {
            // prior_notice_duration_max is forbidden for booking_type 0 (Real time booking) &
            // 2 (Up to prior day(s) booking).
            errors.add(NewGTFSError.forEntity(
                    bookingRule,
                    NewGTFSErrorType.FLEX_FORBIDDEN_PRIOR_NOTICE_DURATION_MAX)
                .setBadValue(Integer.toString(bookingRule.prior_notice_duration_max))
            );
        }
        if (bookingRule.prior_notice_last_day == INT_MISSING && bookingRule.booking_type == 2) {
            // prior_notice_last_day is required for booking_type 2 (Up to prior day(s) booking).
            errors.add(NewGTFSError.forEntity(
                    bookingRule,
                    NewGTFSErrorType.FLEX_REQUIRED_PRIOR_NOTICE_LAST_DAY)
                .setBadValue(Integer.toString(bookingRule.prior_notice_last_day))
            );
        }
        if (bookingRule.prior_notice_last_day != INT_MISSING && bookingRule.booking_type != 2) {
            // prior_notice_last_day is forbidden for all but booking_type 2 (Up to prior day(s) booking).
            errors.add(NewGTFSError.forEntity(
                    bookingRule,
                    NewGTFSErrorType.FLEX_FORBIDDEN_PRIOR_NOTICE_LAST_DAY)
                .setBadValue(Integer.toString(bookingRule.prior_notice_last_day))
            );
        }
        if (bookingRule.prior_notice_start_day != INT_MISSING && bookingRule.booking_type == 0) {
            // prior_notice_start_day is forbidden for booking_type 0 (Real time booking).
            errors.add(NewGTFSError.forEntity(
                    bookingRule,
                    NewGTFSErrorType.FLEX_FORBIDDEN_PRIOR_NOTICE_START_DAY_FOR_BOOKING_TYPE)
                .setBadValue(Integer.toString(bookingRule.prior_notice_start_day))
            );
        }
        if (bookingRule.prior_notice_start_day != INT_MISSING &&
            bookingRule.booking_type == 1 &&
            bookingRule.prior_notice_duration_max != INT_MISSING
        ) {
            // prior_notice_start_day is forbidden for booking_type 1 (Up to same-day booking with advance notice) if
            // prior_notice_duration_max is defined.
            errors.add(NewGTFSError.forEntity(
                    bookingRule,
                    NewGTFSErrorType.FLEX_FORBIDDEN_PRIOR_NOTICE_START_DAY)
                .setBadValue(Integer.toString(bookingRule.prior_notice_start_day))
            );
        }
        if (bookingRule.prior_notice_start_time == null && bookingRule.prior_notice_start_day != INT_MISSING) {
            // prior_notice_start_time is required if prior_notice_start_day is defined.
            errors.add(NewGTFSError.forEntity(
                    bookingRule,
                    NewGTFSErrorType.FLEX_REQUIRED_PRIOR_NOTICE_START_TIME)
                .setBadValue(bookingRule.prior_notice_start_time)
            );
        }
        if (bookingRule.prior_notice_start_time != null && bookingRule.prior_notice_start_day == INT_MISSING) {
            // prior_notice_start_time is forbidden if prior_notice_start_day is not defined.
            errors.add(NewGTFSError.forEntity(
                    bookingRule,
                    NewGTFSErrorType.FLEX_FORBIDDEN_PRIOR_START_TIME)
                .setBadValue(bookingRule.prior_notice_start_time)
            );
        }
        if (StringUtils.isNotBlank(bookingRule.prior_notice_service_id) && bookingRule.booking_type != 2) {
            // prior_notice_service_id is forbidden for all but booking_type 2 (Up to prior day(s) booking).
            errors.add(NewGTFSError.forEntity(
                    bookingRule,
                    NewGTFSErrorType.FLEX_FORBIDDEN_PRIOR_NOTICE_SERVICE_ID)
                .setBadValue(bookingRule.prior_notice_service_id)
            );
        }
        return errors;
    }

    /**
     * Check if a location group id or location id matches any stop ids.
     */
    private static boolean hasMatchOnStopId(List<Stop> stops, String id) {
        return
            isNotEmpty(stops) &&
            stops.stream().anyMatch(stop -> stop.stop_id != null && stop.stop_id.equals(id));
    }

    /**
     * Check if a location group id matches any location ids.
     */
    private static boolean hasMatchOnLocationId(List<Location> locations, String id) {
        return
            isNotEmpty(locations) &&
            locations.stream().anyMatch(location -> location.location_id != null && location.location_id.equals(id));
    }

    /**
     * Check if a location id matches any location group ids.
     */
    private static boolean hasMatchOnLocationGroupId(List<LocationGroup> locationGroups, String id) {
        return
            isNotEmpty(locationGroups) &&
            locationGroups.stream().anyMatch(locationGroup -> locationGroup.location_group_id.equals(id));
    }

    /**
     * If fare rules are defined, check there is a match on zone id.
     */
    private static boolean hasFareRules(List<FareRule> fareRules, String zoneId) {
        return
            isNotEmpty(fareRules) &&
            fareRules.stream().anyMatch(fareRule ->
                (fareRule.contains_id != null && fareRule.destination_id != null && fareRule.origin_id != null) &&
                    !fareRule.contains_id.equals(zoneId) &&
                    !fareRule.destination_id.equals(zoneId) &&
                    !fareRule.origin_id.equals(zoneId));
    }

    /**
     * Stop time does not have a stop id, location group id or location id.
     */
    public static boolean hasNoHalt(StopTime stopTime) {
        return stopTime.stop_id == null && stopTime.location_group_id == null && stopTime.location_id == null;
    }

    /**
     * If a location group or location is defined, a stop id is forbidden.
     */
    public static boolean isStopIdForbidden(StopTime stopTime) {
        return stopTime.location_group_id != null || stopTime.location_id != null;
    }

    /**
     * If a stop id or location id is defined, a location group id is forbidden.
     */
    public static boolean isLocationGroupIdForbidden(StopTime stopTime) {
        return stopTime.location_group_id != null && (stopTime.stop_id != null || stopTime.location_id != null);
    }

    /**
     * If a stop id or location group id is defined, a location id is forbidden.
     */
    public static boolean isLocationIdForbidden(StopTime stopTime) {
        return stopTime.location_id != null && (stopTime.stop_id != null || stopTime.location_group_id != null);
    }

    public static boolean isLocationOrLocationGroupDefined(StopTime stopTime) {
        return stopTime.location_group_id != null || stopTime.location_id != null;
    }

    public static boolean isArriveOrDepartureTimeDefined(StopTime stopTime) {
        return stopTime.arrival_time != INT_MISSING || stopTime.departure_time != INT_MISSING;
    }

    /**
     * Check if a trip contains at least one stop time that references a location group or location.
     */
    public static boolean hasFlexLocation(Trip trip, List<StopTime> stopTimes) {
        for (StopTime stopTime : stopTimes) {
            if (trip.trip_id.equals(stopTime.trip_id) && isLocationOrLocationGroupDefined(stopTime)) {
                return true;
            }
        }
        return false;
    }
}
