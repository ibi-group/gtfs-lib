package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.error.NewGTFSError;
import com.conveyal.gtfs.error.NewGTFSErrorType;
import com.conveyal.gtfs.model.BookingRule;
import com.conveyal.gtfs.model.FareRule;
import com.conveyal.gtfs.model.Location;
import com.conveyal.gtfs.model.LocationGroup;
import com.conveyal.gtfs.model.Route;
import com.conveyal.gtfs.model.Stop;
import com.conveyal.gtfs.model.StopTime;
import com.conveyal.gtfs.model.Trip;
import com.google.common.collect.Lists;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static com.conveyal.gtfs.error.NewGTFSErrorType.FLEX_FORBIDDEN_CONTINUOUS_DROP_OFF;
import static com.conveyal.gtfs.error.NewGTFSErrorType.FLEX_FORBIDDEN_CONTINUOUS_PICKUP;
import static com.conveyal.gtfs.error.NewGTFSErrorType.FLEX_FORBIDDEN_ROUTE_CONTINUOUS_DROP_OFF;
import static com.conveyal.gtfs.error.NewGTFSErrorType.FLEX_FORBIDDEN_ROUTE_CONTINUOUS_PICKUP;
import static com.conveyal.gtfs.model.Entity.INT_MISSING;
import static com.conveyal.gtfs.validator.FlexValidator.CONTINUOUS_PICKUP_DROP_OFF_ALLOWED;
import static com.conveyal.gtfs.validator.FlexValidator.CONTINUOUS_PICKUP_DROP_OFF_DISALLOWED;
import static com.conveyal.gtfs.validator.FlexValidator.CONTINUOUS_PICKUP_DROP_OFF_PHONE;
import static com.conveyal.gtfs.validator.FlexValidator.CONTINUOUS_PICKUP_DROP_OFF_TELL_DRIVER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FlexValidatorTest {

    @ParameterizedTest
    @MethodSource("createLocationGroupChecks")
    void validateLocationGroupTests(
        LocationGroup locationGroup,
        List<Stop> stops,
        List<Location> locations,
        List<NewGTFSErrorType> expectedErrors
    ) {
        List<NewGTFSError> errors = FlexValidator.validateLocationGroup(locationGroup, stops, locations);
        checkValidationErrorsMatchExpectedErrors(errors, expectedErrors);
    }

    private static Stream<Arguments> createLocationGroupChecks() {
        return Stream.of(
            Arguments.of(
                createLocationGroup("1"),
                Lists.newArrayList(createStop("2")),
                Lists.newArrayList(createLocation("3")),
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_LOCATION_GROUP_ID)
            ),
            Arguments.of(
                createLocationGroup("1"),
                Lists.newArrayList(createStop("2")),
                Lists.newArrayList(createLocation("1")),
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_DUPLICATE_LOCATION_GROUP_ID)
            ),
            Arguments.of(createLocationGroup("1"), null, null, null)
        );
    }

    @ParameterizedTest
    @MethodSource("createValidRouteContinuousStoppingChecks")
    void validRouteContinuousStoppingTests(Route route, List<StopTime> stopTimes) {
        List<Trip> trips = Lists.newArrayList(createTrip());
        assertTrue(FlexValidator.validateRoute(route, trips, stopTimes).isEmpty());
    }

    private static Stream<Arguments> createValidRouteContinuousStoppingChecks() {
        return Stream.of(
            Arguments.of(
                createRoute(INT_MISSING, INT_MISSING),
                Lists.newArrayList(createStopTime("trip-id-1", 1, 1))
            ),
            Arguments.of(
                createRoute(CONTINUOUS_PICKUP_DROP_OFF_DISALLOWED, INT_MISSING),
                Lists.newArrayList(createStopTime("trip-id-1", 1, 1))
            ),
            Arguments.of(
                createRoute(INT_MISSING, CONTINUOUS_PICKUP_DROP_OFF_DISALLOWED),
                Lists.newArrayList(createStopTime("trip-id-1", 1, 1))
            ),
            Arguments.of(
                createRoute(INT_MISSING, CONTINUOUS_PICKUP_DROP_OFF_DISALLOWED),
                Lists.newArrayList(createStopTime("trip-id-1", INT_MISSING, INT_MISSING))
            ),
            Arguments.of(
                createRoute(CONTINUOUS_PICKUP_DROP_OFF_DISALLOWED, INT_MISSING),
                Lists.newArrayList(createStopTime("trip-id-1", INT_MISSING, INT_MISSING))
            )
        );
    }

    @ParameterizedTest
    @ValueSource(ints = {
        CONTINUOUS_PICKUP_DROP_OFF_ALLOWED,
        CONTINUOUS_PICKUP_DROP_OFF_PHONE,
        CONTINUOUS_PICKUP_DROP_OFF_TELL_DRIVER
    })
    void invalidRouteContinuousStoppingTests(int continuousPickupDropOff) {
        checkInvalidRouteContinuousStopping(INT_MISSING, continuousPickupDropOff, FLEX_FORBIDDEN_ROUTE_CONTINUOUS_PICKUP);
        checkInvalidRouteContinuousStopping(continuousPickupDropOff, INT_MISSING, FLEX_FORBIDDEN_ROUTE_CONTINUOUS_DROP_OFF);
    }

    void checkInvalidRouteContinuousStopping(int dropOff, int pickup, NewGTFSErrorType errorType) {
        Route route = createRoute(dropOff, pickup);
        List<Trip> trips = Lists.newArrayList(createTrip());
        List<StopTime> stopTimes = Lists.newArrayList(createStopTime("trip-id-1", 1, 1));

        checkSingleError(
            FlexValidator.validateRoute(route, trips, stopTimes),
            errorType,
            pickup == INT_MISSING ? dropOff : pickup
        );
    }

    @ParameterizedTest
    @MethodSource("createLocationChecks")
    void validateLocationTests(
        Location location,
        List<Stop> stops,
        List<FareRule> fareRules,
        List<NewGTFSErrorType> expectedErrors
    ) {
        List<NewGTFSError> errors = FlexValidator.validateLocation(null, location, stops, fareRules);
        checkValidationErrorsMatchExpectedErrors(errors, expectedErrors);
    }

    private static Stream<Arguments> createLocationChecks() {
        return Stream.of(
            // Pass, no id conflicts.
            Arguments.of(
                createLocation("1", null),
                Lists.newArrayList(createStop("2")),
                null,
                null
            ),
            Arguments.of(
                createLocation("1", null),
                Lists.newArrayList(createStop("1")),
                null,
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_DUPLICATE_LOCATION_ID)
            ),
            // Pass, zone id is not required if no fare rules.
            Arguments.of(
                createLocation("1", "1"),
                Lists.newArrayList(createStop("2")),
                null,
                null
            ),
            Arguments.of(
                createLocation("1", "1"),
                Lists.newArrayList(createStop("2")),
                Lists.newArrayList(createFareRule("3", "", "")),
                Lists.newArrayList(NewGTFSErrorType.FLEX_MISSING_FARE_RULE)
            ),
            Arguments.of(
                createLocation("1", "1"),
                Lists.newArrayList(createStop("2")),
                Lists.newArrayList(createFareRule("", "3", "")),
                Lists.newArrayList(NewGTFSErrorType.FLEX_MISSING_FARE_RULE)
            ),
            Arguments.of(
                createLocation("1", "1"),
                Lists.newArrayList(createStop("2")),
                Lists.newArrayList(createFareRule("", "", "3")),
                Lists.newArrayList(NewGTFSErrorType.FLEX_MISSING_FARE_RULE)
            ),
            // Pass, zone id matches fare rule.
            Arguments.of(
                createLocation("1", "1"),
                Lists.newArrayList(createStop("2")),
                Lists.newArrayList(createFareRule("1", "", "")),
                null
            )
        );
    }

    @ParameterizedTest
    @MethodSource("createArrivalTimeTests")
    void validateArrivalTime(StopTime stopTime, List<NewGTFSErrorType> expectedErrors) {
        List<NewGTFSError> errors = new ArrayList<>();
        FlexValidator.validateArrivalTime(stopTime, errors);
        checkValidationErrorsMatchExpectedErrors(errors, expectedErrors);
    }

    private static Stream<Arguments> createArrivalTimeTests() {
        return Stream.of(
            Arguments.of(
                createStopTimeForArrivalTimeTest(INT_MISSING, INT_MISSING),
                null
            ),
            Arguments.of(
                createStopTimeForArrivalTimeTest(1200, INT_MISSING),
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_ARRIVAL_TIME)
            ),
            Arguments.of(
                createStopTimeForArrivalTimeTest(INT_MISSING, 1200),
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_ARRIVAL_TIME)
            )
        );
    }

    @ParameterizedTest
    @MethodSource("createDepartureTimeTests")
    void validateDepartureTime(StopTime stopTime, List<NewGTFSErrorType> expectedErrors) {
        List<NewGTFSError> errors = new ArrayList<>();
        FlexValidator.validateDepartureTime(stopTime, errors);
        checkValidationErrorsMatchExpectedErrors(errors, expectedErrors);
    }

    private static Stream<Arguments> createDepartureTimeTests() {
        return Stream.of(
            Arguments.of(
                createStopTimeForDepartureTimeTest(INT_MISSING, INT_MISSING),
                null
            ),
            Arguments.of(
                createStopTimeForDepartureTimeTest(1200, INT_MISSING),
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_DEPARTURE_TIME)
            ),
            Arguments.of(
                createStopTimeForDepartureTimeTest(INT_MISSING, 1200),
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_DEPARTURE_TIME)
            )
        );
    }

    @ParameterizedTest
    @MethodSource("createStopIdTests")
    void validateStopId(StopTime stopTime, List<NewGTFSErrorType> expectedErrors) {
        List<NewGTFSError> errors = new ArrayList<>();
        FlexValidator.validateStopId(stopTime, errors);
        checkValidationErrorsMatchExpectedErrors(errors, expectedErrors);
    }

    private static Stream<Arguments> createStopIdTests() {
        return Stream.of(
            Arguments.of(
                createStopTimeForStopIdTest("stop-id-1", null, null),
                null
            ),
            Arguments.of(
                createStopTimeForStopIdTest(null, null, null),
                Lists.newArrayList(NewGTFSErrorType.FLEX_REQUIRED_STOP_ID)
            ),
            Arguments.of(
                createStopTimeForStopIdTest("stop-id-1", "location-group-id-1", null),
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_STOP_ID)
            ),
            Arguments.of(
                createStopTimeForStopIdTest("stop-id-1", null, "location-id-1"),
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_STOP_ID)
            ),
            Arguments.of(
                createStopTimeForStopIdTest(null, null, "location-id-1"),
                null
            ),
            Arguments.of(
                createStopTimeForStopIdTest(null, "location-group-id-1", null),
                null
            )
        );
    }

    @ParameterizedTest
    @MethodSource("createLocationGroupIdTests")
    void validateLocationGroupId(StopTime stopTime, List<NewGTFSErrorType> expectedErrors) {
        List<NewGTFSError> errors = new ArrayList<>();
        FlexValidator.validateLocationGroupId(stopTime, errors);
        checkValidationErrorsMatchExpectedErrors(errors, expectedErrors);
    }

    private static Stream<Arguments> createLocationGroupIdTests() {
        return Stream.of(
            Arguments.of(
                createStopTimeForStopIdTest(null, "location-group-id-1", null),
                null
            ),
            Arguments.of(
                createStopTimeForStopIdTest(null, null, null),
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_LOCATION_GROUP_ID)
            ),
            Arguments.of(
                createStopTimeForStopIdTest("stop-id-1", "location-group-id-1", null),
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_LOCATION_GROUP_ID)
            ),
            Arguments.of(
                createStopTimeForStopIdTest(null, "location-group-id-1", "location-id-1"),
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_LOCATION_GROUP_ID)
            )
        );
    }

    @ParameterizedTest
    @MethodSource("createLocationIdTests")
    void validateLocationId(StopTime stopTime, List<NewGTFSErrorType> expectedErrors) {
        List<NewGTFSError> errors = new ArrayList<>();
        FlexValidator.validateLocationId(stopTime, errors);
        checkValidationErrorsMatchExpectedErrors(errors, expectedErrors);
    }

    private static Stream<Arguments> createLocationIdTests() {
        return Stream.of(
            Arguments.of(
                createStopTimeForStopIdTest(null, null, "location-id-1"),
                null
            ),
            Arguments.of(
                createStopTimeForStopIdTest(null, null, null),
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_LOCATION_ID)
            ),
            Arguments.of(
                createStopTimeForStopIdTest("stop-id-1", null, "location-id-1"),
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_LOCATION_ID)
            ),
            Arguments.of(
                createStopTimeForStopIdTest(null, "location-group-id-1", "location-id-1"),
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_LOCATION_ID)
            )
        );
    }

    @ParameterizedTest
    @MethodSource("createStartPickupDropOffWindowTests")
    void validateStartPickupDropOffWindowTest(StopTime stopTime, List<NewGTFSErrorType> expectedErrors) {
        List<NewGTFSError> errors = new ArrayList<>();
        FlexValidator.validateStartPickupDropOffWindow(stopTime, errors);
        checkValidationErrorsMatchExpectedErrors(errors, expectedErrors);
    }

    private static Stream<Arguments> createStartPickupDropOffWindowTests() {
        return Stream.of(
            Arguments.of(
                createStopTimeForStartPickupDropOffWindowTest(INT_MISSING, 1200, 1300),
                null
            ),
            Arguments.of(
                createStopTimeForStartPickupDropOffWindowTest(INT_MISSING, INT_MISSING, INT_MISSING),
                Lists.newArrayList(NewGTFSErrorType.FLEX_REQUIRED_START_PICKUP_DROP_OFF_WINDOW)
            ),
            Arguments.of(
                createStopTimeForStartPickupDropOffWindowTest(INT_MISSING, 1200, INT_MISSING),
                Lists.newArrayList(NewGTFSErrorType.FLEX_REQUIRED_START_PICKUP_DROP_OFF_WINDOW)
            ),
            Arguments.of(
                createStopTimeForStartPickupDropOffWindowTest(1200, 1200, 1300),
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_START_PICKUP_DROP_OFF_WINDOW)
            )
        );
    }

    @ParameterizedTest
    @MethodSource("createEndPickupDropOffWindowTests")
    void validateEndPickupDropOffWindowTest(StopTime stopTime, List<NewGTFSErrorType> expectedErrors) {
        List<NewGTFSError> errors = new ArrayList<>();
        FlexValidator.validateEndPickupDropOffWindow(stopTime, errors);
        checkValidationErrorsMatchExpectedErrors(errors, expectedErrors);
    }

    private static Stream<Arguments> createEndPickupDropOffWindowTests() {
        return Stream.of(
            Arguments.of(
                createStopTimeForStartPickupDropOffWindowTest(INT_MISSING, 1200, 1300),
                null
            ),
            Arguments.of(
                createStopTimeForStartPickupDropOffWindowTest(INT_MISSING, INT_MISSING, INT_MISSING),
                Lists.newArrayList(NewGTFSErrorType.FLEX_REQUIRED_END_PICKUP_DROP_OFF_WINDOW)
            ),
            Arguments.of(
                createStopTimeForStartPickupDropOffWindowTest(INT_MISSING, INT_MISSING, 1200),
                Lists.newArrayList(NewGTFSErrorType.FLEX_REQUIRED_END_PICKUP_DROP_OFF_WINDOW)
            ),
            Arguments.of(
                createStopTimeForStartPickupDropOffWindowTest(1200, 1200, 1300),
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_END_PICKUP_DROP_OFF_WINDOW)
            )
        );
    }

    @ParameterizedTest
    @MethodSource("createPickupTypeTests")
    void validatePickUpTypeTest(StopTime stopTime, List<NewGTFSErrorType> expectedErrors) {
        List<NewGTFSError> errors = new ArrayList<>();
        FlexValidator.validatePickUpType(stopTime, errors);
        checkValidationErrorsMatchExpectedErrors(errors, expectedErrors);
    }

    private static Stream<Arguments> createPickupTypeTests() {
        return Stream.of(
            Arguments.of(createStopTimeForPickupTypeTest(1), null),
            Arguments.of(createStopTimeForPickupTypeTest(0), null),
            Arguments.of(createStopTimeForPickupTypeTest(3), null),
            Arguments.of(
                createStopTimeForPickUpTypeTest(0, 1300),
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_PICKUP_TYPE)
            ),
            Arguments.of(
                createStopTimeForPickUpTypeTest(3, 1200),
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_PICKUP_TYPE)
            )
        );
    }

    @ParameterizedTest
    @MethodSource("createDropOffTests")
    void validateDropOffTest(StopTime stopTime, List<NewGTFSErrorType> expectedErrors) {
        List<NewGTFSError> errors = new ArrayList<>();
        FlexValidator.validateDropOffType(stopTime, errors);
        checkValidationErrorsMatchExpectedErrors(errors, expectedErrors);
    }

    private static Stream<Arguments> createDropOffTests() {
        return Stream.of(
            Arguments.of(createStopTimeForDropOffTypeTest(1, INT_MISSING), null),
            Arguments.of(createStopTimeForDropOffTypeTest(0, INT_MISSING), null),
            Arguments.of(createStopTimeForDropOffTypeTest(3, 1300), null),
            Arguments.of(
                createStopTimeForDropOffTypeTest(0, 1300),
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_DROP_OFF_TYPE)
            )
        );
    }

    @ParameterizedTest
    @MethodSource("createValidContinuousStopValues")
    void validContinuousPickupTest(StopTime stopTime) {
        List<NewGTFSError> errors = new ArrayList<>();
        FlexValidator.validateContinuousPickup(stopTime, errors);
        assertTrue(errors.isEmpty());
    }

    @ParameterizedTest
    @MethodSource("createValidContinuousStopValues")
    void validContinuousDropOffTest(StopTime stopTime) {
        List<NewGTFSError> errors = new ArrayList<>();
        FlexValidator.validateContinuousDropOff(stopTime, errors);
        assertTrue(errors.isEmpty());
    }

    private static Stream<Arguments> createValidContinuousStopValues() {
        return Stream.of(
            Arguments.of(createContinuousStopTime(INT_MISSING, CONTINUOUS_PICKUP_DROP_OFF_DISALLOWED)),
            Arguments.of(createContinuousStopTime(INT_MISSING, INT_MISSING)),
            // Permitted values for continuous_pickup/continuous_drop_off if the pickup/dropoff window is defined.
            Arguments.of(createContinuousStopTime(1300, INT_MISSING)),
            Arguments.of(createContinuousStopTime(1300, CONTINUOUS_PICKUP_DROP_OFF_DISALLOWED))
        );
    }

    @ParameterizedTest
    @ValueSource(ints = {
        CONTINUOUS_PICKUP_DROP_OFF_ALLOWED,
        CONTINUOUS_PICKUP_DROP_OFF_PHONE,
        CONTINUOUS_PICKUP_DROP_OFF_TELL_DRIVER
    })
    void invalidContinuousStopTimeTest(int continuousPickupDropOff) {
        StopTime stopTime = createContinuousStopTime(1300, continuousPickupDropOff);

        List<NewGTFSError> pickupErrors = new ArrayList<>();
        FlexValidator.validateContinuousPickup(stopTime, pickupErrors);
        checkSingleError(pickupErrors, FLEX_FORBIDDEN_CONTINUOUS_PICKUP, continuousPickupDropOff);

        List<NewGTFSError> dropOffErrors = new ArrayList<>();
        FlexValidator.validateContinuousDropOff(stopTime, dropOffErrors);
        checkSingleError(dropOffErrors, FLEX_FORBIDDEN_CONTINUOUS_DROP_OFF, continuousPickupDropOff);
    }

    @ParameterizedTest
    @MethodSource("createBookingRuleChecks")
    void validateBookingRuleTests(BookingRule bookingRule, List<NewGTFSErrorType> expectedErrors) {
        List<NewGTFSError> errors = FlexValidator.validateBookingRule(bookingRule);
        checkValidationErrorsMatchExpectedErrors(errors, expectedErrors);
    }

    private static Stream<Arguments> createBookingRuleChecks() {
        return Stream.of(
            Arguments.of(
                createBookingRule(INT_MISSING, 1, INT_MISSING, INT_MISSING, INT_MISSING, null, null),
                Lists.newArrayList(NewGTFSErrorType.FLEX_REQUIRED_PRIOR_NOTICE_DURATION_MIN)
            ),
            Arguments.of(
                createBookingRule(30, INT_MISSING, INT_MISSING, INT_MISSING, INT_MISSING, null, null),
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_PRIOR_NOTICE_DURATION_MIN)
            ),
            Arguments.of(
                createBookingRule(INT_MISSING, 0, 30, INT_MISSING, INT_MISSING, null, null),
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_PRIOR_NOTICE_DURATION_MAX)
            ),
            Arguments.of(
                createBookingRule(INT_MISSING, 2, 30, INT_MISSING, INT_MISSING, null, null),
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_PRIOR_NOTICE_DURATION_MAX, NewGTFSErrorType.FLEX_REQUIRED_PRIOR_NOTICE_LAST_DAY)
            ),
            Arguments.of(
                createBookingRule(INT_MISSING, 2, INT_MISSING, 1, INT_MISSING, null, null),
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_PRIOR_NOTICE_LAST_DAY)
            ),
            Arguments.of(
                createBookingRule(INT_MISSING, 0, INT_MISSING, INT_MISSING, 1, "07:00:00", null),
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_PRIOR_NOTICE_START_DAY_FOR_BOOKING_TYPE)
            ),
            Arguments.of(
                createBookingRule(30, 1, 30, INT_MISSING, 1, "10:30:00", null),
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_PRIOR_NOTICE_START_DAY)
            ),
            Arguments.of(
                createBookingRule(INT_MISSING, INT_MISSING, 30, INT_MISSING, 2, null, null),
                Lists.newArrayList(NewGTFSErrorType.FLEX_REQUIRED_PRIOR_NOTICE_START_TIME)
            ),
            Arguments.of(
                createBookingRule(INT_MISSING, INT_MISSING, INT_MISSING, INT_MISSING, INT_MISSING, "19:00:00", null),
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_PRIOR_START_TIME)
            ),
            Arguments.of(
                createBookingRule(INT_MISSING, 0, INT_MISSING, INT_MISSING, INT_MISSING, null, "1"),
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_PRIOR_NOTICE_SERVICE_ID)
            )
        );
    }

    @ParameterizedTest
    @MethodSource("createTripChecks")
    void validateTripTests(Trip trip, List<StopTime> stopTimes, List<NewGTFSErrorType> expectedErrors) {
        List<NewGTFSError> errors = FlexValidator.validateTrip(trip, stopTimes);
        checkValidationErrorsMatchExpectedErrors(errors, expectedErrors);
    }

    private static Stream<Arguments> createTripChecks() {
        Trip trip = createTrip();
        return Stream.of(
            Arguments.of(
                trip,
                Lists.newArrayList(createStopTime("1", null, null, trip.trip_id)),
                null
            ),
            Arguments.of(
                trip,
                Lists.newArrayList(createStopTime(null, "1", null, trip.trip_id)),
                Lists.newArrayList(NewGTFSErrorType.TRIP_SPEED_NOT_VALIDATED)
            ),
            Arguments.of(
                trip,
                Lists.newArrayList(createStopTime(null, null, "1", trip.trip_id)),
                Lists.newArrayList(NewGTFSErrorType.TRIP_SPEED_NOT_VALIDATED)
            )
        );
    }

    /**
     * Check that the errors produced by the flex validator match the expected errors. If no errors are expected, check
     * that no errors were produced. If errors are expected loop over the validation errors so as not to hide any
     * unexpected errors.
     */
    private void checkValidationErrorsMatchExpectedErrors(
        List<NewGTFSError> validationErrors,
        List<NewGTFSErrorType> expectedErrors
    ) {
        if (expectedErrors != null) {
            for (int i = 0; i < validationErrors.size(); i++) {
                assertEquals(expectedErrors.get(i), validationErrors.get(i).errorType);
            }
        } else {
            // No errors expected, so the reported errors should be empty.
            assertTrue(validationErrors.isEmpty());
        }
    }

    /**
     * Check that validation errors is just one error of a specific type and bad value.
     */
    private void checkSingleError(
        List<NewGTFSError> validationErrors,
        NewGTFSErrorType errorType,
        int badValue
    ) {
        assertFalse(validationErrors.isEmpty());
        assertEquals(1, validationErrors.size());
        assertEquals(errorType, validationErrors.get(0).errorType);
        assertEquals(Integer.toString(badValue), validationErrors.get(0).badValue);
    }

    private static FareRule createFareRule(String containsId, String destinationId, String originId) {
        FareRule fareRule = new FareRule();
        fareRule.contains_id = containsId;
        fareRule.destination_id = destinationId;
        fareRule.origin_id = originId;
        return fareRule;
    }

    private static BookingRule createBookingRule(
        int priorNoticeDurationMin,
        int bookingType,
        int priorNoticeDurationMax,
        int priorNoticeLastDay,
        int priorNoticeStartDay,
        String priorNoticeStartTime,
        String priorNoticeServiceId
    ) {
        BookingRule bookingRule = new BookingRule();
        bookingRule.prior_notice_duration_min = priorNoticeDurationMin;
        bookingRule.booking_type = bookingType;
        bookingRule.prior_notice_duration_max = priorNoticeDurationMax;
        bookingRule.prior_notice_last_day = priorNoticeLastDay;
        bookingRule.prior_notice_start_day = priorNoticeStartDay;
        bookingRule.prior_notice_start_time = priorNoticeStartTime;
        bookingRule.prior_notice_service_id = priorNoticeServiceId;
        return bookingRule;
    }

    private static Location createLocation(String locationId) {
        return createLocation(locationId, null);
    }

    private static Location createLocation(String locationId, String zoneId) {
        Location location = new Location();
        location.location_id = locationId;
        location.zone_id = zoneId;
        return location;
    }

    private static LocationGroup createLocationGroup(String locationGroupId) {
        LocationGroup locationGroup = new LocationGroup();
        locationGroup.location_group_id = locationGroupId;
        return locationGroup;
    }

    private static Stop createStop(String stopId) {
        Stop stop = new Stop();
        stop.stop_id = stopId;
        return stop;
    }

    private static StopTime createStopTime(String stopId, String locationId, String locationGroupId, String tripId) {
        StopTime stopTime = new StopTime();
        stopTime.stop_id = stopId;
        stopTime.location_id = locationId;
        stopTime.location_group_id = locationGroupId;
        stopTime.trip_id = tripId;
        return stopTime;
    }

    private static StopTime createStopTime(String tripId, int startPickupDropOffWindow, int endPickupDropOffWindow) {
        StopTime stopTime = new StopTime();
        stopTime.trip_id = tripId;
        stopTime.start_pickup_drop_off_window = startPickupDropOffWindow;
        stopTime.end_pickup_drop_off_window = endPickupDropOffWindow;
        return stopTime;
    }

    private static StopTime createStopTimeForArrivalTimeTest(
        int endPickupDropOffWindow,
        int timePoint
    ) {
        StopTime stopTime = new StopTime();
        stopTime.arrival_time = 1130;
        stopTime.start_pickup_drop_off_window = com.conveyal.gtfs.model.Entity.INT_MISSING;
        stopTime.end_pickup_drop_off_window = endPickupDropOffWindow;
        stopTime.timepoint = timePoint;
        return stopTime;
    }
    private static StopTime createStopTimeForDepartureTimeTest(
        int endPickupDropOffWindow,
        int timePoint
    ) {
        StopTime stopTime = new StopTime();
        stopTime.departure_time = 1130;
        stopTime.start_pickup_drop_off_window = com.conveyal.gtfs.model.Entity.INT_MISSING;
        stopTime.end_pickup_drop_off_window = endPickupDropOffWindow;
        stopTime.timepoint = timePoint;
        return stopTime;
    }

    private static StopTime createStopTimeForStopIdTest(
        String stopId,
        String locationGroupId,
        String locationId
    ) {
        StopTime stopTime = new StopTime();
        stopTime.stop_id = stopId;
        stopTime.location_group_id = locationGroupId;
        stopTime.location_id = locationId;
        return stopTime;
    }

    private static StopTime createStopTimeForDropOffTypeTest(
        int dropOffType,
        int startPickupDropOffWindow
    ) {
        StopTime stopTime = new StopTime();
        stopTime.drop_off_type = dropOffType;
        stopTime.start_pickup_drop_off_window = startPickupDropOffWindow;
        return stopTime;
    }

    private static StopTime createContinuousStopTime(
        int startPickupDropOffWindow,
        int continuousPickupOrDropOff
    ) {
        StopTime stopTime = new StopTime();
        stopTime.start_pickup_drop_off_window = startPickupDropOffWindow;
        stopTime.continuous_pickup = continuousPickupOrDropOff;
        stopTime.continuous_drop_off = continuousPickupOrDropOff;
        return stopTime;
    }

    private static StopTime createStopTimeForPickUpTypeTest(int pickupType, int startPickupDropOffWindow) {
        StopTime stopTime = new StopTime();
        stopTime.pickup_type = pickupType;
        stopTime.start_pickup_drop_off_window = startPickupDropOffWindow;
        return stopTime;
    }

    private static StopTime createStopTimeForStartPickupDropOffWindowTest(
        int arrivalTime,
        int startPickupDropOffWindow,
        int endPickupDropOffWindow
    ) {
        StopTime stopTime = new StopTime();
        stopTime.arrival_time = arrivalTime;
        stopTime.location_group_id = "location-group-id-1";
        stopTime.start_pickup_drop_off_window = startPickupDropOffWindow;
        stopTime.end_pickup_drop_off_window = endPickupDropOffWindow;
        return stopTime;
    }

    private static StopTime createStopTimeForPickupTypeTest(int pickupType) {
        StopTime stopTime = new StopTime();
        stopTime.pickup_type = pickupType;
        stopTime.start_pickup_drop_off_window = com.conveyal.gtfs.model.Entity.INT_MISSING;
        return stopTime;
    }

    private static Route createRoute(int continuousDropOff, int continuousPickup) {
        Route route = new Route();
        route.route_id = "route-id-1";
        route.continuous_drop_off = continuousDropOff;
        route.continuous_pickup = continuousPickup;
        return route;
    }

    private static Trip createTrip() {
        Trip trip = new Trip();
        trip.trip_id = "trip-id-1";
        trip.route_id = "route-id-1";
        return trip;
    }
}
