package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.error.NewGTFSError;
import com.conveyal.gtfs.error.NewGTFSErrorType;
import com.conveyal.gtfs.model.BookingRule;
import com.conveyal.gtfs.model.FareRule;
import com.conveyal.gtfs.model.Location;
import com.conveyal.gtfs.model.LocationGroup;
import com.conveyal.gtfs.model.Stop;
import com.conveyal.gtfs.model.StopTime;
import com.conveyal.gtfs.model.Trip;
import com.google.common.collect.Lists;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static com.conveyal.gtfs.model.Entity.INT_MISSING;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FlexValidatorTest {

    @ParameterizedTest
    @MethodSource("createLocationGroupChecks")
    void validateLocationGroupTests(LocationGroupArguments locationGroupArguments) {
        List<NewGTFSError> errors = FlexValidator.validateLocationGroup(
            (LocationGroup) locationGroupArguments.testObject,
            locationGroupArguments.stops,
            locationGroupArguments.locations
        );
        checkValidationErrorsMatchExpectedErrors(errors, locationGroupArguments.expectedErrors);
    }

    private static Stream<LocationGroupArguments> createLocationGroupChecks() {
        return Stream.of(
            new LocationGroupArguments(
                "1","2", "3",
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_LOCATION_GROUP_ID)
            ),
            new LocationGroupArguments(
                "1","2", "1",
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_DUPLICATE_LOCATION_GROUP_ID)
            ),
            new LocationGroupArguments(
                "2","2", "1",
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_DUPLICATE_LOCATION_GROUP_ID)
            ),
            new LocationGroupArguments(
                "1",null, null,
                null
            )
        );
    }

    @ParameterizedTest
    @MethodSource("createLocationChecks")
    void validateLocationTests(LocationArguments locationArguments) {
        List<NewGTFSError> errors = FlexValidator.validateLocation(
            null,
            (Location) locationArguments.testObject,
            locationArguments.stops,
            locationArguments.fareRules
        );
        checkValidationErrorsMatchExpectedErrors(errors, locationArguments.expectedErrors);
    }

    private static Stream<LocationArguments> createLocationChecks() {
        return Stream.of(
            // Pass, no id conflicts
            new LocationArguments(
                createLocation("1", null),"2", null, null, null,
                null
            ),
            new LocationArguments(
                createLocation("1", null),"1", null, null, null,
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_DUPLICATE_LOCATION_ID)
            ),
            // Pass, zone id is not required if no fare rules
            new LocationArguments(
                createLocation("1", "1"),"2", null, null, null,
                null
            ),
            new LocationArguments(
                createLocation("1", "1"),"2", "3", "", "",
                Lists.newArrayList(NewGTFSErrorType.FLEX_MISSING_FARE_RULE)
            ),
            new LocationArguments(
                createLocation("1", "1"),"2", "", "3", "",
                Lists.newArrayList(NewGTFSErrorType.FLEX_MISSING_FARE_RULE)
            ),
            new LocationArguments(
                createLocation("1", "1"),"2", "", "", "3",
                Lists.newArrayList(NewGTFSErrorType.FLEX_MISSING_FARE_RULE)
            ),
            // Pass, zone id matches fare rule
            new LocationArguments(
                createLocation("1", "1"),"2", "1", "", "",
                null
            )
        );
    }

    @ParameterizedTest
    @MethodSource("createArrivalTimeTests")
    void validateArrivalTime(BaseArguments baseArguments) {
        List<NewGTFSError> errors = new ArrayList<>();
        FlexValidator.validateArrivalTime((StopTime) baseArguments.testObject, errors);
        checkValidationErrorsMatchExpectedErrors(errors, baseArguments.expectedErrors);
    }

    private static Stream<BaseArguments> createArrivalTimeTests() {
        return Stream.of(
            new BaseArguments(
                createStopTimeForArrivalTimeTest(1130, INT_MISSING, INT_MISSING, INT_MISSING),null
            ),
            new BaseArguments(
                createStopTimeForArrivalTimeTest(1130, INT_MISSING, 1200, INT_MISSING),
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_ARRIVAL_TIME)
            ),
            new BaseArguments(
                createStopTimeForArrivalTimeTest(1130, INT_MISSING, INT_MISSING, 1200),
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_ARRIVAL_TIME)
            )
        );
    }

    @ParameterizedTest
    @MethodSource("createDepartureTimeTests")
    void validateDepartureTime(BaseArguments baseArguments) {
        List<NewGTFSError> errors = new ArrayList<>();
        FlexValidator.validateDepartureTime((StopTime) baseArguments.testObject, errors);
        checkValidationErrorsMatchExpectedErrors(errors, baseArguments.expectedErrors);
    }

    private static Stream<BaseArguments> createDepartureTimeTests() {
        return Stream.of(
            new BaseArguments(
                createStopTimeForDepartureTimeTest(1130, INT_MISSING, INT_MISSING, INT_MISSING),null
            ),
            new BaseArguments(
                createStopTimeForDepartureTimeTest(1130, INT_MISSING, 1200, INT_MISSING),
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_DEPARTURE_TIME)
            ),
            new BaseArguments(
                createStopTimeForDepartureTimeTest(1130, INT_MISSING, INT_MISSING, 1200),
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_DEPARTURE_TIME)
            )
        );
    }

    @ParameterizedTest
    @MethodSource("createStopIdTests")
    void validateStopId(BaseArguments baseArguments) {
        List<NewGTFSError> errors = new ArrayList<>();
        FlexValidator.validateStopId((StopTime) baseArguments.testObject, errors);
        checkValidationErrorsMatchExpectedErrors(errors, baseArguments.expectedErrors);
    }

    private static Stream<BaseArguments> createStopIdTests() {
        return Stream.of(
            new BaseArguments(
                createStopTimeForStopIdTest("stop-id-1", null, null),null
            ),
            new BaseArguments(
                createStopTimeForStopIdTest(null, null, null),
                Lists.newArrayList(NewGTFSErrorType.FLEX_REQUIRED_STOP_ID)
            ),
            new BaseArguments(
                createStopTimeForStopIdTest("stop-id-1", "location-group-id-1", null),
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_STOP_ID)
            ),
            new BaseArguments(
                createStopTimeForStopIdTest("stop-id-1", null, "location-id-1"),
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_STOP_ID)
            )
        );
    }

    @ParameterizedTest
    @MethodSource("createLocationGroupIdTests")
    void validateLocationGroupId(BaseArguments baseArguments) {
        List<NewGTFSError> errors = new ArrayList<>();
        FlexValidator.validateLocationGroupId((StopTime) baseArguments.testObject, errors);
        checkValidationErrorsMatchExpectedErrors(errors, baseArguments.expectedErrors);
    }

    private static Stream<BaseArguments> createLocationGroupIdTests() {
        return Stream.of(
            new BaseArguments(
                createStopTimeForStopIdTest(null, "location-group-id-1", null),null
            ),
            new BaseArguments(
                createStopTimeForStopIdTest(null, null, null),
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_LOCATION_GROUP_ID)
            ),
            new BaseArguments(
                createStopTimeForStopIdTest("stop-id-1", "location-group-id-1", null),
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_LOCATION_GROUP_ID)
            ),
            new BaseArguments(
                createStopTimeForStopIdTest(null, "location-group-id-1", "location-id-1"),
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_LOCATION_GROUP_ID)
            )
        );
    }

    @ParameterizedTest
    @MethodSource("createLocationIdTests")
    void validateLocationId(BaseArguments baseArguments) {
        List<NewGTFSError> errors = new ArrayList<>();
        FlexValidator.validateLocationId((StopTime) baseArguments.testObject, errors);
        checkValidationErrorsMatchExpectedErrors(errors, baseArguments.expectedErrors);
    }

    private static Stream<BaseArguments> createLocationIdTests() {
        return Stream.of(
            new BaseArguments(
                createStopTimeForStopIdTest(null, null, "location-id-1"), null
            ),
            new BaseArguments(
                createStopTimeForStopIdTest(null, null, null),
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_LOCATION_ID)
            ),
            new BaseArguments(
                createStopTimeForStopIdTest("stop-id-1", null, "location-id-1"),
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_LOCATION_ID)
            ),
            new BaseArguments(
                createStopTimeForStopIdTest(null, "location-group-id-1", "location-id-1"),
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_LOCATION_ID)
            )
        );
    }

    @ParameterizedTest
    @MethodSource("createStartPickupDropOffWindowTests")
    void validateStartPickupDropOffWindowTest(BaseArguments baseArguments) {
        List<NewGTFSError> errors = new ArrayList<>();
        FlexValidator.validateStartPickupDropOffWindow((StopTime) baseArguments.testObject, errors);
        checkValidationErrorsMatchExpectedErrors(errors, baseArguments.expectedErrors);
    }

    private static Stream<BaseArguments> createStartPickupDropOffWindowTests() {
        return Stream.of(
            new BaseArguments(
                createStopTimeForStartPickupDropOffWindowTest(INT_MISSING, "location-group-id-1", 1200, 1300), null
            ),
            new BaseArguments(
                createStopTimeForStartPickupDropOffWindowTest(INT_MISSING, "location-group-id-1", INT_MISSING, INT_MISSING),
                Lists.newArrayList(NewGTFSErrorType.FLEX_REQUIRED_START_PICKUP_DROP_OFF_WINDOW)
            ),
            new BaseArguments(
                createStopTimeForStartPickupDropOffWindowTest(INT_MISSING, "location-group-id-1", 1200, INT_MISSING),
                Lists.newArrayList(NewGTFSErrorType.FLEX_REQUIRED_START_PICKUP_DROP_OFF_WINDOW)
            ),
            new BaseArguments(
                createStopTimeForStartPickupDropOffWindowTest(1200, "location-group-id-1", 1200, 1300),
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_START_PICKUP_DROP_OFF_WINDOW)
            )
        );
    }

    @ParameterizedTest
    @MethodSource("createEndPickupDropOffWindowTests")
    void validateEndPickupDropOffWindowTest(BaseArguments baseArguments) {
        List<NewGTFSError> errors = new ArrayList<>();
        FlexValidator.validateEndPickupDropOffWindow((StopTime) baseArguments.testObject, errors);
        checkValidationErrorsMatchExpectedErrors(errors, baseArguments.expectedErrors);
    }

    private static Stream<BaseArguments> createEndPickupDropOffWindowTests() {
        return Stream.of(
            new BaseArguments(
                createStopTimeForStartPickupDropOffWindowTest(INT_MISSING, "location-group-id-1", 1200, 1300), null
            ),
            new BaseArguments(
                createStopTimeForStartPickupDropOffWindowTest(INT_MISSING, "location-group-id-1", INT_MISSING, INT_MISSING),
                Lists.newArrayList(NewGTFSErrorType.FLEX_REQUIRED_END_PICKUP_DROP_OFF_WINDOW)
            ),
            new BaseArguments(
                createStopTimeForStartPickupDropOffWindowTest(INT_MISSING, "location-group-id-1", INT_MISSING, 1200),
                Lists.newArrayList(NewGTFSErrorType.FLEX_REQUIRED_END_PICKUP_DROP_OFF_WINDOW)
            ),
            new BaseArguments(
                createStopTimeForStartPickupDropOffWindowTest(1200, "location-group-id-1", 1200, 1300),
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_END_PICKUP_DROP_OFF_WINDOW)
            )
        );
    }

    @ParameterizedTest
    @MethodSource("createPickupTypeTests")
    void validatePickUpTypeTest(BaseArguments baseArguments) {
        List<NewGTFSError> errors = new ArrayList<>();
        FlexValidator.validatePickUpType((StopTime) baseArguments.testObject, errors);
        checkValidationErrorsMatchExpectedErrors(errors, baseArguments.expectedErrors);
    }

    private static Stream<BaseArguments> createPickupTypeTests() {
        return Stream.of(
            new BaseArguments(
                createStopTimeForPickupTypeTest(1, INT_MISSING), null
            ),
            new BaseArguments(
                createStopTimeForPickupTypeTest(0, INT_MISSING), null
            ),
            new BaseArguments(
                createStopTimeForPickupTypeTest(3, INT_MISSING), null
            ),
            new BaseArguments(
                createStopTimeForPickUpTypeTest(0, 1300),
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_PICKUP_TYPE)
            ),
            new BaseArguments(
                createStopTimeForPickUpTypeTest(3, 1200),
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_PICKUP_TYPE)
            )
        );
    }

    @ParameterizedTest
    @MethodSource("createDropOffTests")
    void validateDropOffTest(BaseArguments baseArguments) {
        List<NewGTFSError> errors = new ArrayList<>();
        FlexValidator.validateDropOffType((StopTime) baseArguments.testObject, errors);
        checkValidationErrorsMatchExpectedErrors(errors, baseArguments.expectedErrors);
    }

    private static Stream<BaseArguments> createDropOffTests() {
        return Stream.of(
            new BaseArguments(
                createStopTimeForDropOffTypeTest(1, INT_MISSING), null
            ),
            new BaseArguments(
                createStopTimeForDropOffTypeTest(0, INT_MISSING), null
            ),
            new BaseArguments(
                createStopTimeForDropOffTypeTest(3, 1300), null
            ),
            new BaseArguments(
                createStopTimeForDropOffTypeTest(0, 1300),
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_DROP_OFF_TYPE)
            )
        );
    }

    @ParameterizedTest
    @MethodSource("createContinuousPickupTests")
    void validateContinuousPickupTest(BaseArguments baseArguments) {
        List<NewGTFSError> errors = new ArrayList<>();
        FlexValidator.validateContinuousPickup((StopTime) baseArguments.testObject, errors);
        checkValidationErrorsMatchExpectedErrors(errors, baseArguments.expectedErrors);
    }

    private static Stream<BaseArguments> createContinuousPickupTests() {
        return Stream.of(
            new BaseArguments(
                createStopTimeForContinuousPickupTest(INT_MISSING), null
            ),
            new BaseArguments(
                createStopTimeForContinuousPickupTest(1300),
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_CONTINUOUS_PICKUP)
            )
        );
    }

    @ParameterizedTest
    @MethodSource("createContinuousDropOffTests")
    void validateContinuousDropOffTest(BaseArguments baseArguments) {
        List<NewGTFSError> errors = new ArrayList<>();
        FlexValidator.validateContinuousDropOff((StopTime) baseArguments.testObject, errors);
        checkValidationErrorsMatchExpectedErrors(errors, baseArguments.expectedErrors);
    }

    private static Stream<BaseArguments> createContinuousDropOffTests() {
        return Stream.of(
            new BaseArguments(
                createStopTimeForContinuousPickupTest(INT_MISSING), null
            ),
            new BaseArguments(
                createStopTimeForContinuousPickupTest(1300),
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_CONTINUOUS_DROP_OFF)
            )
        );
    }

    @ParameterizedTest
    @MethodSource("createBookingRuleChecks")
    void validateBookingRuleTests(BaseArguments baseArguments) {
        List<NewGTFSError> errors = FlexValidator.validateBookingRule((BookingRule) baseArguments.testObject);
        checkValidationErrorsMatchExpectedErrors(errors, baseArguments.expectedErrors);
    }

    private static Stream<BaseArguments> createBookingRuleChecks() {
        return Stream.of(
            new BaseArguments(
                createBookingRule(INT_MISSING, 1, INT_MISSING, INT_MISSING, INT_MISSING, null, null),
                Lists.newArrayList(NewGTFSErrorType.FLEX_REQUIRED_PRIOR_NOTICE_DURATION_MIN)
            ),
            new BaseArguments(
                createBookingRule(30, INT_MISSING, INT_MISSING, INT_MISSING, INT_MISSING, null, null),
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_PRIOR_NOTICE_DURATION_MIN)
            ),
            new BaseArguments(
                createBookingRule(INT_MISSING, 0, 30, INT_MISSING, INT_MISSING, null, null),
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_PRIOR_NOTICE_DURATION_MAX)
            ),
            new BaseArguments(
                createBookingRule(INT_MISSING, 2, 30, INT_MISSING, INT_MISSING, null, null),
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_PRIOR_NOTICE_DURATION_MAX, NewGTFSErrorType.FLEX_REQUIRED_PRIOR_NOTICE_LAST_DAY)
            ),
            new BaseArguments(
                createBookingRule(INT_MISSING, 2, INT_MISSING, 1, INT_MISSING, null, null),
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_PRIOR_NOTICE_LAST_DAY)
            ),
            new BaseArguments(
                createBookingRule(INT_MISSING, 0, INT_MISSING, INT_MISSING, 1, "07:00:00", null),
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_PRIOR_NOTICE_START_DAY_FOR_BOOKING_TYPE)
            ),
            new BaseArguments(
                createBookingRule(30, 1, 30, INT_MISSING, 1, "10:30:00", null),
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_PRIOR_NOTICE_START_DAY)
            ),
            new BaseArguments(
                createBookingRule(INT_MISSING, INT_MISSING, 30, INT_MISSING, 2, null, null),
                Lists.newArrayList(NewGTFSErrorType.FLEX_REQUIRED_PRIOR_NOTICE_START_TIME)
            ),
            new BaseArguments(
                createBookingRule(INT_MISSING, INT_MISSING, INT_MISSING, INT_MISSING, INT_MISSING, "19:00:00", null),
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_PRIOR_START_TIME)
            ),
            new BaseArguments(
                createBookingRule(INT_MISSING, 0, INT_MISSING, INT_MISSING, INT_MISSING, null, "1"),
                Lists.newArrayList(NewGTFSErrorType.FLEX_FORBIDDEN_PRIOR_NOTICE_SERVICE_ID)
            )
        );
    }

    @ParameterizedTest
    @MethodSource("createTripChecks")
    void validateTripTests(TripArguments tripArguments) {
        List<NewGTFSError> errors = FlexValidator.validateTrip(
            (Trip) tripArguments.testObject,
            tripArguments.stopTimes
        );
        checkValidationErrorsMatchExpectedErrors(errors, tripArguments.expectedErrors);
    }

    private static Stream<TripArguments> createTripChecks() {
        return Stream.of(
            new TripArguments(
                createTrip(), "1", "0","0",
                null
            ),
            new TripArguments(
                createTrip(), "1", "1","0",
                Lists.newArrayList(NewGTFSErrorType.TRIP_SPEED_NOT_VALIDATED)
            ),
            new TripArguments(
                createTrip(), "1", "0","1",
                Lists.newArrayList(NewGTFSErrorType.TRIP_SPEED_NOT_VALIDATED)
            )
        );
    }

    private static class BaseArguments {
        public Object testObject;
        public List<NewGTFSErrorType> expectedErrors;

        private BaseArguments(Object testData, List<NewGTFSErrorType> expectedErrors) {
            this.testObject = testData;
            this.expectedErrors = expectedErrors;
        }
    }

    private static class StopTimeArguments extends BaseArguments {
        public final List<LocationGroup> locationGroups;
        public final List<Location> locations;

        private StopTimeArguments(
            Object stopTime,
            String locationId,
            String locationGroupId,
            List<NewGTFSErrorType> expectedErrors
        ) {
            super(stopTime, expectedErrors);
            this.locationGroups = (locationGroupId != null) ? Lists.newArrayList(createLocationGroup(locationGroupId)) : null;
            this.locations = (locationId != null) ? Lists.newArrayList(createLocation(locationId)) : null;
       }
    }

    private static class LocationArguments extends BaseArguments {
        public final List<Stop> stops;
        public List<FareRule> fareRules = new ArrayList<>();

        private LocationArguments(
            Object location,
            String stopId,
            String containsId,
            String destinationId,
            String originId,
            List<NewGTFSErrorType> expectedErrors
        ) {
            super(location, expectedErrors);
            this.stops = Lists.newArrayList(createStop(stopId));
            if (containsId != null || destinationId != null || originId != null) {
                FareRule fareRule = new FareRule();
                fareRule.contains_id = containsId;
                fareRule.destination_id = destinationId;
                fareRule.origin_id = originId;
                fareRules = Lists.newArrayList(fareRule);
            }

        }
    }

    private static class LocationGroupArguments extends BaseArguments {
        public final List<Stop> stops;
        public List<Location> locations;

        private LocationGroupArguments(
            String locationGroupId,
            String locationId,
            String stopId,
            List<NewGTFSErrorType> expectedErrors
        ) {
            super(createLocationGroup(locationGroupId), expectedErrors);
            this.stops = Lists.newArrayList(createStop(stopId));
            this.locations = Lists.newArrayList(createLocation(locationId));
        }
    }

    private static class TripArguments extends BaseArguments {
        public final List<StopTime> stopTimes;
        public final List<LocationGroup> locationGroups;
        public final List<Location> locations;

        private TripArguments(
            Object trip,
            String stopId,
            String locationId,
            String locationGroupId,
            List<NewGTFSErrorType> expectedErrors
        ) {
            super(trip, expectedErrors);
            this.stopTimes = (stopId != null) ? Lists.newArrayList(createStopTime(stopId, ((Trip)trip).trip_id)) : null;
            this.locationGroups = (locationGroupId != null) ? Lists.newArrayList(createLocationGroup(locationGroupId)) : null;
            this.locations = (locationId != null) ? Lists.newArrayList(createLocation(locationId)) : null;
        }
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

    private static StopTime createStopTime(String stopId, String tripId) {
        StopTime stopTime = new StopTime();
        stopTime.stop_id = stopId;
        stopTime.trip_id = tripId;
        return stopTime;
    }

    private static StopTime createStopTimeForArrivalTimeTest(
        int arrivalTime,
        int startPickupDropOffWindow,
        int endPickupDropOffWindow,
        int timePoint
    ) {
        StopTime stopTime = new StopTime();
        stopTime.arrival_time = arrivalTime;
        stopTime.start_pickup_drop_off_window = startPickupDropOffWindow;
        stopTime.end_pickup_drop_off_window = endPickupDropOffWindow;
        stopTime.timepoint = timePoint;
        return stopTime;
    }
    private static StopTime createStopTimeForDepartureTimeTest(
        int departureTime,
        int startPickupDropOffWindow,
        int endPickupDropOffWindow,
        int timePoint
    ) {
        StopTime stopTime = new StopTime();
        stopTime.departure_time = departureTime;
        stopTime.start_pickup_drop_off_window = startPickupDropOffWindow;
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

    private static StopTime createStopTimeForContinuousPickupTest(
        int startPickupDropOffWindow
    ) {
        StopTime stopTime = new StopTime();
        stopTime.start_pickup_drop_off_window = startPickupDropOffWindow;
        return stopTime;
    }

    private static StopTime createStopTimeForPickUpTypeTest(
        int pickupType,
        int startPickupDropOffWindow
    ) {
        StopTime stopTime = new StopTime();
        stopTime.pickup_type = pickupType;
        stopTime.start_pickup_drop_off_window = startPickupDropOffWindow;
        return stopTime;
    }

    private static StopTime createStopTimeForStartPickupDropOffWindowTest(
        int arrivalTime,
        String locationGroupId,
        int startPickupDropOffWindow,
        int endPickupDropOffWindow
    ) {
        StopTime stopTime = new StopTime();
        stopTime.arrival_time = arrivalTime;
        stopTime.location_group_id = locationGroupId;
        stopTime.start_pickup_drop_off_window = startPickupDropOffWindow;
        stopTime.end_pickup_drop_off_window = endPickupDropOffWindow;
        return stopTime;
    }

    private static StopTime createStopTimeForPickupTypeTest(
        int pickupType,
        int startPickupDropOffWindow
    ) {
        StopTime stopTime = new StopTime();
        stopTime.pickup_type = pickupType;
        stopTime.start_pickup_drop_off_window = startPickupDropOffWindow;
        return stopTime;
    }

    private static Trip createTrip() {
        Trip trip = new Trip();
        trip.trip_id = "12345";
        return trip;
    }
}
