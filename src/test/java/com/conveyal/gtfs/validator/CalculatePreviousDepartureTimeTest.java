package com.conveyal.gtfs.validator;

import com.beust.jcommander.internal.Lists;
import com.conveyal.gtfs.PatternBuilder;
import com.conveyal.gtfs.TripPatternKey;
import com.conveyal.gtfs.model.StopTime;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CalculatePreviousDepartureTimeTest {
    private final PatternBuilder patternBuilder;

    CalculatePreviousDepartureTimeTest() {
        patternBuilder = new PatternBuilder();
    }

    @ParameterizedTest
    @MethodSource("createTrips")
    void calculatePreviousDepartureTimeForTrip(
        TripPatternKey key,
        List<Integer> expectedDepartureTimes,
        String message
    ) {
        List<Integer> actualDepartureTimes = patternBuilder.calculatePreviousDepartureTimes(key);
        assertEquals(expectedDepartureTimes, actualDepartureTimes, message);
    }

    /**
     * Produce the required trips for testing.
     */
    private static Stream<Arguments> createTrips() {
        // Confirm that a trip that consists of just normal stops will produce the correct departure times.
        TripPatternKey normalStopTripPatternKey = new TripPatternKey("test-route");
        normalStopTripPatternKey.addStopTime(createStopTime("stop-id-1", 2));
        normalStopTripPatternKey.addStopTime(createStopTime("stop-id-2", 3));
        normalStopTripPatternKey.addStopTime(createStopTime("stop-id-3", 4));
        normalStopTripPatternKey.addStopTime(createStopTime("stop-id-4", 5));
        normalStopTripPatternKey.addStopTime(createStopTime("stop-id-5", 6));

        List<Integer> pointToPointExpectedDepartures = Lists.newArrayList(0, 2, 3, 4, 5);

        // Confirm that a trip that consists of just flex stops will produce the correct departure times.
        TripPatternKey flexStopTripPatternKey = new TripPatternKey("test-route");
        flexStopTripPatternKey.addStopTime(createFlexStopTime("stop-id-1", 600));
        flexStopTripPatternKey.addStopTime(createFlexStopTime("stop-id-2", 720));

        List<Integer> flexExpectedDepartures = Lists.newArrayList(0, 0);

        // Confirm that a combination of normal and flex stops will produce the correct departure times.
        TripPatternKey flexAndPointKey = new TripPatternKey("test-route");
        flexAndPointKey.addStopTime(createStopTime("stop-id-1", 2));
        flexAndPointKey.addStopTime(createFlexStopTime("stop-id-2", 600));
        flexAndPointKey.addStopTime(createFlexStopTime("stop-id-3",720));
        flexAndPointKey.addStopTime(createStopTime("stop-id-4", 722));
        flexAndPointKey.addStopTime(createFlexStopTime("stop-id-5", 900));
        flexAndPointKey.addStopTime(createStopTime("stop-id-6", 903));
        flexAndPointKey.addStopTime(createStopTime("stop-id-7", 904));

        List<Integer> flexAndPointExpectedDepartures = Lists.newArrayList(0, 2, 0, 720, 722, 900, 903);

        // Confirm that a trip that consists of invalid departure times will still produce the correct departure times.
        TripPatternKey pointToPointWithInvalidDeparturesKey = new TripPatternKey("test-route");
        pointToPointWithInvalidDeparturesKey.addStopTime(createStopTime("stop-id-1", 2));
        pointToPointWithInvalidDeparturesKey.addStopTime(createStopTime("stop-id-2", 3));
        pointToPointWithInvalidDeparturesKey.addStopTime(createStopTime("stop-id-3", 0));
        pointToPointWithInvalidDeparturesKey.addStopTime(createStopTime("stop-id-4", 1));
        pointToPointWithInvalidDeparturesKey.addStopTime(createStopTime("stop-id-5", 6));

        List<Integer> pointToPointWithInvalidDepartures = Lists.newArrayList(0, 2, 3, 3, 3);

        return Stream.of(
            Arguments.of(
                normalStopTripPatternKey,
                pointToPointExpectedDepartures,
                "A trip that consists of just normal stops will produce the correct departure times."
            ),
            Arguments.of(
                flexStopTripPatternKey,
                flexExpectedDepartures,
                "A trip that consists of just flex stops will produce the correct departure times."
            ),
            Arguments.of(
                flexAndPointKey,
                flexAndPointExpectedDepartures,
                "A combination of normal and flex stops will produce the correct departure times."
            ),
            Arguments.of(
                pointToPointWithInvalidDeparturesKey,
                pointToPointWithInvalidDepartures,
                "A trip that consists of invalid departure times will still produce the correct departure times."
            )
        );
    }

    private static StopTime createStopTime(String id, int departureTime) {
        return createStopTime(id, departureTime,0, true);
    }

    private static StopTime createFlexStopTime(String id, int endPickupDropOffWindow) {
        return createStopTime(id, 0, endPickupDropOffWindow, false);
    }

    private static StopTime createStopTime(
        String id,
        int departureTime,
        int endPickupDropOffWindow,
        boolean isNormalStop
    ) {
        StopTime stopTime = new StopTime();
        if (isNormalStop) {
            stopTime.stop_id = id;
        } else {
            stopTime.location_id = id;
        }
        stopTime.departure_time = departureTime;
        stopTime.end_pickup_drop_off_window = endPickupDropOffWindow;
        return stopTime;
    }
}
