package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.TestUtils;
import com.conveyal.gtfs.dto.BookingRuleDTO;
import com.conveyal.gtfs.dto.CalendarDTO;
import com.conveyal.gtfs.dto.CalendarDateDTO;
import com.conveyal.gtfs.dto.FareDTO;
import com.conveyal.gtfs.dto.FeedInfoDTO;
import com.conveyal.gtfs.dto.LocationDTO;
import com.conveyal.gtfs.dto.LocationGroupDTO;
import com.conveyal.gtfs.dto.LocationGroupStopDTO;
import com.conveyal.gtfs.dto.LocationShapeDTO;
import com.conveyal.gtfs.dto.PatternDTO;
import com.conveyal.gtfs.dto.PatternStopDTO;
import com.conveyal.gtfs.dto.PatternStopWithFlexDTO;
import com.conveyal.gtfs.dto.RouteDTO;
import com.conveyal.gtfs.dto.ScheduleExceptionDTO;
import com.conveyal.gtfs.dto.ShapePointDTO;
import com.conveyal.gtfs.dto.StopDTO;
import com.conveyal.gtfs.dto.StopTimeDTO;
import com.conveyal.gtfs.dto.StopTimeWithFlexDTO;
import com.conveyal.gtfs.dto.TripDTO;
import com.conveyal.gtfs.model.ScheduleException;
import com.conveyal.gtfs.model.StopTime;
import com.conveyal.gtfs.util.InvalidNamespaceException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import static com.conveyal.gtfs.GTFS.load;
import static com.conveyal.gtfs.GTFS.makeSnapshot;
import static com.conveyal.gtfs.GTFS.validate;
import static com.conveyal.gtfs.TestUtils.getResourceFileName;
import static com.conveyal.gtfs.model.LocationShape.polygonCornerCountErrorMessage;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * This class contains CRUD tests for {@link JdbcTableWriter} (i.e., editing GTFS entities in the RDBMS). Set up
 * consists of creating a scratch database and an empty feed snapshot, which is the necessary starting condition
 * for building a GTFS feed from scratch. It then runs the various CRUD tests and finishes by dropping the database
 * (even if tests fail).
 */
public class JDBCTableWriterTest {

    private static final Logger LOG = LoggerFactory.getLogger(JDBCTableWriterTest.class);

    private static String testDBName;
    private static DataSource testDataSource;
    private static Connection connection;
    private static String testNamespace;
    private static String testGtfsGLSnapshotNamespace;

    private static final String SIMPLE_SERVICE_ID = "1";
    private static final String FIRST_STOP_ID = "1";
    private static final String SECOND_STOP_ID = "1.5";
    private static final String LAST_STOP_ID = "2";
    private static final double FIRST_STOP_LAT = 34.2222;
    private static final double FIRST_STOP_LON = -87.333;
    private static final double LAST_STOP_LAT = 34.2233;
    private static final double LAST_STOP_LON = -87.334;
    private static final ObjectMapper mapper = new ObjectMapper();

    private static PatternDTO pattern;
    private static TripDTO createdTrip;
    private static StopDTO stopOne;
    private static StopDTO stopTwo;
    private static StopDTO stopThree;
    private static LocationDTO locationOne;
    private static LocationDTO locationTwo;
    private static LocationDTO locationThree;
    private static LocationGroupDTO locationGroupOne;
    private static LocationGroupDTO locationGroupTwo;

    private static JdbcTableWriter createTestTableWriter(Table table) throws InvalidNamespaceException {
        return new JdbcTableWriter(table, testDataSource, testNamespace);
    }

    @BeforeAll
    public static void setUpClass() throws SQLException, IOException, InvalidNamespaceException {
        // Create a new database
        testDBName = TestUtils.generateNewDB();
        testDataSource = TestUtils.createTestDataSource(String.format("jdbc:postgresql://localhost/%s", testDBName));
        connection = testDataSource.getConnection();
        connection.createStatement().execute(JdbcGtfsLoader.getCreateFeedRegistrySQL());
        connection.commit();

        // Create an empty snapshot to create a new namespace and all the tables
        FeedLoadResult result = makeSnapshot(null, testDataSource, false);
        testNamespace = result.uniqueIdentifier;

        // Create a service calendar and two stops, both of which are necessary to perform pattern and trip tests.
        saveRecord(Table.CALENDAR, CalendarDTO.create(SIMPLE_SERVICE_ID, "20180103", "20180104"), CalendarDTO.class);
        saveRecord(Table.STOPS, StopDTO.create(FIRST_STOP_ID, "First Stop", FIRST_STOP_LAT, FIRST_STOP_LON), StopDTO.class);
        double secondStopLat = 34.2227;
        double secondStopLon = -87.3335;
        saveRecord(Table.STOPS, StopDTO.create(SECOND_STOP_ID, "Second Stop", secondStopLat, secondStopLon), StopDTO.class);
        saveRecord(Table.STOPS, StopDTO.create(LAST_STOP_ID, "Last Stop", LAST_STOP_LAT, LAST_STOP_LON), StopDTO.class);

        // Load the following real-life GTFS for use with {@link JDBCTableWriterTest#canUpdateServiceId()}
        FeedLoadResult feedLoadResult = load(getResourceFileName("real-world-gtfs-feeds/gtfs_GL.zip"), testDataSource);
        String testGtfsGLNamespace = feedLoadResult.uniqueIdentifier;
        // validate feed to create additional tables
        validate(testGtfsGLNamespace, testDataSource);
        // load into editor via snapshot
        JdbcGtfsSnapshotter snapshotter = new JdbcGtfsSnapshotter(testGtfsGLNamespace, testDataSource, false);
        SnapshotResult snapshotResult = snapshotter.copyTables();
        testGtfsGLSnapshotNamespace = snapshotResult.uniqueIdentifier;
        patternReconciliationSetUp();
    }

    /**
     * Create the required entities for pattern reconciliation tests.
     */
    private static void patternReconciliationSetUp() throws SQLException, IOException, InvalidNamespaceException {
        stopOne = saveRecord(Table.STOPS, StopDTO.create(newUUID(), "-stop-1", 0.0, 0.0), StopDTO.class);
        stopTwo = saveRecord(Table.STOPS, StopDTO.create(newUUID(), "-stop-2", 0.0, 0.0), StopDTO.class);
        stopThree = saveRecord(Table.STOPS, StopDTO.create(newUUID(), "-stop-3", 0.0, 0.0), StopDTO.class);
        locationOne = saveRecord(Table.LOCATIONS, LocationDTO.create(newUUID() + "-location-1"), LocationDTO.class);
        locationTwo = saveRecord(Table.LOCATIONS, LocationDTO.create(newUUID() + "-location-2"), LocationDTO.class);
        locationThree = saveRecord(Table.LOCATIONS, LocationDTO.create(newUUID() + "-location-3"), LocationDTO.class);
        locationGroupOne = saveRecord(Table.LOCATION_GROUP, LocationGroupDTO.create(newUUID() + "-location-group-1"), LocationGroupDTO.class);
        locationGroupTwo = saveRecord(Table.LOCATION_GROUP, LocationGroupDTO.create(newUUID() + "-location-group-2"), LocationGroupDTO.class);

        String patternId = newUUID();
        pattern = createRouteAndPattern(
            newUUID(),
            patternId,
            "pattern name",
            null,
            new ShapePointDTO[] {},
            new PatternStopWithFlexDTO[] {
                PatternStopWithFlexDTO.createFlexPatternStop(patternId, null, locationOne.location_id, 0, 0),
                new PatternStopWithFlexDTO(patternId, stopOne.stop_id, 1)
            },
            1
        );

        StopTimeWithFlexDTO[] stopTimes = new StopTimeWithFlexDTO[] {
            StopTimeWithFlexDTO.create(null, locationOne.location_id, 0, 0, 0),
            new StopTimeWithFlexDTO(stopOne.stop_id, 0, 0, 1)
        };
        createdTrip = saveRecord(Table.TRIPS, TripDTO.create(pattern.pattern_id, pattern.route_id, stopTimes), TripDTO.class);
    }

    @AfterAll
    public static void tearDownClass() throws SQLException {
        connection.close();
        TestUtils.dropDB(testDBName);
    }

    @Test
    void canCreateUpdateAndDeleteFeedInfoEntities() throws IOException, SQLException, InvalidNamespaceException {
        final Table table = Table.FEED_INFO;
        final Class<FeedInfoDTO> clazz = FeedInfoDTO.class;
        FeedInfoDTO feedInfoInput = FeedInfoDTO.create();
        FeedInfoDTO createdFeedInfo = saveRecord(table, feedInfoInput, clazz);

        // make sure saved data matches expected data
        assertEquals(createdFeedInfo.feed_publisher_name, feedInfoInput.feed_publisher_name);

        createdFeedInfo.feed_publisher_name = "test-publisher-updated";
        FeedInfoDTO updatedFeedInfoDTO = updateRecord(createdFeedInfo.id, table, createdFeedInfo, clazz);

        // make sure saved data matches expected data
        assertEquals(updatedFeedInfoDTO.feed_publisher_name, createdFeedInfo.feed_publisher_name);

        deleteRecord(Table.FEED_INFO, createdFeedInfo.id);
    }

    /**
     * Ensure that potentially malicious SQL injection is sanitized properly during create operations.
     */
    @Test
    void canPreventSQLInjection() throws IOException, SQLException, InvalidNamespaceException {
        FeedInfoDTO table = FeedInfoDTO.create();
        FeedInfoDTO createdFeedInfo = saveRecord(Table.FEED_INFO, table, FeedInfoDTO.class);
        assertEquals(createdFeedInfo.feed_publisher_name, table.feed_publisher_name);
    }

    @Test
    void canCreateUpdateAndDeleteFares() throws IOException, SQLException, InvalidNamespaceException {
        final Table table = Table.FARE_ATTRIBUTES;
        final Class<FareDTO> clazz = FareDTO.class;
        FareDTO fare = FareDTO.create();
        FareDTO createdFare = saveRecord(table, fare, clazz);
        assertEquals(createdFare.fare_id, fare.fare_id);
        assertEquals(createdFare.fare_rules[0].fare_id, fare.fare_id);

        // Ensure transfers value is null to check database integrity.
        ResultSet resultSet = getResultSetForId(createdFare.id, Table.FARE_ATTRIBUTES);
        while (resultSet.next()) {
            // We must match against null value for transfers because the database stored value will
            // not be an empty string, but null.
            assertResultValue(resultSet, "transfers", Matchers.nullValue());
            assertResultValue(resultSet, "fare_id", equalTo(fare.fare_id));
            assertResultValue(resultSet, "currency_type", equalTo(fare.currency_type));
            assertResultValue(resultSet, "price", equalTo(fare.price));
            assertResultValue(resultSet, "agency_id", equalTo(fare.agency_id));
            assertResultValue(resultSet, "payment_method", equalTo(fare.payment_method));
            assertResultValue(resultSet, "transfer_duration", equalTo(fare.transfer_duration));
        }

        // try to update record
        createdFare.fare_id = "3B";
        createdFare.transfers = "0";

        FareDTO updatedFareDTO = updateRecord(createdFare.id, table, createdFare, clazz);
        assertEquals(updatedFareDTO.fare_id, createdFare.fare_id);
        assertEquals(updatedFareDTO.fare_rules[0].fare_id, createdFare.fare_id);

        // Ensure transfers value is updated correctly to check database integrity.
        ResultSet updatedResult = getResultSetForId(createdFare.id, Table.FARE_ATTRIBUTES);
        while (updatedResult.next()) {
            assertResultValue(updatedResult, "transfers", equalTo(0));
            assertResultValue(updatedResult, "fare_id", equalTo(createdFare.fare_id));
        }
        deleteRecord(table, createdFare.id);
    }

    @Test
    void canCreateUpdateAndDeleteRoutes() throws IOException, SQLException, InvalidNamespaceException {
        final Table routeTable = Table.ROUTES;
        final Class<RouteDTO> routeDTOClass = RouteDTO.class;
        RouteDTO route = RouteDTO.create();
        RouteDTO createdRoute = saveRecord(routeTable, route, routeDTOClass);

        assertEquals(createdRoute.route_id, route.route_id);
        assertThatSqlQueryYieldsRowCount(getColumnsForId(createdRoute.id, routeTable), 1);

        createdRoute.route_id = "600";

        RouteDTO updatedRouteDTO = updateRecord(createdRoute.id, routeTable, createdRoute, routeDTOClass);
        assertEquals(updatedRouteDTO.route_id, createdRoute.route_id);
        assertNull(updatedRouteDTO.route_color);

        ResultSet resultSet = getResultSetForId(updatedRouteDTO.id, routeTable);
        while (resultSet.next()) {
            assertResultValue(resultSet, "route_color", Matchers.nullValue());
            assertResultValue(resultSet, "route_id", equalTo(createdRoute.route_id));
            assertResultValue(resultSet, "route_sort_order", Matchers.nullValue());
            assertResultValue(resultSet, "route_type", equalTo(createdRoute.route_type));
        }
        deleteRecord(routeTable, createdRoute.id);
    }

    @Test
    void canCreateUpdateAndDeleteBookingRules() throws IOException, SQLException, InvalidNamespaceException {
        final Table bookingRuleTable = Table.BOOKING_RULES;
        final Class<BookingRuleDTO> bookingRuleDTOClass = BookingRuleDTO.class;
        BookingRuleDTO bookingRule = BookingRuleDTO.create();
        BookingRuleDTO createdBookingRule = saveRecord(bookingRuleTable, bookingRule, bookingRuleDTOClass);

        assertEquals(createdBookingRule.booking_rule_id, bookingRule.booking_rule_id);
        assertThatSqlQueryYieldsRowCount(getColumnsForId(createdBookingRule.id, Table.BOOKING_RULES), 1);

        createdBookingRule.booking_rule_id = "1749";

        BookingRuleDTO updatedBookingRuleDTO = updateRecord(createdBookingRule.id, bookingRuleTable, createdBookingRule, bookingRuleDTOClass);
        assertEquals(updatedBookingRuleDTO.booking_rule_id, createdBookingRule.booking_rule_id);
        assertNull(updatedBookingRuleDTO.message);

        ResultSet resultSet = getResultSetForId(updatedBookingRuleDTO.id, bookingRuleTable);
        while (resultSet.next()) {
            assertResultValue(resultSet, "booking_rule_id", equalTo(createdBookingRule.booking_rule_id));
            assertResultValue(resultSet, "message", Matchers.nullValue());
            assertResultValue(resultSet, "prior_notice_duration_min", equalTo(createdBookingRule.prior_notice_duration_min));
            assertResultValue(resultSet, "prior_notice_duration_max", equalTo(createdBookingRule.prior_notice_duration_max));
            assertResultValue(resultSet, "prior_notice_start_time", equalTo(createdBookingRule.prior_notice_start_time));
            assertResultValue(resultSet, "prior_notice_last_time", equalTo(createdBookingRule.prior_notice_last_time));
        }
        deleteRecord(bookingRuleTable, createdBookingRule.id);
    }

    @Test
    void canCreateUpdateAndDeletePattern() throws IOException, SQLException, InvalidNamespaceException {
        final Table patternStopTable = Table.PATTERNS;
        final Class<PatternDTO> patternDTOClass = PatternDTO.class;

        String[] stopIds = { "2320969", "2320967", "759190", "759180", "759181", "29673", "759182", "759193", "759183", "759195", "13528", "2326079", "759188", "759189"};
        for (String stopId : stopIds) {
            saveRecord(Table.STOPS, StopDTO.create(stopId, stopId, FIRST_STOP_LAT, FIRST_STOP_LON), StopDTO.class);
        }
        saveRecord(Table.LOCATIONS, LocationDTO.create("radius_1207_s_2322275_s_759180"), LocationDTO.class);
        saveRecord(Table.TRIPS, TripDTO.create("pattern-id-123", "1116"), TripDTO.class);

        String patternOne = "{\n" +
            "   \"id\":1,\n" +
            "   \"shape_id\":\"raxl\",\n" +
            "   \"pattern_id\":\"pattern-id-123\",\n" +
            "   \"route_id\":\"1116\",\n" +
            "   \"direction_id\":0,\n" +
            "   \"use_frequency\":0,\n" +
            "   \"name\":\"14 stops from Sandy Transit Operations Center to Timberline (7 trips)\",\n" +
            "   \"pattern_stops\":[\n" +
            "      {\n" +
            "         \"id\":\"23209670\",\n" +
            "         \"pattern_id\":\"pattern-id-123\",\n" +
            "         \"stop_id\":\"2320967\",\n" +
            "         \"location_id\":null,\n" +
            "         \"location_group_id\":null,\n" +
            "         \"default_travel_time\":0,\n" +
            "         \"default_dwell_time\":0,\n" +
            "         \"stop_sequence\":0,\n" +
            "         \"shape_dist_traveled\":0,\n" +
            "         \"pickup_type\":0,\n" +
            "         \"drop_off_type\":0,\n" +
            "         \"timepoint\":0,\n" +
            "         \"continuous_pickup\":1,\n" +
            "         \"continuous_drop_off\":1,\n" +
            "         \"pickup_booking_rule_id\":null,\n" +
            "         \"drop_off_booking_rule_id\":null,\n" +
            "         \"stop_headsign\":null\n" +
            "      },\n" +
            "      {\n" +
            "         \"id\":\"7591951\",\n" +
            "         \"pattern_id\":\"pattern-id-123\",\n" +
            "         \"stop_id\":\"759195\",\n" +
            "         \"location_id\":null,\n" +
            "         \"location_group_id\":null,\n" +
            "         \"default_travel_time\":35,\n" +
            "         \"default_dwell_time\":0,\n" +
            "         \"stop_sequence\":1,\n" +
            "         \"shape_dist_traveled\":505.52209687932935,\n" +
            "         \"pickup_type\":0,\n" +
            "         \"drop_off_type\":0,\n" +
            "         \"timepoint\":0,\n" +
            "         \"continuous_pickup\":1,\n" +
            "         \"continuous_drop_off\":1,\n" +
            "         \"pickup_booking_rule_id\":null,\n" +
            "         \"drop_off_booking_rule_id\":null,\n" +
            "         \"stop_headsign\":\"Timberline\"\n" +
            "      },\n" +
            "      {\n" +
            "         \"id\":\"135282\",\n" +
            "         \"pattern_id\":\"pattern-id-123\",\n" +
            "         \"stop_id\":\"13528\",\n" +
            "         \"location_id\":null,\n" +
            "         \"location_group_id\":null,\n" +
            "         \"default_travel_time\":85,\n" +
            "         \"default_dwell_time\":0,\n" +
            "         \"stop_sequence\":2,\n" +
            "         \"shape_dist_traveled\":1718.975446911759,\n" +
            "         \"pickup_type\":0,\n" +
            "         \"drop_off_type\":0,\n" +
            "         \"timepoint\":0,\n" +
            "         \"continuous_pickup\":1,\n" +
            "         \"continuous_drop_off\":1,\n" +
            "         \"pickup_booking_rule_id\":null,\n" +
            "         \"drop_off_booking_rule_id\":null,\n" +
            "         \"stop_headsign\":null\n" +
            "      },\n" +
            "      {\n" +
            "         \"id\":\"296733\",\n" +
            "         \"pattern_id\":\"pattern-id-123\",\n" +
            "         \"stop_id\":\"29673\",\n" +
            "         \"location_id\":null,\n" +
            "         \"location_group_id\":null,\n" +
            "         \"default_travel_time\":480,\n" +
            "         \"default_dwell_time\":0,\n" +
            "         \"stop_sequence\":3,\n" +
            "         \"shape_dist_traveled\":3454.848016627383,\n" +
            "         \"pickup_type\":0,\n" +
            "         \"drop_off_type\":0,\n" +
            "         \"timepoint\":0,\n" +
            "         \"continuous_pickup\":1,\n" +
            "         \"continuous_drop_off\":1,\n" +
            "         \"pickup_booking_rule_id\":null,\n" +
            "         \"drop_off_booking_rule_id\":null,\n" +
            "         \"stop_headsign\":null\n" +
            "      },\n" +
            "      {\n" +
            "         \"id\":\"7591804\",\n" +
            "         \"pattern_id\":\"pattern-id-123\",\n" +
            "         \"stop_id\":\"759180\",\n" +
            "         \"location_id\":null,\n" +
            "         \"location_group_id\":null,\n" +
            "         \"default_travel_time\":600,\n" +
            "         \"default_dwell_time\":0,\n" +
            "         \"stop_sequence\":4,\n" +
            "         \"shape_dist_traveled\":17587.83777129988,\n" +
            "         \"pickup_type\":0,\n" +
            "         \"drop_off_type\":0,\n" +
            "         \"timepoint\":0,\n" +
            "         \"continuous_pickup\":1,\n" +
            "         \"continuous_drop_off\":1,\n" +
            "         \"pickup_booking_rule_id\":null,\n" +
            "         \"drop_off_booking_rule_id\":null,\n" +
            "         \"stop_headsign\":null\n" +
            "      },\n" +
            "      {\n" +
            "         \"id\":\"7591815\",\n" +
            "         \"pattern_id\":\"pattern-id-123\",\n" +
            "         \"stop_id\":\"759181\",\n" +
            "         \"location_id\":null,\n" +
            "         \"location_group_id\":null,\n" +
            "         \"default_travel_time\":540,\n" +
            "         \"default_dwell_time\":0,\n" +
            "         \"stop_sequence\":5,\n" +
            "         \"shape_dist_traveled\":26473.98462457874,\n" +
            "         \"pickup_type\":0,\n" +
            "         \"drop_off_type\":0,\n" +
            "         \"timepoint\":0,\n" +
            "         \"continuous_pickup\":1,\n" +
            "         \"continuous_drop_off\":1,\n" +
            "         \"pickup_booking_rule_id\":null,\n" +
            "         \"drop_off_booking_rule_id\":null,\n" +
            "         \"stop_headsign\":null\n" +
            "      },\n" +
            "      {\n" +
            "         \"id\":\"7591826\",\n" +
            "         \"pattern_id\":\"pattern-id-123\",\n" +
            "         \"stop_id\":\"759182\",\n" +
            "         \"location_id\":null,\n" +
            "         \"location_group_id\":null,\n" +
            "         \"default_travel_time\":180,\n" +
            "         \"default_dwell_time\":0,\n" +
            "         \"stop_sequence\":6,\n" +
            "         \"shape_dist_traveled\":29552.841773878637,\n" +
            "         \"pickup_type\":0,\n" +
            "         \"drop_off_type\":0,\n" +
            "         \"timepoint\":0,\n" +
            "         \"continuous_pickup\":1,\n" +
            "         \"continuous_drop_off\":1,\n" +
            "         \"pickup_booking_rule_id\":null,\n" +
            "         \"drop_off_booking_rule_id\":null,\n" +
            "         \"stop_headsign\":null\n" +
            "      },\n" +
            "      {\n" +
            "         \"id\":\"7591887\",\n" +
            "         \"pattern_id\":\"pattern-id-123\",\n" +
            "         \"stop_id\":\"759188\",\n" +
            "         \"location_id\":null,\n" +
            "         \"location_group_id\":null,\n" +
            "         \"default_travel_time\":240,\n" +
            "         \"default_dwell_time\":0,\n" +
            "         \"stop_sequence\":7,\n" +
            "         \"shape_dist_traveled\":31330.49610122987,\n" +
            "         \"pickup_type\":0,\n" +
            "         \"drop_off_type\":0,\n" +
            "         \"timepoint\":0,\n" +
            "         \"continuous_pickup\":1,\n" +
            "         \"continuous_drop_off\":1,\n" +
            "         \"pickup_booking_rule_id\":null,\n" +
            "         \"drop_off_booking_rule_id\":null,\n" +
            "         \"stop_headsign\":null\n" +
            "      },\n" +
            "      {\n" +
            "         \"id\":\"7591838\",\n" +
            "         \"pattern_id\":\"pattern-id-123\",\n" +
            "         \"stop_id\":\"759183\",\n" +
            "         \"location_id\":null,\n" +
            "         \"location_group_id\":null,\n" +
            "         \"default_travel_time\":180,\n" +
            "         \"default_dwell_time\":0,\n" +
            "         \"stop_sequence\":8,\n" +
            "         \"shape_dist_traveled\":34805.07506567049,\n" +
            "         \"pickup_type\":0,\n" +
            "         \"drop_off_type\":0,\n" +
            "         \"timepoint\":0,\n" +
            "         \"continuous_pickup\":1,\n" +
            "         \"continuous_drop_off\":1,\n" +
            "         \"pickup_booking_rule_id\":null,\n" +
            "         \"drop_off_booking_rule_id\":null,\n" +
            "         \"stop_headsign\":null\n" +
            "      },\n" +
            "      {\n" +
            "         \"id\":\"7591899\",\n" +
            "         \"pattern_id\":\"pattern-id-123\",\n" +
            "         \"stop_id\":\"759189\",\n" +
            "         \"location_id\":null,\n" +
            "         \"location_group_id\":null,\n" +
            "         \"default_travel_time\":480,\n" +
            "         \"default_dwell_time\":0,\n" +
            "         \"stop_sequence\":9,\n" +
            "         \"shape_dist_traveled\":48454.40024406261,\n" +
            "         \"pickup_type\":0,\n" +
            "         \"drop_off_type\":0,\n" +
            "         \"timepoint\":0,\n" +
            "         \"continuous_pickup\":1,\n" +
            "         \"continuous_drop_off\":1,\n" +
            "         \"pickup_booking_rule_id\":null,\n" +
            "         \"drop_off_booking_rule_id\":null,\n" +
            "         \"stop_headsign\":null\n" +
            "      },\n" +
            "      {\n" +
            "         \"id\":\"232096911\",\n" +
            "         \"pattern_id\":\"pattern-id-123\",\n" +
            "         \"stop_id\":\"2320969\",\n" +
            "         \"location_id\":null,\n" +
            "         \"location_group_id\":null,\n" +
            "         \"default_travel_time\":120,\n" +
            "         \"default_dwell_time\":0,\n" +
            "         \"stop_sequence\":10,\n" +
            "         \"shape_dist_traveled\":115462.71972918736,\n" +
            "         \"pickup_type\":0,\n" +
            "         \"drop_off_type\":0,\n" +
            "         \"timepoint\":0,\n" +
            "         \"continuous_pickup\":1,\n" +
            "         \"continuous_drop_off\":1,\n" +
            "         \"pickup_booking_rule_id\":null,\n" +
            "         \"drop_off_booking_rule_id\":null,\n" +
            "         \"stop_headsign\":null\n" +
            "      },\n" +
            "      {\n" +
            "         \"id\":\"75919012\",\n" +
            "         \"pattern_id\":\"pattern-id-123\",\n" +
            "         \"stop_id\":\"759190\",\n" +
            "         \"location_id\":null,\n" +
            "         \"location_group_id\":null,\n" +
            "         \"default_travel_time\":360,\n" +
            "         \"default_dwell_time\":0,\n" +
            "         \"stop_sequence\":11,\n" +
            "         \"shape_dist_traveled\":116183.43127396851,\n" +
            "         \"pickup_type\":0,\n" +
            "         \"drop_off_type\":0,\n" +
            "         \"timepoint\":0,\n" +
            "         \"continuous_pickup\":1,\n" +
            "         \"continuous_drop_off\":1,\n" +
            "         \"pickup_booking_rule_id\":null,\n" +
            "         \"drop_off_booking_rule_id\":null,\n" +
            "         \"stop_headsign\":null\n" +
            "      },\n" +
            "      {\n" +
            "         \"id\":\"232607913\",\n" +
            "         \"pattern_id\":\"pattern-id-123\",\n" +
            "         \"stop_id\":\"2326079\",\n" +
            "         \"location_id\":null,\n" +
            "         \"location_group_id\":null,\n" +
            "         \"default_travel_time\":180,\n" +
            "         \"default_dwell_time\":0,\n" +
            "         \"stop_sequence\":12,\n" +
            "         \"shape_dist_traveled\":117032.2738073744,\n" +
            "         \"pickup_type\":0,\n" +
            "         \"drop_off_type\":0,\n" +
            "         \"timepoint\":0,\n" +
            "         \"continuous_pickup\":1,\n" +
            "         \"continuous_drop_off\":1,\n" +
            "         \"pickup_booking_rule_id\":null,\n" +
            "         \"drop_off_booking_rule_id\":null,\n" +
            "         \"stop_headsign\":null\n" +
            "      },\n" +
            "      {\n" +
            "         \"id\":\"75919314\",\n" +
            "         \"pattern_id\":\"pattern-id-123\",\n" +
            "         \"stop_id\":\"759193\",\n" +
            "         \"location_id\":null,\n" +
            "         \"location_group_id\":null,\n" +
            "         \"default_travel_time\":1020,\n" +
            "         \"default_dwell_time\":0,\n" +
            "         \"stop_sequence\":13,\n" +
            "         \"shape_dist_traveled\":126223.95428254058,\n" +
            "         \"pickup_type\":0,\n" +
            "         \"drop_off_type\":0,\n" +
            "         \"timepoint\":0,\n" +
            "         \"continuous_pickup\":1,\n" +
            "         \"continuous_drop_off\":1,\n" +
            "         \"pickup_booking_rule_id\":null,\n" +
            "         \"drop_off_booking_rule_id\":null,\n" +
            "         \"stop_headsign\":\"Sandy\"\n" +
            "      }\n" +
            "   ],\n" +
            "   \"shapes\":[]\n" +
            "}";

        String patternTwo = "{\n" +
            "   \"id\":1,\n" +
            "   \"shape_id\":\"raxl\",\n" +
            "   \"pattern_id\":\"pattern-id-123\",\n" +
            "   \"route_id\":\"1116\",\n" +
            "   \"direction_id\":0,\n" +
            "   \"use_frequency\":0,\n" +
            "   \"name\":\"14 stops from Sandy Transit Operations Center to Timberline (7 trips)\",\n" +
            "   \"pattern_stops\":[\n" +
            "      {\n" +
            "         \"id\":\"23209670\",\n" +
            "         \"pattern_id\":\"pattern-id-123\",\n" +
            "         \"stop_id\":\"2320967\",\n" +
            "         \"location_id\":null,\n" +
            "         \"location_group_id\":null,\n" +
            "         \"default_travel_time\":0,\n" +
            "         \"default_dwell_time\":0,\n" +
            "         \"stop_sequence\":0,\n" +
            "         \"shape_dist_traveled\":0,\n" +
            "         \"pickup_type\":0,\n" +
            "         \"drop_off_type\":0,\n" +
            "         \"timepoint\":0,\n" +
            "         \"continuous_pickup\":1,\n" +
            "         \"continuous_drop_off\":1,\n" +
            "         \"pickup_booking_rule_id\":null,\n" +
            "         \"drop_off_booking_rule_id\":null,\n" +
            "         \"stop_headsign\":null\n" +
            "      },\n" +
            "      {\n" +
            "         \"id\":\"7591951\",\n" +
            "         \"pattern_id\":\"pattern-id-123\",\n" +
            "         \"stop_id\":\"759195\",\n" +
            "         \"location_id\":null,\n" +
            "         \"location_group_id\":null,\n" +
            "         \"default_travel_time\":35,\n" +
            "         \"default_dwell_time\":0,\n" +
            "         \"stop_sequence\":1,\n" +
            "         \"shape_dist_traveled\":505.52209687932935,\n" +
            "         \"pickup_type\":0,\n" +
            "         \"drop_off_type\":0,\n" +
            "         \"timepoint\":0,\n" +
            "         \"continuous_pickup\":1,\n" +
            "         \"continuous_drop_off\":1,\n" +
            "         \"pickup_booking_rule_id\":null,\n" +
            "         \"drop_off_booking_rule_id\":null,\n" +
            "         \"stop_headsign\":\"Timberline\"\n" +
            "      },\n" +
            "      {\n" +
            "         \"id\":\"135282\",\n" +
            "         \"pattern_id\":\"pattern-id-123\",\n" +
            "         \"stop_id\":\"13528\",\n" +
            "         \"location_id\":null,\n" +
            "         \"location_group_id\":null,\n" +
            "         \"default_travel_time\":85,\n" +
            "         \"default_dwell_time\":0,\n" +
            "         \"stop_sequence\":2,\n" +
            "         \"shape_dist_traveled\":1718.975446911759,\n" +
            "         \"pickup_type\":0,\n" +
            "         \"drop_off_type\":0,\n" +
            "         \"timepoint\":0,\n" +
            "         \"continuous_pickup\":1,\n" +
            "         \"continuous_drop_off\":1,\n" +
            "         \"pickup_booking_rule_id\":null,\n" +
            "         \"drop_off_booking_rule_id\":null,\n" +
            "         \"stop_headsign\":null\n" +
            "      },\n" +
            "      {\n" +
            "         \"id\":\"296733\",\n" +
            "         \"pattern_id\":\"pattern-id-123\",\n" +
            "         \"stop_id\":\"29673\",\n" +
            "         \"location_id\":null,\n" +
            "         \"location_group_id\":null,\n" +
            "         \"default_travel_time\":480,\n" +
            "         \"default_dwell_time\":0,\n" +
            "         \"stop_sequence\":3,\n" +
            "         \"shape_dist_traveled\":3454.848016627383,\n" +
            "         \"pickup_type\":0,\n" +
            "         \"drop_off_type\":0,\n" +
            "         \"timepoint\":0,\n" +
            "         \"continuous_pickup\":1,\n" +
            "         \"continuous_drop_off\":1,\n" +
            "         \"pickup_booking_rule_id\":null,\n" +
            "         \"drop_off_booking_rule_id\":null,\n" +
            "         \"stop_headsign\":null\n" +
            "      },\n" +
            "      {\n" +
            "         \"id\":\"7591804\",\n" +
            "         \"pattern_id\":\"pattern-id-123\",\n" +
            "         \"stop_id\":\"759180\",\n" +
            "         \"location_id\":null,\n" +
            "         \"location_group_id\":null,\n" +
            "         \"default_travel_time\":600,\n" +
            "         \"default_dwell_time\":0,\n" +
            "         \"stop_sequence\":4,\n" +
            "         \"shape_dist_traveled\":17587.83777129988,\n" +
            "         \"pickup_type\":0,\n" +
            "         \"drop_off_type\":0,\n" +
            "         \"timepoint\":0,\n" +
            "         \"continuous_pickup\":1,\n" +
            "         \"continuous_drop_off\":1,\n" +
            "         \"pickup_booking_rule_id\":null,\n" +
            "         \"drop_off_booking_rule_id\":null,\n" +
            "         \"stop_headsign\":null\n" +
            "      },\n" +
            "      {\n" +
            "         \"id\":\"7591815\",\n" +
            "         \"pattern_id\":\"pattern-id-123\",\n" +
            "         \"stop_id\":\"759181\",\n" +
            "         \"location_id\":null,\n" +
            "         \"location_group_id\":null,\n" +
            "         \"default_travel_time\":540,\n" +
            "         \"default_dwell_time\":0,\n" +
            "         \"stop_sequence\":5,\n" +
            "         \"shape_dist_traveled\":26473.98462457874,\n" +
            "         \"pickup_type\":0,\n" +
            "         \"drop_off_type\":0,\n" +
            "         \"timepoint\":0,\n" +
            "         \"continuous_pickup\":1,\n" +
            "         \"continuous_drop_off\":1,\n" +
            "         \"pickup_booking_rule_id\":null,\n" +
            "         \"drop_off_booking_rule_id\":null,\n" +
            "         \"stop_headsign\":null\n" +
            "      },\n" +
            "      {\n" +
            "         \"id\":\"7591826\",\n" +
            "         \"pattern_id\":\"pattern-id-123\",\n" +
            "         \"stop_id\":\"759182\",\n" +
            "         \"location_id\":null,\n" +
            "         \"location_group_id\":null,\n" +
            "         \"default_travel_time\":180,\n" +
            "         \"default_dwell_time\":0,\n" +
            "         \"stop_sequence\":6,\n" +
            "         \"shape_dist_traveled\":29552.841773878637,\n" +
            "         \"pickup_type\":0,\n" +
            "         \"drop_off_type\":0,\n" +
            "         \"timepoint\":0,\n" +
            "         \"continuous_pickup\":1,\n" +
            "         \"continuous_drop_off\":1,\n" +
            "         \"pickup_booking_rule_id\":null,\n" +
            "         \"drop_off_booking_rule_id\":null,\n" +
            "         \"stop_headsign\":null\n" +
            "      },\n" +
            "      {\n" +
            "         \"id\":\"7591887\",\n" +
            "         \"pattern_id\":\"pattern-id-123\",\n" +
            "         \"stop_id\":\"759188\",\n" +
            "         \"location_id\":null,\n" +
            "         \"location_group_id\":null,\n" +
            "         \"default_travel_time\":240,\n" +
            "         \"default_dwell_time\":0,\n" +
            "         \"stop_sequence\":7,\n" +
            "         \"shape_dist_traveled\":31330.49610122987,\n" +
            "         \"pickup_type\":0,\n" +
            "         \"drop_off_type\":0,\n" +
            "         \"timepoint\":0,\n" +
            "         \"continuous_pickup\":1,\n" +
            "         \"continuous_drop_off\":1,\n" +
            "         \"pickup_booking_rule_id\":null,\n" +
            "         \"drop_off_booking_rule_id\":null,\n" +
            "         \"stop_headsign\":null\n" +
            "      },\n" +
            "      {\n" +
            "         \"id\":\"7591838\",\n" +
            "         \"pattern_id\":\"pattern-id-123\",\n" +
            "         \"stop_id\":\"759183\",\n" +
            "         \"location_id\":null,\n" +
            "         \"location_group_id\":null,\n" +
            "         \"default_travel_time\":180,\n" +
            "         \"default_dwell_time\":0,\n" +
            "         \"stop_sequence\":8,\n" +
            "         \"shape_dist_traveled\":34805.07506567049,\n" +
            "         \"pickup_type\":0,\n" +
            "         \"drop_off_type\":0,\n" +
            "         \"timepoint\":0,\n" +
            "         \"continuous_pickup\":1,\n" +
            "         \"continuous_drop_off\":1,\n" +
            "         \"pickup_booking_rule_id\":null,\n" +
            "         \"drop_off_booking_rule_id\":null,\n" +
            "         \"stop_headsign\":null\n" +
            "      },\n" +
            "      {\n" +
            "         \"id\":\"7591899\",\n" +
            "         \"pattern_id\":\"pattern-id-123\",\n" +
            "         \"stop_id\":\"759189\",\n" +
            "         \"location_id\":null,\n" +
            "         \"location_group_id\":null,\n" +
            "         \"default_travel_time\":480,\n" +
            "         \"default_dwell_time\":0,\n" +
            "         \"stop_sequence\":9,\n" +
            "         \"shape_dist_traveled\":48454.40024406261,\n" +
            "         \"pickup_type\":0,\n" +
            "         \"drop_off_type\":0,\n" +
            "         \"timepoint\":0,\n" +
            "         \"continuous_pickup\":1,\n" +
            "         \"continuous_drop_off\":1,\n" +
            "         \"pickup_booking_rule_id\":null,\n" +
            "         \"drop_off_booking_rule_id\":null,\n" +
            "         \"stop_headsign\":null\n" +
            "      },\n" +
            "      {\n" +
            "         \"pattern_id\":\"pattern-id-123\",\n" +
            "         \"continuous_drop_off\":1,\n" +
            "         \"continuous_pickup\":1,\n" +
            "         \"default_dwell_time\":0,\n" +
            "         \"default_travel_time\":0,\n" +
            "         \"drop_off_booking_rule_id\":null,\n" +
            "         \"drop_off_type\":2,\n" +
            "         \"id\":\"radius_1207_s_2322275_s_75918010\",\n" +
            "         \"location_group_id\":null,\n" +
            "         \"location_id\":\"radius_1207_s_2322275_s_759180\",\n" +
            "         \"pickup_booking_rule_id\":null,\n" +
            "         \"pickup_type\":2,\n" +
            "         \"shape_dist_traveled\":81830.44703927514,\n" +
            "         \"stop_headsign\":\"\",\n" +
            "         \"stop_id\":null,\n" +
            "         \"stop_sequence\":10,\n" +
            "         \"timepoint\":null\n" +
            "      },\n" +
            "      {\n" +
            "         \"id\":\"232096911\",\n" +
            "         \"pattern_id\":\"pattern-id-123\",\n" +
            "         \"stop_id\":\"2320969\",\n" +
            "         \"location_id\":null,\n" +
            "         \"location_group_id\":null,\n" +
            "         \"default_travel_time\":120,\n" +
            "         \"default_dwell_time\":0,\n" +
            "         \"stop_sequence\":11,\n" +
            "         \"shape_dist_traveled\":115462.71972918736,\n" +
            "         \"pickup_type\":0,\n" +
            "         \"drop_off_type\":0,\n" +
            "         \"timepoint\":0,\n" +
            "         \"continuous_pickup\":1,\n" +
            "         \"continuous_drop_off\":1,\n" +
            "         \"pickup_booking_rule_id\":null,\n" +
            "         \"drop_off_booking_rule_id\":null,\n" +
            "         \"stop_headsign\":null\n" +
            "      },\n" +
            "      {\n" +
            "         \"id\":\"75919012\",\n" +
            "         \"pattern_id\":\"pattern-id-123\",\n" +
            "         \"stop_id\":\"759190\",\n" +
            "         \"location_id\":null,\n" +
            "         \"location_group_id\":null,\n" +
            "         \"default_travel_time\":360,\n" +
            "         \"default_dwell_time\":0,\n" +
            "         \"stop_sequence\":12,\n" +
            "         \"shape_dist_traveled\":116183.43127396851,\n" +
            "         \"pickup_type\":0,\n" +
            "         \"drop_off_type\":0,\n" +
            "         \"timepoint\":0,\n" +
            "         \"continuous_pickup\":1,\n" +
            "         \"continuous_drop_off\":1,\n" +
            "         \"pickup_booking_rule_id\":null,\n" +
            "         \"drop_off_booking_rule_id\":null,\n" +
            "         \"stop_headsign\":null\n" +
            "      },\n" +
            "      {\n" +
            "         \"id\":\"232607913\",\n" +
            "         \"pattern_id\":\"pattern-id-123\",\n" +
            "         \"stop_id\":\"2326079\",\n" +
            "         \"location_id\":null,\n" +
            "         \"location_group_id\":null,\n" +
            "         \"default_travel_time\":180,\n" +
            "         \"default_dwell_time\":0,\n" +
            "         \"stop_sequence\":13,\n" +
            "         \"shape_dist_traveled\":117032.2738073744,\n" +
            "         \"pickup_type\":0,\n" +
            "         \"drop_off_type\":0,\n" +
            "         \"timepoint\":0,\n" +
            "         \"continuous_pickup\":1,\n" +
            "         \"continuous_drop_off\":1,\n" +
            "         \"pickup_booking_rule_id\":null,\n" +
            "         \"drop_off_booking_rule_id\":null,\n" +
            "         \"stop_headsign\":null\n" +
            "      },\n" +
            "      {\n" +
            "         \"id\":\"75919314\",\n" +
            "         \"pattern_id\":\"pattern-id-123\",\n" +
            "         \"stop_id\":\"759193\",\n" +
            "         \"location_id\":null,\n" +
            "         \"location_group_id\":null,\n" +
            "         \"default_travel_time\":1020,\n" +
            "         \"default_dwell_time\":0,\n" +
            "         \"stop_sequence\":14,\n" +
            "         \"shape_dist_traveled\":126223.95428254058,\n" +
            "         \"pickup_type\":0,\n" +
            "         \"drop_off_type\":0,\n" +
            "         \"timepoint\":0,\n" +
            "         \"continuous_pickup\":1,\n" +
            "         \"continuous_drop_off\":1,\n" +
            "         \"pickup_booking_rule_id\":null,\n" +
            "         \"drop_off_booking_rule_id\":null,\n" +
            "         \"stop_headsign\":\"Sandy\"\n" +
            "      }\n" +
            "   ],\n" +
            "   \"shapes\":[]\n" +
            "}";

        PatternDTO createdPatternStop = saveRecord(patternStopTable, patternOne, patternDTOClass);
        // 14 blank stop times inserted here.
        JdbcTableWriter jdbcTableWriter = createTestTableWriter(Table.PATTERNS);
        jdbcTableWriter.normalizeStopTimesForPattern(createdPatternStop.id, 0);

        createdPatternStop = updateRecord(createdPatternStop.id, Table.PATTERNS, patternTwo, patternDTOClass);
        // 1 blank stop time inserted here.
        jdbcTableWriter = createTestTableWriter(Table.PATTERNS);
        jdbcTableWriter.normalizeStopTimesForPattern(createdPatternStop.id, 0);

        deleteRecord(patternStopTable, createdPatternStop.id);
    }

    @Test
    void canCreateUpdateAndDeleteNonFlexPatternStop() throws IOException, SQLException, InvalidNamespaceException {
        final Table patternStopTable = Table.PATTERN_STOP;
        final Class<PatternStopDTO> patternStopDTOClass = PatternStopDTO.class;
        PatternStopDTO patternStop = PatternStopDTO.create();
        PatternStopDTO createdPatternStop = saveRecord(patternStopTable, patternStop, patternStopDTOClass);

        assertThat(createdPatternStop.pattern_id, equalTo(patternStop.pattern_id));
        assertThatSqlQueryYieldsRowCount(getColumnsForId(createdPatternStop.id, patternStopTable), 1);

        createdPatternStop.pattern_id = "updated-pattern-id-2";

        PatternStopDTO updatedPatternStopDTO = updateRecord(createdPatternStop.id, patternStopTable, createdPatternStop, patternStopDTOClass);
        assertEquals(updatedPatternStopDTO.pattern_id, createdPatternStop.pattern_id);

        ResultSet resultSet = getResultSetForId(updatedPatternStopDTO.id, patternStopTable);
        while (resultSet.next()) {
            assertResultValue(resultSet, "pattern_id", equalTo(createdPatternStop.pattern_id));
        }
        deleteRecord(patternStopTable, createdPatternStop.id);
    }

    @Test
    void canCreateUpdateAndDeleteNonFlexStopTime() throws IOException, SQLException, InvalidNamespaceException {
        final Table stopTimesTable = Table.STOP_TIMES;
        final Class<StopTimeDTO> stopDTOClass = StopTimeDTO.class;
        StopTimeDTO stopTime = StopTimeDTO.create();
        StopTimeDTO createdStopTime = saveRecord(stopTimesTable, stopTime, stopDTOClass);

        assertEquals(createdStopTime.stop_id, stopTime.stop_id);
        assertThatSqlQueryYieldsRowCount(getColumnsForId(createdStopTime.id, stopTimesTable), 1);

        createdStopTime.stop_id = "updated-stop-id-2";

        StopTimeDTO updatedStopTimeDTO = updateRecord(createdStopTime.id, stopTimesTable, createdStopTime, stopDTOClass);
        assertEquals(updatedStopTimeDTO.stop_id, createdStopTime.stop_id);

        ResultSet resultSet = getResultSetForId(updatedStopTimeDTO.id, stopTimesTable);
        while (resultSet.next()) {
            assertResultValue(resultSet, "stop_id", equalTo(createdStopTime.stop_id));
        }
        deleteRecord(stopTimesTable, createdStopTime.id);
    }

    @Test
    void canCreateUpdateAndDeleteLocationGroupStops() throws IOException, SQLException, InvalidNamespaceException {
        final Table table = Table.LOCATION_GROUP_STOPS;
        final Class<LocationGroupStopDTO> clazz = LocationGroupStopDTO.class;
        LocationGroupStopDTO locationGroupStop = LocationGroupStopDTO.create();
        LocationGroupStopDTO createdLocationGroupStop = saveRecord(table, locationGroupStop, clazz);
        // Set value to empty strings/null to later verify that it is set to null in the database.
        createdLocationGroupStop.stop_id = "";

        assertThat(createdLocationGroupStop.location_group_id, equalTo(locationGroupStop.location_group_id));
        assertThatSqlQueryYieldsRowCount(getColumnsForId(createdLocationGroupStop.id, table), 1);

        createdLocationGroupStop.location_group_id = "location-group-id-2";

        LocationGroupStopDTO updatedLocationGroupStopDTO = updateRecord(createdLocationGroupStop.id, table, createdLocationGroupStop, clazz);
        assertEquals(updatedLocationGroupStopDTO.location_group_id, createdLocationGroupStop.location_group_id);
        assertNull(updatedLocationGroupStopDTO.stop_id);

        ResultSet resultSet = getResultSetForId(updatedLocationGroupStopDTO.id, table);
        while (resultSet.next()) {
            assertResultValue(resultSet, "location_group_id", equalTo(createdLocationGroupStop.location_group_id));
            assertResultValue(resultSet, "stop_id",  Matchers.nullValue());
        }
        deleteRecord(table, createdLocationGroupStop.id);
    }

    @Test
    void canValidateLocationShapes() throws IOException, SQLException, InvalidNamespaceException {
        final Table table = Table.LOCATIONS;
        final Class<LocationDTO> clazz = LocationDTO.class;
        try {
            saveRecord(table, LocationDTO.create("location-id-1", 3, true), clazz);
        } catch (IOException e) {
            assertEquals(polygonCornerCountErrorMessage, e.getMessage());
        }
        LocationDTO createdLocation = saveRecord(table, LocationDTO.create("location-id-2", 4, true), clazz);

        assertEquals(createdLocation.location_shapes[0].geometry_pt_lat, createdLocation.location_shapes[3].geometry_pt_lat);
        assertEquals(createdLocation.location_shapes[0].geometry_pt_lon, createdLocation.location_shapes[3].geometry_pt_lon);

        createdLocation = saveRecord(table, LocationDTO.create("location-id-3", 4, false), clazz);

        assertEquals(createdLocation.location_shapes[0].geometry_pt_lat, createdLocation.location_shapes[4].geometry_pt_lat);
        assertEquals(createdLocation.location_shapes[0].geometry_pt_lon, createdLocation.location_shapes[4].geometry_pt_lon);
    }

    @Test
    void canCreateUpdateAndDeleteLocation() throws IOException, SQLException, InvalidNamespaceException {
        final Table table = Table.LOCATIONS;
        final Class<LocationDTO> clazz = LocationDTO.class;
        LocationDTO location = LocationDTO.create();
        LocationDTO createdLocation = saveRecord(table, location, clazz);

        assertThat(createdLocation.location_id, equalTo(location.location_id));
        assertThatSqlQueryYieldsRowCount(getColumnsForId(createdLocation.id, table), 1);

        createdLocation.location_id = "location-id-2";
        LocationDTO updatedLocationDTO = updateRecord(createdLocation.id, table, createdLocation, clazz);
        assertEquals(updatedLocationDTO.location_id, createdLocation.location_id);

        // Verify that certain values are correctly set in the location table.
        ResultSet locationResultSet = getResultSetForId(updatedLocationDTO.id, table);
        while (locationResultSet.next()) {
            assertResultValue(locationResultSet, "location_id", equalTo(createdLocation.location_id));
            assertResultValue(locationResultSet, "stop_name", equalTo(createdLocation.stop_name));
            assertResultValue(locationResultSet, "stop_desc", equalTo(createdLocation.stop_desc));
            assertResultValue(locationResultSet, "zone_id", equalTo(createdLocation.zone_id));
            assertResultValue(locationResultSet, "stop_url", equalTo(createdLocation.stop_url.toString()));
            assertResultValue(locationResultSet, "geometry_type", equalTo(createdLocation.geometry_type));
        }
        deleteRecord(table, createdLocation.id);
    }

    @Test
    void canCreateUpdateAndDeleteLocationShapes() throws IOException, SQLException, InvalidNamespaceException {
        final Table table = Table.LOCATION_SHAPES;
        final Class<LocationShapeDTO> clazz = LocationShapeDTO.class;
        LocationShapeDTO locationShape = LocationShapeDTO.create();
        LocationShapeDTO createdLocationShape = saveRecord(table, locationShape, clazz);

        assertEquals(createdLocationShape.location_id, locationShape.location_id);
        assertThatSqlQueryYieldsRowCount(getColumnsForId(createdLocationShape.id, table), 1);

        createdLocationShape.location_id = "location-shape-2";
        LocationShapeDTO updatedLocationShapeDTO = updateRecord(createdLocationShape.id, table, createdLocationShape, clazz);
        assertEquals(updatedLocationShapeDTO.location_id, createdLocationShape.location_id);

        // Verify that certain values are correctly set in the database.
        ResultSet resultSet = getResultSetForId(updatedLocationShapeDTO.id, table);
        while (resultSet.next()) {
            assertResultValue(resultSet, "geometry_pt_lat", equalTo(createdLocationShape.geometry_pt_lat));
            assertResultValue(resultSet, "geometry_pt_lon", equalTo(createdLocationShape.geometry_pt_lon));
        }
        deleteRecord(table, createdLocationShape.id);
    }

    @Test
    void canCreateUpdateAndDeleteStopTimes() throws IOException, SQLException, InvalidNamespaceException {
        final Table table = Table.STOP_TIMES;
        final Class<StopTimeWithFlexDTO> clazz = StopTimeWithFlexDTO.class;
        StopTimeWithFlexDTO stopTimeWithFlex = StopTimeWithFlexDTO.create();
        StopTimeWithFlexDTO createdStopTime = saveRecord(table, stopTimeWithFlex, clazz);

        assertEquals(createdStopTime.stop_id, stopTimeWithFlex.stop_id);
        assertThatSqlQueryYieldsRowCount(getColumnsForId(createdStopTime.id, table), 1);
        createdStopTime.stop_id = "stop-id-2";
        createdStopTime.pickup_booking_rule_id = "2";
        createdStopTime.drop_off_booking_rule_id = "3";
        createdStopTime.start_pickup_drop_off_window = 60;
        createdStopTime.end_pickup_drop_off_window = 60;
        StopTimeWithFlexDTO updatedStopTimeWithFlexDTO =  updateRecord(createdStopTime.id, table, createdStopTime, clazz);
        assertEquals(updatedStopTimeWithFlexDTO.stop_id, createdStopTime.stop_id);

        // Verify that certain values are correctly set in the database.
        ResultSet resultSet = getResultSetForId(updatedStopTimeWithFlexDTO.id, table);
        while (resultSet.next()) {
            assertResultValue(resultSet, "pickup_booking_rule_id", equalTo(createdStopTime.pickup_booking_rule_id));
            assertResultValue(resultSet, "drop_off_booking_rule_id", equalTo(createdStopTime.drop_off_booking_rule_id));
            assertResultValue(resultSet, "start_pickup_drop_off_window", equalTo(createdStopTime.start_pickup_drop_off_window));
            assertResultValue(resultSet, "end_pickup_drop_off_window", equalTo(createdStopTime.end_pickup_drop_off_window));
        }
        deleteRecord(table, createdStopTime.id);
    }

    /**
     * Ensure that a simple {@link ScheduleException} can be created, updated, and deleted.
     */
    @Test
    void canCreateUpdateAndDeleteScheduleExceptions() throws IOException, SQLException, InvalidNamespaceException {
        final Table scheduleExceptionTable = Table.SCHEDULE_EXCEPTIONS;
        ScheduleExceptionDTO exceptionInput = ScheduleExceptionDTO.create();
        ScheduleExceptionDTO scheduleException = saveRecord(Table.SCHEDULE_EXCEPTIONS, exceptionInput, ScheduleExceptionDTO.class);

        assertThat(scheduleException.removed_service[0], equalTo(SIMPLE_SERVICE_ID));
        ResultSet resultSet = getResultSetForId(scheduleException.id, scheduleExceptionTable, "removed_service");
        while (resultSet.next()) {
            String[] array = (String[]) resultSet.getArray(1).getArray();
            for (int i = 0; i < array.length; i++) {
                assertEquals(exceptionInput.removed_service[i], array[i]);
            }
        }

        String[] updatedDates = new String[] {"20191031", "20201031"};
        scheduleException.dates = updatedDates;
        ScheduleExceptionDTO updatedDTO = updateRecord(scheduleException.id, Table.SCHEDULE_EXCEPTIONS, scheduleException, ScheduleExceptionDTO.class);

        assertThat(updatedDTO.dates, equalTo(updatedDates));
        ResultSet rs2 = getResultSetForId(scheduleException.id, scheduleExceptionTable, "dates");
        while (rs2.next()) {
            String[] array = (String[]) rs2.getArray(1).getArray();
            for (int i = 0; i < array.length; i++) {
                assertEquals(updatedDates[i], array[i]);
            }
        }
        deleteRecord(Table.SCHEDULE_EXCEPTIONS, scheduleException.id);
    }

    /**
     * Ensure that {@link ScheduleException}s which are loaded from an existing GTFS can be removed properly,
     * including created entries in calendar_dates.
     */
    @Test
    void canCreateAndDeleteCalendarDates() throws IOException, SQLException, InvalidNamespaceException {
        String firstServiceId = "REMOVED";
        String secondServiceId = "ADDED";
        String[] allServiceIds = new String[] {firstServiceId, secondServiceId};
        String[] holidayDates = new String[] {"20190812", "20190813", "20190814"};

        // Create new schedule exception which involves 2 service IDs and multiple dates
        ScheduleExceptionDTO exceptionInput = new ScheduleExceptionDTO();
        exceptionInput.name = "Incredible multi day holiday";
        exceptionInput.exemplar = 9; // Add, swap, or remove type
        exceptionInput.removed_service = new String[] {firstServiceId};
        exceptionInput.added_service = new String[] {secondServiceId};
        exceptionInput.dates = holidayDates;

        // Save the schedule exception
        ScheduleExceptionDTO scheduleException = saveRecord(Table.SCHEDULE_EXCEPTIONS, exceptionInput, ScheduleExceptionDTO.class);

        // Create a calendar_dates entry for each date of the schedule exception
        for (String date: holidayDates) {
            // firstServiceId is removed
            saveRecord(Table.CALENDAR_DATES, CalendarDateDTO.create(firstServiceId, date, 2), CalendarDateDTO.class);
            // secondServiceId is added
            saveRecord(Table.CALENDAR_DATES, CalendarDateDTO.create(secondServiceId, date, 1), CalendarDateDTO.class);
        }

        // Delete a schedule exception
        deleteRecord(Table.SCHEDULE_EXCEPTIONS, scheduleException.id);

        // Verify that the entries in calendar_dates are removed after deleting the schedule exception.
        for (String date : holidayDates) {
            for (String serviceId : allServiceIds){
                String sql = String.format("select * from %s.%s where service_id = '%s' and date = '%s'",
                    testNamespace,
                    Table.CALENDAR_DATES.name,
                    serviceId,
                    date
                );
                assertThatSqlQueryYieldsZeroRows(sql);
            }
        }
    }

    /**
     * This test verifies that stop_times#shape_dist_traveled and other linked fields are updated when a pattern
     * is updated.
     */
    @Test
    void shouldUpdateStopTimeOnPatternStopUpdate() throws IOException, SQLException, InvalidNamespaceException {
        final String[] STOP_TIMES_LINKED_FIELDS = new String[] {
            "shape_dist_traveled",
            "timepoint",
            "drop_off_type",
            "pickup_type",
            "continuous_pickup",
            "continuous_drop_off"
        };
        String routeId = newUUID();
        String patternId = newUUID();
        int startTime = 6 * 60 * 60; // 6 AM
        PatternDTO pattern = createRouteAndPattern(
            routeId,
            patternId,
            "pattern name",
            null,
            new ShapePointDTO[] {},
            new PatternStopWithFlexDTO[] {
                new PatternStopWithFlexDTO(patternId, FIRST_STOP_ID, 0),
                new PatternStopWithFlexDTO(patternId, LAST_STOP_ID, 1)
            },
            0
        );
        // Make sure saved data matches expected data.
        assertThat(pattern.route_id, equalTo(routeId));
        // Create trip so we can check that the stop_time values are updated after the patter update.
        TripDTO tripInput = constructTimetableTrip(pattern.pattern_id, pattern.route_id, startTime, 60);
        // Set trip_id to empty string to verify that it gets overwritten with auto-generated UUID.
        tripInput.trip_id = "";
        TripDTO createdTrip = saveRecord(Table.TRIPS, tripInput, TripDTO.class);

        // Check that trip_id is not empty.
        assertNotEquals("", createdTrip.trip_id);

        // Check that trip_id is a UUID.
        LOG.info("New trip_id = {}", createdTrip.trip_id);
        UUID uuid = UUID.fromString(createdTrip.trip_id);
        assertNotNull(uuid);
        // Check that trip exists.
        assertThatSqlQueryYieldsRowCount(getColumnsForId(createdTrip.id, Table.TRIPS), 1);

        // Check the stop_time's initial shape_dist_traveled value and other linked fields.
        PreparedStatement statement = connection.prepareStatement(
            String.format(
                "select %s from %s.stop_times where stop_sequence=1 and trip_id='%s'",
                String.join(", ", STOP_TIMES_LINKED_FIELDS),
                testNamespace,
                createdTrip.trip_id
            )
        );
        LOG.info(statement.toString());
        ResultSet resultSet = statement.executeQuery();
        while (resultSet.next()) {
            // First stop_time shape_dist_traveled should be zero.
            // Other linked fields should be interpreted as zero too.
            for (int i = 1; i <= STOP_TIMES_LINKED_FIELDS.length; i++) {
                assertThat(resultSet.getInt(i), equalTo(0));
            }
        }

        // Update pattern_stop#shape_dist_traveled and check that the stop_time's shape_dist value is updated.
        final double updatedShapeDistTraveled = 45.5;
        PatternStopWithFlexDTO patternStop = pattern.pattern_stops[1];
        patternStop.shape_dist_traveled = updatedShapeDistTraveled;
        // Assign an arbitrary value (the order of appearance in STOP_TIMES_LINKED_FIELDS) for the other linked fields.
        patternStop.timepoint = 2;
        patternStop.drop_off_type = 3;
        patternStop.pickup_type = 4;
        patternStop.continuous_pickup = 5;
        patternStop.continuous_drop_off = 6;
        updateRecord(pattern.id, Table.PATTERNS, pattern, PatternDTO.class);

        resultSet = statement.executeQuery();
        while (resultSet.next()) {
            // First stop_time shape_dist_traveled should be updated.
            assertThat(resultSet.getDouble(1), equalTo(updatedShapeDistTraveled));

            // Other linked fields should be as set above.
            for (int i = 2; i <= STOP_TIMES_LINKED_FIELDS.length; i++) {
                assertThat(resultSet.getInt(i), equalTo(i));
            }
        }
    }

    @Test
    void shouldDeleteReferencingTripsAndStopTimesOnPatternDelete() throws IOException, SQLException, InvalidNamespaceException {
        String routeId = "9834914";
        int startTime = 6 * 60 * 60; // 6 AM
        PatternDTO pattern = createRouteAndSimplePattern(routeId, "9901900", "The Line");
        // make sure saved data matches expected data
        assertThat(pattern.route_id, equalTo(routeId));

        TripDTO tripInput = constructTimetableTrip(pattern.pattern_id, pattern.route_id, startTime, 60);
        TripDTO createdTrip = saveRecord(Table.TRIPS, tripInput, TripDTO.class);
        assertThatSqlQueryYieldsRowCount(getColumnsForId(createdTrip.id, Table.TRIPS), 1);

        // Delete pattern record
        deleteRecord(Table.PATTERNS, pattern.id);

        // Check that pattern record does not exist in DB
        assertThatSqlQueryYieldsZeroRows(getColumnsForId(pattern.id, Table.PATTERNS));

        // Check that trip records for pattern do not exist in DB
        assertThatSqlQueryYieldsZeroRows(
            String.format(
                "select * from %s.%s where pattern_id='%s'",
                testNamespace,
                Table.TRIPS.name,
                pattern.pattern_id
            )
        );

        // Check that stop_times records for trip do not exist in DB
        assertThatSqlQueryYieldsZeroRows(
            String.format(
                "select * from %s.%s where trip_id='%s'",
                testNamespace,
                Table.STOP_TIMES.name,
                createdTrip.trip_id
            )
        );
    }

    /**
     * Deleting a route should also delete related shapes because they are unique to this route.
     */
    @Test
    void shouldDeleteRouteShapes() throws IOException, SQLException, InvalidNamespaceException {
        String routeId = "8472017";
        String shapeId = "uniqueShapeId";

        createThenDeleteRoute(routeId, shapeId);

        // Check that shape records for pattern do not exist in DB
        assertThatSqlQueryYieldsZeroRows(
            String.format(
                "select * from %s where shape_id = '%s'",
                String.format("%s.%s", testNamespace, Table.SHAPES.name),
                shapeId
            )
        );
    }

    /**
     * Deleting a route should retain shapes that are shared by multiple patterns.
     */
    @Test
    void shouldRetainSharedShapes() throws IOException, SQLException, InvalidNamespaceException {
        String routeId = "8472017";
        String shapeId = "sharedShapeId";

        // Create a pattern which uses the same shape as the pattern that will be deleted. This is to prevent the shape
        // from being deleted.
        saveRecord(Table.PATTERNS, PatternDTO.create("111222", "8802800", "The Line", shapeId), PatternDTO.class);
        createThenDeleteRoute(routeId, shapeId);

        // Check that shape records persist in DB. Two shapes are created per pattern. Two patterns equals four shapes.
        assertThatSqlQueryYieldsRowCount(
            String.format(
                "select * from %s where shape_id = '%s'",
                String.format("%s.%s", testNamespace, Table.SHAPES.name),
                shapeId
            ), 4
        );
    }

    /**
     * Create a route with related pattern, trip and stop times. Confirm entities have been created successfully, then
     * delete the route to trigger cascade deleting of patterns, trips, stop times and shapes.
     *
     */
    private void createThenDeleteRoute(String routeId, String shapeId)
        throws InvalidNamespaceException, SQLException, IOException {

        int startTime = 6 * 60 * 60; // 6 AM

        RouteDTO createdRoute = saveRecord(Table.ROUTES, RouteDTO.create(routeId), RouteDTO.class);
        PatternDTO pattern = saveRecord(Table.PATTERNS, PatternDTO.create(routeId, "9901900", "The Line", shapeId), PatternDTO.class);

        // Make sure saved data matches expected data.
        assertThat(pattern.route_id, equalTo(routeId));

        TripDTO createdTrip = saveRecord(Table.TRIPS, TripDTO.create(pattern.pattern_id, pattern.route_id, startTime), TripDTO.class);
        assertThatSqlQueryYieldsRowCount(getColumnsForId(createdTrip.id, Table.TRIPS), 1);

        deleteRecord(Table.ROUTES, createdRoute.id);
        confirmRemovalOfRouteRelatedData(pattern.id, pattern.pattern_id, createdTrip.trip_id);
    }

    /**
     * Confirm that items related to a route no longer exist after a cascade delete.
     */
    private void confirmRemovalOfRouteRelatedData(Integer id, String patternId, String tripId) throws SQLException {
        // Check that pattern record does not exist in DB
        assertThatSqlQueryYieldsZeroRows(getColumnsForId(id, Table.PATTERNS));

        // Check that trip records for pattern do not exist in DB
        assertThatSqlQueryYieldsZeroRows(
            String.format(
                "select * from %s.%s where pattern_id='%s'",
                testNamespace,
                Table.TRIPS.name,
                patternId
            )
        );

        // Check that stop_times records for trip do not exist in DB
        assertThatSqlQueryYieldsZeroRows(
            String.format(
                "select * from %s.%s where trip_id='%s'",
                testNamespace,
                Table.STOP_TIMES.name,
                tripId
            )
        );

        // Check that pattern_stops records for pattern do not exist in DB
        assertThatSqlQueryYieldsZeroRows(
            String.format(
                "select * from %s.%s where pattern_id='%s'",
                testNamespace,
                Table.PATTERN_STOP.name,
                patternId
            )
        );

        // Check that frequency records for trip do not exist in DB
        assertThatSqlQueryYieldsZeroRows(
            String.format(
                "select * from %s.%s where trip_id='%s'",
                testNamespace,
                Table.FREQUENCIES.name,
                tripId
            )
        );
    }

    /**
     * Test that a frequency trip entry CANNOT be added for a timetable-based pattern. Expects an exception to be thrown.
     */
    @Test
    void cannotCreateFrequencyForTimetablePattern() throws SQLException, InvalidNamespaceException, IOException {
        PatternDTO simplePattern = createRouteAndSimplePattern("900", "8", "The Loop");
        JdbcTableWriter createTripWriter = createTestTableWriter(Table.TRIPS);
        String json = mapper.writeValueAsString(TripDTO.create(simplePattern.pattern_id, simplePattern.route_id, 6 * 60 * 60));
        Assertions.assertThrows(IllegalStateException.class, () -> {
            createTripWriter.create(json, true);
        });
    }

    /**
     * When multiple patterns reference a single shape_id, the returned JSON from an update to any of these patterns
     * (whether the shape points were updated or not) should have a new shape_id because of the "copy on update" logic
     * that ensures the shared shape is not modified.
     */
    @Test
    void shouldChangeShapeIdOnPatternUpdate() throws IOException, SQLException, InvalidNamespaceException {
        String patternId = "10";
        String sharedShapeId = "shared_shape_id";
        ShapePointDTO[] shapes = new ShapePointDTO[] {
            new ShapePointDTO(2, 0.0, sharedShapeId, FIRST_STOP_LAT, FIRST_STOP_LON, 0),
            new ShapePointDTO(2, 150.0, sharedShapeId, LAST_STOP_LAT, LAST_STOP_LON, 1)
        };
        PatternStopWithFlexDTO[] patternStops = new PatternStopWithFlexDTO[] {
            new PatternStopWithFlexDTO(patternId, FIRST_STOP_ID, 0),
            new PatternStopWithFlexDTO(patternId, LAST_STOP_ID, 1)
        };
        PatternDTO simplePattern = createRouteAndPattern("1001", patternId, "The Line", sharedShapeId, shapes, patternStops, 0);
        assertThat(simplePattern.shape_id, equalTo(sharedShapeId));
        // Create pattern with shared shape. Note: typically we would encounter shared shapes on imported feeds (e.g.,
        // BART), but this should simulate the situation well enough.
        String secondPatternId = "11";
        patternStops[0].pattern_id = secondPatternId;
        patternStops[1].pattern_id = secondPatternId;
        PatternDTO patternWithSharedShape = createRouteAndPattern("1002", secondPatternId, "The Line 2", sharedShapeId, shapes, patternStops, 0);
        // Verify that shape_id is shared.
        assertThat(patternWithSharedShape.shape_id, equalTo(sharedShapeId));

        // Update any field on one of the patterns.
        patternWithSharedShape.name = "The shape_id should update";
        PatternDTO updatedSharedPattern = updateRecord(patternWithSharedShape.id, Table.PATTERNS, patternWithSharedShape, PatternDTO.class);
        String newShapeId = updatedSharedPattern.shape_id;
        assertThat(newShapeId, not(equalTo(sharedShapeId)));

        // Ensure that pattern record in database reflects updated shape ID.
        assertThatSqlQueryYieldsRowCount(
            String.format(
                "select * from %s.%s where shape_id='%s' and pattern_id='%s'",
                testNamespace,
                Table.PATTERNS.name,
                newShapeId,
                secondPatternId
            ),
            1
        );
    }

    /**
     * Checks that creating a frequency trip functions properly. This also updates the pattern to include pattern stops,
     * which is a prerequisite for creating a frequency trip with stop times.
     */
    @Test
    void canCreateUpdateAndDeleteFrequencyTripForFrequencyPattern() throws IOException, SQLException, InvalidNamespaceException {
        final Table tripsTable = Table.TRIPS;
        int startTime = 6 * 60 * 60;
        PatternDTO simplePattern = createRouteAndSimplePattern("1000", "9", "The Line");
        TripDTO tripInput = TripDTO.create(simplePattern.pattern_id, simplePattern.route_id, startTime);
        // Update pattern with pattern stops, set to use frequencies
        simplePattern.use_frequency = 1;
        simplePattern.pattern_stops = new PatternStopWithFlexDTO[] {
            new PatternStopWithFlexDTO(simplePattern.pattern_id, FIRST_STOP_ID, 0),
            new PatternStopWithFlexDTO(simplePattern.pattern_id, LAST_STOP_ID, 1)
        };

        updateRecord(simplePattern.id, Table.PATTERNS, simplePattern, PatternDTO.class);
        TripDTO createdTrip = saveRecord(Table.TRIPS, tripInput, TripDTO.class);

        createdTrip.trip_id = "100A";
        TripDTO updatedTrip = updateRecord(createdTrip.id, Table.TRIPS, createdTrip, TripDTO.class);
        assertEquals(updatedTrip.frequencies[0].start_time, startTime);
        assertEquals(updatedTrip.trip_id, createdTrip.trip_id);
        deleteRecord(Table.TRIPS, createdTrip.id);

        // Check that trip record does not exist in DB
        assertThatSqlQueryYieldsZeroRows(
            String.format(
                "select * from %s.%s where id=%d",
                testNamespace,
                tripsTable.name,
                updatedTrip.id
            ));
        // Check that stop_times records do not exist in DB
        assertThatSqlQueryYieldsZeroRows(
            String.format(
                "select * from %s.%s where trip_id='%s'",
                testNamespace,
                Table.STOP_TIMES.name,
                updatedTrip.trip_id
            ));
    }

    private static String normalizeStopsForPattern(
        PatternStopWithFlexDTO[] patternStops,
        int updatedStopSequence,
        boolean interpolateStopTimes,
        int initialTravelTime,
        int updatedTravelTime,
        int startTime,
        String patternId
    ) throws SQLException, InvalidNamespaceException, IOException {
        final Table tripsTable = Table.TRIPS;
        patternStops[1].default_travel_time = initialTravelTime;
        PatternDTO pattern = createRouteAndPattern(newUUID(),
            patternId,
            "Pattern A",
            null,
            new ShapePointDTO[] {},
            patternStops,
            0
        );

        // Create trip with travel times that match pattern stops.
        TripDTO createdTrip = saveRecord(Table.TRIPS, constructTimetableTrip(pattern.pattern_id, pattern.route_id, startTime, initialTravelTime), TripDTO.class);

        // Update pattern stop with new travel time.
        pattern.pattern_stops[updatedStopSequence].default_travel_time = updatedTravelTime;
        updateRecord(pattern.id, Table.PATTERNS, pattern, PatternDTO.class);

        // Normalize stop times.
        JdbcTableWriter updateTripWriter = createTestTableWriter(tripsTable);
        updateTripWriter.normalizeStopTimesForPattern(pattern.id, 0, interpolateStopTimes);

        return createdTrip.trip_id;
    }

    /**
     * Checks that {@link JdbcTableWriter#normalizeStopTimesForPattern(int, int, boolean)} can interpolate stop times between timepoints.
     */
    @Test
    void canInterpolatePatternStopTimes() throws IOException, SQLException, InvalidNamespaceException {
        // Parameters are shared with canNormalizePatternStopTimes, but maintained for test flexibility.
        int startTime = 6 * 60 * 60; // 6AM
        int initialTravelTime = 60; // seconds
        int updatedTravelTime = 600; // ten minutes
        String patternId = "123456-interpolated";
        double[] shapeDistTraveledValues = new double[] {0.0, 300.0, 600.0};
        double timepointTravelTime = (shapeDistTraveledValues[2] - shapeDistTraveledValues[0]) / updatedTravelTime; // 1 m/s

        // Create the array of patterns, set the timepoints properly.
        PatternStopWithFlexDTO[] patternStops = new PatternStopWithFlexDTO[]{
            new PatternStopWithFlexDTO(patternId, FIRST_STOP_ID, 0, 1, shapeDistTraveledValues[0]),
            new PatternStopWithFlexDTO(patternId, SECOND_STOP_ID, 1, 0, shapeDistTraveledValues[1]),
            new PatternStopWithFlexDTO(patternId, LAST_STOP_ID, 2, 1, shapeDistTraveledValues[2]),
        };

        patternStops[2].default_travel_time = initialTravelTime;

        // Pass the array of patterns to the body method with param
        String createdTripId = normalizeStopsForPattern(patternStops, 2, true, initialTravelTime, updatedTravelTime, startTime, patternId);

        // Read pattern stops from database and check that the arrivals/departures have been updated.
        JDBCTableReader<StopTime> stopTimesTable = new JDBCTableReader(Table.STOP_TIMES,
            testDataSource,
            testNamespace + ".",
            EntityPopulator.STOP_TIME);
        int index = 0;
        for (StopTime stopTime : stopTimesTable.getOrdered(createdTripId)) {
            LOG.info("stop times i={} arrival={} departure={}", index, stopTime.arrival_time, stopTime.departure_time);
            int calculatedArrivalTime = (int) (startTime + shapeDistTraveledValues[index] * timepointTravelTime);
            assertThat(stopTime.arrival_time, equalTo(calculatedArrivalTime));
            index++;
        }
    }

    /**
     * Checks that {@link JdbcTableWriter#normalizeStopTimesForPattern(int, int, boolean)} can normalize stop times to a pattern's
     * default travel times.
     */
    @Test
    void canNormalizePatternStopTimes() throws IOException, SQLException, InvalidNamespaceException {
        // Parameters are shared with canNormalizePatternStopTimes, but maintained for test flexibility.
        int initialTravelTime = 60; // one minute
        int startTime = 6 * 60 * 60; // 6AM
        int updatedTravelTime = 3600;
        String patternId = "123456";

        PatternStopWithFlexDTO[] patternStops = new PatternStopWithFlexDTO[]{
            new PatternStopWithFlexDTO(patternId, FIRST_STOP_ID, 0),
            new PatternStopWithFlexDTO(patternId, LAST_STOP_ID, 1)
        };

        String createdTripId = normalizeStopsForPattern(
            patternStops,
            1,
            false,
            initialTravelTime,
            updatedTravelTime,
            startTime,
            patternId
        );
        JDBCTableReader<StopTime> stopTimesTable = new JDBCTableReader(
            Table.STOP_TIMES,
            testDataSource,
            testNamespace + ".",
            EntityPopulator.STOP_TIME
        );
        int index = 0;
        for (StopTime stopTime : stopTimesTable.getOrdered(createdTripId)) {
            LOG.info("stop times i={} arrival={} departure={}", index, stopTime.arrival_time, stopTime.departure_time);
            assertThat(stopTime.arrival_time, equalTo(startTime + index * updatedTravelTime));
            index++;
        }
        // Ensure that updated stop times equals pattern stops length
        assertThat(index, equalTo(patternStops.length));
    }

    /**
     * Checks that {@link JdbcTableWriter#normalizeStopTimesForPattern(int, int)} can normalize stop times for flex
     * patterns.
     */
    @Test
    void canNormalizePatternStopTimesForFlex() throws IOException, SQLException, InvalidNamespaceException {
        int startTime = 6 * 60 * 60; // 6AM
        String patternId = newUUID();

        StopDTO stopOne = saveRecord(Table.STOPS, StopDTO.create(newUUID(), "-stop-1", 0.0, 0.0), StopDTO.class);
        StopDTO stopTwo = saveRecord(Table.STOPS, StopDTO.create(newUUID(), "-stop-2", 0.0, 0.0), StopDTO.class);
        LocationDTO locationOne = saveRecord(Table.LOCATIONS, LocationDTO.create(newUUID() + "location-1"), LocationDTO.class);
        LocationDTO locationTwo = saveRecord(Table.LOCATIONS, LocationDTO.create(newUUID() + "location-2"), LocationDTO.class);
        LocationGroupDTO locationGroup1 = saveRecord(Table.LOCATION_GROUP, LocationGroupDTO.create(newUUID() + "location-group-1"), LocationGroupDTO.class);
        LocationGroupDTO locationGroup2 = saveRecord(Table.LOCATION_GROUP, LocationGroupDTO.create(newUUID() + "location-group-2"), LocationGroupDTO.class);

        int travelTime = 60;
        PatternStopWithFlexDTO[] patternStops = new PatternStopWithFlexDTO[] {
            new PatternStopWithFlexDTO(patternId, stopOne.stop_id, 0,0),
            new PatternStopWithFlexDTO(patternId, stopTwo.stop_id, travelTime, 1),
            PatternStopWithFlexDTO.createFlexPatternStop(patternId, null, locationOne.location_id, 2, travelTime),
            PatternStopWithFlexDTO.createFlexPatternStop(patternId, null, locationTwo.location_id, 3, travelTime),
            PatternStopWithFlexDTO.createFlexPatternStop(patternId, locationGroup1.location_group_id, null, 4, travelTime),
            PatternStopWithFlexDTO.createFlexPatternStop(patternId, locationGroup2.location_group_id, null, 5, travelTime)
        };

        PatternDTO pattern = createRouteAndPattern(
            newUUID(),
            patternId,
            "pattern-1",
            null,
            new ShapePointDTO[] {},
            patternStops,
            0
        );

        int cumulativeTravelTime = startTime + travelTime;
        StopTimeWithFlexDTO[] stopTimes = new StopTimeWithFlexDTO[] {
            new StopTimeWithFlexDTO(stopOne.stop_id, startTime, startTime, 0),
            new StopTimeWithFlexDTO(stopTwo.stop_id, startTime, startTime, 1),
            StopTimeWithFlexDTO.create(null, locationOne.location_id, cumulativeTravelTime, cumulativeTravelTime, 2),
            StopTimeWithFlexDTO.create(null, locationTwo.location_id, (cumulativeTravelTime += travelTime), cumulativeTravelTime, 3),
            StopTimeWithFlexDTO.create(locationGroup1.location_group_id, null, (cumulativeTravelTime += travelTime), cumulativeTravelTime, 4),
            StopTimeWithFlexDTO.create(locationGroup2.location_group_id, null, (cumulativeTravelTime += travelTime), cumulativeTravelTime, 5)
        };

        TripDTO createdTrip = saveRecord(Table.TRIPS, TripDTO.create(pattern.pattern_id, pattern.route_id, stopTimes), TripDTO.class);

        checkStopArrivalAndDepartures(createdTrip.trip_id, startTime, 0, travelTime, patternStops.length);

        // Update pattern stop with new travel time.
        int updatedTravelTime = 3600; // one hour
        pattern.pattern_stops[1].default_travel_time = updatedTravelTime;
        updateRecord(pattern.id, Table.PATTERNS, pattern, PatternDTO.class);

        // Normalize stop times.
        JdbcTableWriter updateTripWriter = createTestTableWriter(Table.TRIPS);
        updateTripWriter.normalizeStopTimesForPattern(pattern.id, 0);
        checkStopArrivalAndDepartures(
            createdTrip.trip_id,
            startTime,
            updatedTravelTime,
            travelTime,
            patternStops.length
        );
    }

    /**
     * Read stop times from the database and check that the arrivals/departures have been set correctly.
     */
    private void checkStopArrivalAndDepartures(
        String tripId,
        int startTime,
        int updatedTravelTime,
        int travelTime,
        int totalNumberOfPatterns
    ) {
        JDBCTableReader<StopTime> stopTimesTable = new JDBCTableReader(Table.STOP_TIMES,
            testDataSource,
            testNamespace + ".",
            EntityPopulator.STOP_TIME
        );
        int index = 0;
        for (StopTime stopTime : stopTimesTable.getOrdered(tripId)) {
            // This expects the first two stop times to be normal stops and the reminder to be flex stops.
            if (stopTime.stop_sequence < 2) {
                LOG.info("stop times i={} arrival={} departure={}",
                    index,
                    stopTime.arrival_time,
                    stopTime.departure_time
                );
                assertEquals(stopTime.arrival_time, startTime + index * updatedTravelTime);
                assertEquals(stopTime.departure_time, startTime + index * updatedTravelTime);
            } else {
                LOG.info("stop times i={} start_pickup_drop_off_window={} end_pickup_drop_off_window={}",
                    index,
                    stopTime.start_pickup_drop_off_window,
                    stopTime.end_pickup_drop_off_window
                );
                assertEquals(stopTime.start_pickup_drop_off_window, startTime + updatedTravelTime + (index-1) * travelTime);
                assertEquals(stopTime.end_pickup_drop_off_window, startTime + updatedTravelTime + (index-1) * travelTime);
            }
            index++;
        }
        // Ensure that updated stop times equals pattern stops length
        assertEquals(index, totalNumberOfPatterns);
    }

    /**
     * This test makes sure that updated the service_id will properly update affected referenced entities properly.
     * This test case was initially developed to prove that https://github.com/conveyal/gtfs-lib/issues/203 is
     * happening.
     */
    @Test
    void canUpdateServiceId() throws InvalidNamespaceException, IOException, SQLException {
        // change the service id
        JdbcTableWriter tableWriter = new JdbcTableWriter(Table.CALENDAR, testDataSource, testGtfsGLSnapshotNamespace);
        tableWriter.update(
            2,
            "{\"id\":2,\"service_id\":\"test\",\"description\":\"MoTuWeThFrSaSu\",\"monday\":1,\"tuesday\":1,\"wednesday\":1,\"thursday\":1,\"friday\":1,\"saturday\":1,\"sunday\":1,\"start_date\":\"20180526\",\"end_date\":\"20201231\"}",
            true
        );

        // assert that the amount of stop times equals the original amount of stop times in the feed
        assertThatSqlQueryYieldsRowCount(
            String.format(
                "select * from %s.%s",
                testGtfsGLSnapshotNamespace,
                Table.STOP_TIMES.name
            ),
            53
        );
    }

    /**
     * Various test cases for pattern reconciliation. These tests are applied to a single pattern
     * see {@link JDBCTableWriterTest#patternReconciliationSetUp()} in the order defined.
     */
    private static Stream<PatternArguments> createPatternTests() {
        String patternId = pattern.pattern_id;
        return Stream.of(
            // Add a new stop to the end.
            new PatternArguments(
                new PatternStopWithFlexDTO[] {
                    PatternStopWithFlexDTO.createFlexPatternStop(patternId, null, locationOne.location_id, 0, 10, 10),
                    new PatternStopWithFlexDTO(patternId, stopOne.stop_id, 1, 10, 1),
                    new PatternStopWithFlexDTO(patternId, stopTwo.stop_id, 2, 10, 1)
                },
                ImmutableMap.of(
                    locationOne.location_id, "location_id",
                    stopOne.stop_id, "stop_id",
                    stopTwo.stop_id, "stop_id"
                ),
                10, 10, 10, 1
            ),
            // Delete stop from the middle.
            new PatternArguments(
                new PatternStopWithFlexDTO[] {
                    PatternStopWithFlexDTO.createFlexPatternStop(patternId, null, locationOne.location_id, 0, 20, 20),
                    new PatternStopWithFlexDTO(patternId, stopTwo.stop_id, 1, 12, 1)
                },
                ImmutableMap.of(
                    locationOne.location_id, "location_id",
                    stopTwo.stop_id, "stop_id"
                ),
                20, 20, 12, 1
            ),
            // Change the order of the location and stop.
            new PatternArguments(
                new PatternStopWithFlexDTO[] {
                    new PatternStopWithFlexDTO(patternId, stopTwo.stop_id, 0, 11, 1),
                    PatternStopWithFlexDTO.createFlexPatternStop(patternId, null, locationOne.location_id, 1, 30, 30)
                },
                ImmutableMap.of(
                    stopTwo.stop_id, "stop_id",
                    locationOne.location_id, "location_id"
                ),
                30, 30, 11, 1
            ),
            // Add a new location between the location and stop.
            new PatternArguments(
                new PatternStopWithFlexDTO[] {
                    new PatternStopWithFlexDTO(patternId, stopTwo.stop_id, 0, 12, 5),
                    PatternStopWithFlexDTO.createFlexPatternStop(patternId, null, locationTwo.location_id, 1, 40, 40),
                    PatternStopWithFlexDTO.createFlexPatternStop(patternId, null, locationOne.location_id, 2, 40, 40)
                },
                ImmutableMap.of(
                    stopTwo.stop_id, "stop_id",
                    locationTwo.location_id, "location_id",
                    locationOne.location_id, "location_id"
                ),
                40, 40, 12, 5
            ),
            // Add a new stop at the end.
            new PatternArguments(
                new PatternStopWithFlexDTO[] {
                    new PatternStopWithFlexDTO(patternId, stopTwo.stop_id, 0, 14, 3),
                    PatternStopWithFlexDTO.createFlexPatternStop(patternId, null, locationTwo.location_id, 1, 50, 50),
                    PatternStopWithFlexDTO.createFlexPatternStop(patternId, null, locationOne.location_id, 2, 50, 50),
                    new PatternStopWithFlexDTO(patternId, stopOne.stop_id, 3, 14, 3)
                },
                ImmutableMap.of(
                    stopTwo.stop_id, "stop_id",
                    locationTwo.location_id, "location_id",
                    locationOne.location_id, "location_id",
                    stopOne.stop_id, "stop_id"
                ),
                50, 50, 14, 3
            ),
            // Delete the first location.
            new PatternArguments(
                new PatternStopWithFlexDTO[] {
                    new PatternStopWithFlexDTO(patternId, stopTwo.stop_id, 0, 23, 1),
                    PatternStopWithFlexDTO.createFlexPatternStop(patternId, null, locationOne.location_id, 1, 60, 60),
                    new PatternStopWithFlexDTO(patternId, stopOne.stop_id, 2, 23, 1)
                },
                ImmutableMap.of(
                    stopTwo.stop_id, "stop_id",
                    locationOne.location_id, "location_id",
                    stopOne.stop_id, "stop_id"
                ),
                60, 60, 23, 1
            ),
            // Add a stop and location to the end.
            new PatternArguments(
                new PatternStopWithFlexDTO[] {
                    new PatternStopWithFlexDTO(patternId, stopTwo.stop_id, 0, 13, 6),
                    PatternStopWithFlexDTO.createFlexPatternStop(patternId, null, locationOne.location_id, 1, 70, 70),
                    new PatternStopWithFlexDTO(patternId, stopOne.stop_id, 2, 13, 6),
                    PatternStopWithFlexDTO.createFlexPatternStop(patternId, null, locationThree.location_id, 3, 70, 70),
                    new PatternStopWithFlexDTO(patternId, stopThree.stop_id, 4, 13, 6),
                },
                ImmutableMap.of(
                    stopTwo.stop_id, "stop_id",
                    locationOne.location_id, "location_id",
                    stopOne.stop_id, "stop_id",
                    locationThree.location_id, "location_id",
                    stopThree.stop_id, "stop_id"
                ),
                70, 70, 13, 6
            ),
            // Delete the last stop.
            new PatternArguments(
                new PatternStopWithFlexDTO[] {
                    new PatternStopWithFlexDTO(patternId, stopTwo.stop_id, 0, 13, 6),
                    PatternStopWithFlexDTO.createFlexPatternStop(patternId, null, locationOne.location_id, 1, 70, 70),
                    new PatternStopWithFlexDTO(patternId, stopOne.stop_id, 2, 13, 6),
                    PatternStopWithFlexDTO.createFlexPatternStop(patternId, null, locationThree.location_id, 3, 70, 70),
                },
                ImmutableMap.of(
                    stopTwo.stop_id, "stop_id",
                    locationOne.location_id, "location_id",
                    stopOne.stop_id, "stop_id",
                    locationThree.location_id, "location_id"
                ),
                70, 70, 13, 6
            ),
            // Add a location group to the end.
            new PatternArguments(
                new PatternStopWithFlexDTO[] {
                    new PatternStopWithFlexDTO(patternId, stopTwo.stop_id, 0, 13, 6),
                    PatternStopWithFlexDTO.createFlexPatternStop(patternId, null, locationOne.location_id, 1, 70, 70),
                    new PatternStopWithFlexDTO(patternId, stopOne.stop_id, 2, 13, 6),
                    PatternStopWithFlexDTO.createFlexPatternStop(patternId, null, locationThree.location_id, 3, 70, 70),
                    PatternStopWithFlexDTO.createFlexPatternStop(patternId, locationGroupOne.location_group_id, null, 4, 70, 70)
                },
                ImmutableMap.of(
                    stopTwo.stop_id, "stop_id",
                    locationOne.location_id, "location_id",
                    stopOne.stop_id, "stop_id",
                    locationThree.location_id, "location_id",
                    locationGroupOne.location_group_id, "location_group_id"
                ),
                70, 70, 13, 6
            ),
            // Delete stop from middle.
            new PatternArguments(
                new PatternStopWithFlexDTO[] {
                    new PatternStopWithFlexDTO(patternId, stopTwo.stop_id, 0, 13, 6),
                    PatternStopWithFlexDTO.createFlexPatternStop(patternId, null, locationOne.location_id, 1, 70, 70),
                    PatternStopWithFlexDTO.createFlexPatternStop(patternId, null, locationThree.location_id, 2, 70, 70),
                    PatternStopWithFlexDTO.createFlexPatternStop(patternId, locationGroupOne.location_group_id, null, 3, 70, 70)
                },
                ImmutableMap.of(
                    stopTwo.stop_id, "stop_id",
                    locationOne.location_id, "location_id",
                    locationThree.location_id, "location_id",
                    locationGroupOne.location_group_id, "location_group_id"
                ),
                70, 70, 13, 6
            ),
            // Add stop area to middle.
            new PatternArguments(
                new PatternStopWithFlexDTO[] {
                    new PatternStopWithFlexDTO(patternId, stopTwo.stop_id, 0, 13, 6),
                    PatternStopWithFlexDTO.createFlexPatternStop(patternId, null, locationOne.location_id, 1, 70, 70),
                    PatternStopWithFlexDTO.createFlexPatternStop(patternId, locationGroupTwo.location_group_id, null, 2, 70, 70),
                    PatternStopWithFlexDTO.createFlexPatternStop(patternId, null, locationThree.location_id, 3, 70, 70),
                    PatternStopWithFlexDTO.createFlexPatternStop(patternId, locationGroupOne.location_group_id, null, 4, 70, 70)
                },
                ImmutableMap.of(
                    stopTwo.stop_id, "stop_id",
                    locationOne.location_id, "location_id",
                    locationGroupTwo.location_group_id, "location_group_id",
                    locationThree.location_id, "location_id",
                    locationGroupOne.location_group_id, "location_group_id"
                ),
                70, 70, 13, 6
            )
        );
    }

    /**
     * Test that the stop times for a pattern are correctly ordered after pattern updates and that the travel times are
     * correctly recalculated inline with this.
     */
    @ParameterizedTest
    @MethodSource("createPatternTests")
    void canReconcilePatterns(PatternArguments patternArguments)
        throws IOException, SQLException, InvalidNamespaceException {

        int cumulativeTravelTime = 0;
        pattern.pattern_stops = patternArguments.patternStops;
        updateRecord(pattern.id, Table.PATTERNS, pattern, PatternDTO.class);

        int stopSequence = 0;
        for (Map.Entry<String, String> entry : patternArguments.referenceIdAndColumn.entrySet()) {
            boolean flex = isFlex(entry);
            verifyStopTime(
                createdTrip.trip_id,
                entry.getKey(),
                entry.getValue(),
                stopSequence,
                (cumulativeTravelTime += flex ? patternArguments.expectedFlexDefaultTravelTime : patternArguments.expectedDefaultTravelTime),
                (cumulativeTravelTime += flex ? patternArguments.expectedFlexDefaultZoneTime : patternArguments.expectedDefaultDwellTime)
            );
            stopSequence++;
        }
    }

    private boolean isFlex(Map.Entry<String, String> entry) {
        return entry.getValue().equals("location_id") || entry.getValue().equals("location_group_id");
    }

    /**
     * Verify that the correct values have been updated for a stop depending on the pattern type. If no results are
     * returned fail the check.
     */
    private void verifyStopTime(
        String tripId,
        String id,
        String columnName,
        int stopSequence,
        int start,
        int end
    ) throws SQLException {
        try (
            ResultSet stopTimesResultSet = connection.createStatement().executeQuery(
                String.format(
                    "select * from %s.%s where trip_id='%s' and %s='%s' and stop_sequence=%s",
                    testNamespace,
                    Table.STOP_TIMES.name,
                    tripId,
                    columnName,
                    id,
                    stopSequence
                )
            )
        ) {
            if (!stopTimesResultSet.isBeforeFirst()) {
                throw new SQLException(
                    String.format(
                        "No stop time matching trip_id: %s, %s: %s and stop_sequence: %s.",
                        tripId,
                        columnName,
                        id,
                        stopSequence
                    )
                );
            }
            while (stopTimesResultSet.next()) {
                if (columnName.equalsIgnoreCase("stop_id")) {
                    assertResultValue(stopTimesResultSet, "arrival_time", equalTo(start));
                    assertResultValue(stopTimesResultSet, "departure_time", equalTo(end));
                } else {
                    assertResultValue(stopTimesResultSet, "start_pickup_drop_off_window", equalTo(start));
                    assertResultValue(stopTimesResultSet, "end_pickup_drop_off_window", equalTo(end));
                }
            }
        }
    }

    /**
     * Create a new random unique id.
     */
    private static String newUUID() {
        return UUID.randomUUID().toString();
    }

    /**
     * Constructs an SQL query for the specified ID and columns and returns the resulting result set.
     */
    private String getColumnsForId(int id, Table table, String... columns) {
        String sql = String.format(
            "select %s from %s.%s where id=%d",
            columns.length > 0 ? String.join(", ", columns) : "*",
            testNamespace,
            table.name,
            id
        );
        LOG.info(sql);
        return sql;
    }

    /**
     * Executes SQL query for the specified ID and columns and returns the resulting result set.
     */
    private ResultSet getResultSetForId(int id, Table table, String... columns) throws SQLException {
        String sql = getColumnsForId(id, table, columns);
        return testDataSource.getConnection().prepareStatement(sql).executeQuery();
    }

    private void assertThatSqlQueryYieldsRowCount(String sql, int expectedRowCount) throws SQLException {
        LOG.info(sql);
        int recordCount = 0;
        ResultSet rs = testDataSource.getConnection().prepareStatement(sql).executeQuery();
        while (rs.next()) recordCount++;
        assertThat("Records matching query should equal expected count.", recordCount, equalTo(expectedRowCount));
    }

    void assertThatSqlQueryYieldsZeroRows(String sql) throws SQLException {
        assertThatSqlQueryYieldsRowCount(sql, 0);
    }

    /**
     * Asserts that a given value for the specified field in result set matches provided matcher.
     */
    public static void assertResultValue(ResultSet resultSet, String field, Matcher matcher) throws SQLException {
        assertThat(resultSet.getObject(field), matcher);
    }

    /**
     * Construct (without writing to the database) a timetable trip.
     */
    private static TripDTO constructTimetableTrip(
        String patternId,
        String routeId,
        int startTime,
        int travelTime
    ) {
        StopTimeWithFlexDTO[] stopTimes = new StopTimeWithFlexDTO[] {
            new StopTimeWithFlexDTO(JDBCTableWriterTest.FIRST_STOP_ID, startTime, startTime, 0),
            new StopTimeWithFlexDTO(
                JDBCTableWriterTest.LAST_STOP_ID,
                startTime + travelTime,
                startTime + travelTime,
                1
            )
        };
        return TripDTO.create(patternId, routeId, stopTimes);
    }

    /**
     * Creates a pattern by first creating a route and then a pattern for that route.
     */
    private static PatternDTO createRouteAndPattern(
        String routeId,
        String patternId,
        String name,
        String shapeId,
        ShapePointDTO[] shapes,
        PatternStopWithFlexDTO[] patternStops,
        int useFrequency
    ) throws InvalidNamespaceException, SQLException, IOException {
        // Create new route.
        saveRecord(Table.ROUTES, RouteDTO.create(routeId), RouteDTO.class);
        // Create new pattern for route.
        return saveRecord(Table.PATTERNS, PatternDTO.create(routeId, patternId, name, shapeId, shapes, patternStops, useFrequency), PatternDTO.class);
    }

    /**
     * Creates a pattern by first creating a route and then a pattern for that route.
     */
    private static PatternDTO createRouteAndSimplePattern(
        String routeId,
        String patternId,
        String name
    ) throws InvalidNamespaceException, SQLException, IOException {
        return createRouteAndPattern(routeId, patternId, name, null, new ShapePointDTO[] {}, new PatternStopWithFlexDTO[] {}, 0);
    }

    private static class PatternArguments {
        PatternStopWithFlexDTO[] patternStops;
        // stop id, location group id or location id and matching column. Items must be added in sequence order.
        ImmutableMap<String, String> referenceIdAndColumn;
        int expectedFlexDefaultTravelTime;
        int expectedFlexDefaultZoneTime;
        int expectedDefaultTravelTime;
        int expectedDefaultDwellTime;

        public PatternArguments(
            PatternStopWithFlexDTO[] patternStops,
            ImmutableMap<String, String> referenceIdAndColumn,
            int expectedFlexDefaultTravelTime,
            int expectedFlexDefaultZoneTime,
            int expectedDefaultTravelTime,
            int expectedDefaultDwellTime
        ) {
            this.patternStops = patternStops;
            this.referenceIdAndColumn = referenceIdAndColumn;
            this.expectedFlexDefaultTravelTime = expectedFlexDefaultTravelTime;
            this.expectedFlexDefaultZoneTime = expectedFlexDefaultZoneTime;
            this.expectedDefaultTravelTime = expectedDefaultTravelTime;
            this.expectedDefaultDwellTime = expectedDefaultDwellTime;
        }
    }

    private void deleteRecord(Table table, Integer id) throws InvalidNamespaceException, SQLException {
        JdbcTableWriter deleteTableWriter = createTestTableWriter(table);
        int deleteOutput = deleteTableWriter.delete(id, true);
        LOG.info("deleted {} records from {}", deleteOutput, table.name);
        assertThatSqlQueryYieldsZeroRows(getColumnsForId(id, table));
    }

    private static <T> T saveRecord(Table table, Object obj, Class<T> clazz) throws InvalidNamespaceException, IOException, SQLException {
        JdbcTableWriter createTableWriter = createTestTableWriter(table);
        return mapper.readValue(createTableWriter.create(mapper.writeValueAsString(obj), true), clazz);

    }

    private static <T> T saveRecord(Table table, String obj, Class<T> clazz) throws InvalidNamespaceException, IOException, SQLException {
        JdbcTableWriter createTableWriter = createTestTableWriter(table);
        return mapper.readValue(createTableWriter.create(obj, true), clazz);

    }

    private static <T> T updateRecord(Integer id, Table table, Object obj, Class<T> clazz) throws InvalidNamespaceException, IOException, SQLException {
        JdbcTableWriter updateTableWriter = createTestTableWriter(table);
        return mapper.readValue(updateTableWriter.update(id, mapper.writeValueAsString(obj), true), clazz);
    }

    private static <T> T updateRecord(Integer id, Table table, String obj, Class<T> clazz) throws InvalidNamespaceException, IOException, SQLException {
        JdbcTableWriter updateTableWriter = createTestTableWriter(table);
        return mapper.readValue(updateTableWriter.update(id, obj, true), clazz);
    }

}