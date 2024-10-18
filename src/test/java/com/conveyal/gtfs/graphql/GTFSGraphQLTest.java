package com.conveyal.gtfs.graphql;

import com.conveyal.gtfs.TestUtils;
import com.conveyal.gtfs.loader.FeedLoadResult;
import graphql.ExecutionInput;
import org.apache.commons.io.IOUtils;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.conveyal.gtfs.GTFS.load;
import static com.conveyal.gtfs.GTFS.validate;
import static com.conveyal.gtfs.TestUtils.getResourceFileName;
import static com.zenika.snapshotmatcher.SnapshotMatcher.matchesSnapshot;
import static org.junit.jupiter.api.Assertions.assertTimeout;


/**
 * A test suite for all things related to fetching objects with GraphQL.
 * Important note: Snapshot testing is heavily used in this test suite and can potentially result in false negatives
 *   in cases where the order of items in a list is not important.
 */
public class GTFSGraphQLTest {
    public static String testDBName;
    private static DataSource testDataSource;
    private static String testNamespace;

    private static String testInjectionDBName;
    private static DataSource testInjectionDataSource;
    private static String testInjectionNamespace;
    private static String badCalendarDateNamespace;

    public static String faresDBName;
    private static DataSource faresDataSource;
    private static String faresNamespace;

    private static final int TEST_TIMEOUT = 5000;

    @BeforeAll
    public static void setUpClass() throws IOException {
        // create a new database
        testDBName = TestUtils.generateNewDB();
        String dbConnectionUrl = String.format("jdbc:postgresql://localhost/%s", testDBName);
        testDataSource = TestUtils.createTestDataSource(dbConnectionUrl);
        // zip up test folder into temp zip file
        String zipFileName = TestUtils.zipFolderFiles("fake-agency", true);
        // load feed into db
        FeedLoadResult feedLoadResult = load(zipFileName, testDataSource);
        testNamespace = feedLoadResult.uniqueIdentifier;
        // validate feed to create additional tables
        validate(testNamespace, testDataSource);

        // Zip up test folder into temp zip file for loading into database.
        FeedLoadResult feed2LoadResult = load(
            TestUtils.zipFolderFiles("fake-agency-bad-calendar-date", true),
            testDataSource
        );
        badCalendarDateNamespace = feed2LoadResult.uniqueIdentifier;
        // validate feed to create additional tables
        validate(badCalendarDateNamespace, testDataSource);

        // create a separate injection database to use in injection tests
        // create a new database
        testInjectionDBName = TestUtils.generateNewDB();
        String injectionDbConnectionUrl = String.format("jdbc:postgresql://localhost/%s", testInjectionDBName);
        testInjectionDataSource = TestUtils.createTestDataSource(injectionDbConnectionUrl);
        // load feed into db
        FeedLoadResult injectionFeedLoadResult = load(zipFileName, testInjectionDataSource);
        testInjectionNamespace = injectionFeedLoadResult.uniqueIdentifier;
        // validate feed to create additional tables
        validate(testInjectionNamespace, testInjectionDataSource);

        String folderName = "fake-agency-with-fares-v2";
        String faresZipFileName = TestUtils.zipFolderFiles(folderName, true);
        // create a new database
        faresDBName = TestUtils.generateNewDB();
        String faresConnectionUrl = String.format("jdbc:postgresql://localhost/%s", faresDBName);
        faresDataSource = TestUtils.createTestDataSource(faresConnectionUrl);
        // load feed into db
        FeedLoadResult faresFeedLoadResult = load(faresZipFileName, faresDataSource);
        faresNamespace = faresFeedLoadResult.uniqueIdentifier;
        // validate feed to create additional tables
        validate(faresNamespace, faresDataSource);
    }

    @AfterAll
    public static void tearDownClass() {
        TestUtils.dropDB(testDBName);
        TestUtils.dropDB(testInjectionDBName);
        TestUtils.dropDB(faresDBName);
    }

    /** Tests that the graphQL schema can initialize. */
    @Test
    void canInitialize() {
        assertTimeout(Duration.ofMillis(TEST_TIMEOUT), () -> {
            GTFSGraphQL.initialize(testDataSource);
            GTFSGraphQL.getGraphQl();
        });
    }

    /** Tests that the root element of a feed can be fetched. */
    @Test
    void canFetchFeed() {
        assertTimeout(Duration.ofMillis(TEST_TIMEOUT), () -> {
            MatcherAssert.assertThat(queryGraphQL("feed.txt"), matchesSnapshot());
        });
    }

    /** Tests that the row counts of a feed can be fetched. */
    @Test
    void canFetchFeedRowCounts() {
        assertTimeout(Duration.ofMillis(TEST_TIMEOUT), () -> {
            MatcherAssert.assertThat(queryGraphQL("feedRowCounts.txt"), matchesSnapshot());
        });
    }

    /** Tests that the errors of a feed can be fetched. */
    @Test
    void canFetchErrors() {
        assertTimeout(Duration.ofMillis(TEST_TIMEOUT), () -> {
            MatcherAssert.assertThat(queryGraphQL("feedErrors.txt"), matchesSnapshot());
        });
    }

    /** Tests that the feed_info of a feed can be fetched. */
    @Test
    void canFetchFeedInfo() {
        assertTimeout(Duration.ofMillis(TEST_TIMEOUT), () -> {
            MatcherAssert.assertThat(queryGraphQL("feedFeedInfo.txt"), matchesSnapshot());
        });
    }

    /** Tests that the patterns of a feed can be fetched. */
    @Test
    void canFetchPatterns() {
        assertTimeout(Duration.ofMillis(TEST_TIMEOUT), () -> {
            MatcherAssert.assertThat(queryGraphQL("feedPatterns.txt"), matchesSnapshot());
        });
    }

    /** Tests that the poly lines of a feed can be fetched. */
    @Test
    void canFetchPolylines() {
        assertTimeout(Duration.ofMillis(TEST_TIMEOUT), () -> {
            MatcherAssert.assertThat(queryGraphQL("feedPolylines.txt"), matchesSnapshot());
        });
    }

    /** Tests that the agencies of a feed can be fetched. */
    @Test
    void canFetchAgencies() {
        assertTimeout(Duration.ofMillis(TEST_TIMEOUT), () -> {
            MatcherAssert.assertThat(queryGraphQL("feedAgencies.txt"), matchesSnapshot());
        });
    }

    /** Tests that the attributions of a feed can be fetched. */
    @Test
    void canFetchAttributions() {
        assertTimeout(Duration.ofMillis(TEST_TIMEOUT), () -> {
            MatcherAssert.assertThat(queryGraphQL("feedAttributions.txt"), matchesSnapshot());
        });
    }

    /** Tests that the calendars of a feed can be fetched. */
    @Test
    void canFetchCalendars() {
        assertTimeout(Duration.ofMillis(TEST_TIMEOUT), () -> {
            MatcherAssert.assertThat(queryGraphQL("feedCalendars.txt"), matchesSnapshot());
        });
    }

    /** Tests that the fares of a feed can be fetched. */
    @Test
    void canFetchFares() {
        assertTimeout(Duration.ofMillis(TEST_TIMEOUT), () -> {
            MatcherAssert.assertThat(queryGraphQL("feedFares.txt"), matchesSnapshot());
        });
    }

    /** Tests that the routes of a feed can be fetched. */
    @Test
    void canFetchRoutes() {
        assertTimeout(Duration.ofMillis(TEST_TIMEOUT), () -> {
            MatcherAssert.assertThat(queryGraphQL("feedRoutes.txt"), matchesSnapshot());
        });
    }

    /** Tests that the stops of a feed can be fetched. */
    @Test
    void canFetchStops() {
        assertTimeout(Duration.ofMillis(TEST_TIMEOUT), () -> {
            MatcherAssert.assertThat(queryGraphQL("feedStops.txt"), matchesSnapshot());
        });
    }

    /** Tests that stops with children can be fetched. */
    @Test
    void canFetchStopWithChildren() {
        assertTimeout(Duration.ofMillis(TEST_TIMEOUT), () -> {
            MatcherAssert.assertThat(queryGraphQL("feedStopWithChildren.txt"), matchesSnapshot());
        });
    }

    /** Tests that the trips of a feed can be fetched. */
    @Test
    void canFetchTrips() {
        assertTimeout(Duration.ofMillis(TEST_TIMEOUT), () -> {
            MatcherAssert.assertThat(queryGraphQL("feedTrips.txt"), matchesSnapshot());
        });
    }

    /** Tests that the translations of a feed can be fetched. */
    @Test
    void canFetchTranslations() {
        assertTimeout(Duration.ofMillis(TEST_TIMEOUT), () -> {
            MatcherAssert.assertThat(queryGraphQL("feedTranslations.txt"), matchesSnapshot());
        });
    }

    // TODO: make tests for schedule_exceptions / calendar_dates

    /** Tests that the stop times of a feed can be fetched. */
    @Test
    void canFetchStopTimes() {
        assertTimeout(Duration.ofMillis(TEST_TIMEOUT), () -> {
            MatcherAssert.assertThat(queryGraphQL("feedStopTimes.txt"), matchesSnapshot());
        });
    }

    /** Tests that the services of a feed can be fetched. */
    @Test
    void canFetchServices() {
        assertTimeout(Duration.ofMillis(TEST_TIMEOUT), () -> {
            MatcherAssert.assertThat(queryGraphQL("feedServices.txt"), matchesSnapshot());
        });
    }

    @Test
    void canFetchAreas() {
        assertTimeout(Duration.ofMillis(TEST_TIMEOUT), () -> {
            MatcherAssert.assertThat(queryFaresV2GraphQL("feedAreas.txt"), matchesSnapshot());
        });
    }

    @Test
    void canFetchStopAreas() {
        assertTimeout(Duration.ofMillis(TEST_TIMEOUT), () -> {
            MatcherAssert.assertThat(queryFaresV2GraphQL("feedStopAreas.txt"), matchesSnapshot());
        });
    }

    @Test
    void canFetchFareTransferRules() {
        assertTimeout(Duration.ofMillis(TEST_TIMEOUT), () -> {
            MatcherAssert.assertThat(queryFaresV2GraphQL("feedFareTransferRules.txt"), matchesSnapshot());
        });
    }

    @Test
    void canFetchFareProducts() {
        assertTimeout(Duration.ofMillis(TEST_TIMEOUT), () -> {
            MatcherAssert.assertThat(queryFaresV2GraphQL("feedFareProducts.txt"), matchesSnapshot());
        });
    }

    @Test
    void canFetchFareMedias() {
        assertTimeout(Duration.ofMillis(TEST_TIMEOUT), () -> {
            MatcherAssert.assertThat(queryFaresV2GraphQL("feedFareMedias.txt"), matchesSnapshot());
        });
    }

    @Test
    void canFetchFareLegRules() {
        assertTimeout(Duration.ofMillis(TEST_TIMEOUT), () -> {
            MatcherAssert.assertThat(queryFaresV2GraphQL("feedFareLegRules.txt"), matchesSnapshot());
        });
    }

    @Test
    void canFetchTimeFrames() {
        assertTimeout(Duration.ofMillis(TEST_TIMEOUT), () -> {
            MatcherAssert.assertThat(queryFaresV2GraphQL("feedTimeFrames.txt"), matchesSnapshot());
        });
    }

    @Test
    void canFetchNetworks() {
        assertTimeout(Duration.ofMillis(TEST_TIMEOUT), () -> {
            MatcherAssert.assertThat(queryFaresV2GraphQL("feedNetworks.txt"), matchesSnapshot());
        });
    }

    @Test
    void canFetchRouteNetworks() {
        assertTimeout(Duration.ofMillis(TEST_TIMEOUT), () -> {
            MatcherAssert.assertThat(queryFaresV2GraphQL("feedRouteNetworks.txt"), matchesSnapshot());
        });
    }

    @Test
    void canFetchRoutesAndFilterTripsByDateAndTime() {
        Map<String, Object> variables = new HashMap<>();
        variables.put("namespace", testNamespace);
        variables.put("date", "20170915");
        variables.put("from", 24000);
        variables.put("to", 28000);
        assertTimeout(Duration.ofMillis(TEST_TIMEOUT), () -> {
            MatcherAssert.assertThat(
                queryGraphQL("feedRoutesAndTripsByTime.txt", variables, testDataSource),
                matchesSnapshot()
            );
        });
    }

    /** Tests that the limit argument applies properly to a fetcher defined with auto limit set to false. */
    @Test
    void canFetchNestedEntityWithLimit() {
        assertTimeout(Duration.ofMillis(TEST_TIMEOUT), () -> {
            MatcherAssert.assertThat(queryGraphQL("feedStopsStopTimeLimit.txt"), matchesSnapshot());
        });
    }

    /** Tests whether a graphQL query that has superfluous and redundant nesting can find the right result. */
    // if the graphQL dataloader is enabled correctly, there will not be any repeating sql queries in the logs
    @Test
    void canFetchMultiNestedEntities() {
        assertTimeout(Duration.ofMillis(TEST_TIMEOUT), () -> {
            MatcherAssert.assertThat(queryGraphQL("superNested.txt"), matchesSnapshot());
        });
    }

    /**
     * Tests whether a graphQL query that has superfluous and redundant nesting can find the right result.
     * If the graphQL dataloader is enabled correctly, there will not be any repeating sql queries in the logs.
     * Furthermore, some queries should have been combined.
     */
    @Test
    void canFetchMultiNestedEntitiesWithoutLimits() {
        assertTimeout(Duration.ofMillis(TEST_TIMEOUT), () -> {
            MatcherAssert.assertThat(queryGraphQL("superNestedNoLimits.txt"), matchesSnapshot());
        });
    }

    /** Tests that a query for child stops does not throw an exception for a feed with no
     * parent_station column in the imported stops table.
     */
    @Test
    void canFetchStopsWithoutParentStationColumn() {
        Map<String, Object> variables = new HashMap<>();
        variables.put("namespace", badCalendarDateNamespace);
        assertTimeout(Duration.ofMillis(TEST_TIMEOUT), () -> {
            MatcherAssert.assertThat(
                queryGraphQL(
                    "feedStopWithChildren.txt",
                    variables,
                    testDataSource
                ),
                matchesSnapshot()
            );
        });
    }

    /**
     * Attempt to fetch more than one record with SQL injection as inputs.
     * The graphql library should properly escape the string and return 0 results for stops.
     */
    @Test
    void canSanitizeSQLInjectionSentAsInput() {
        Map<String, Object> variables = new HashMap<>();
        variables.put("namespace", testInjectionNamespace);
        variables.put("stop_id", Arrays.asList("' OR 1=1;"));
        assertTimeout(Duration.ofMillis(TEST_TIMEOUT), () -> {
            MatcherAssert.assertThat(
                queryGraphQL(
                    "feedStopsByStopId.txt",
                    variables,
                    testInjectionDataSource
                ),
                matchesSnapshot()
            );
        });
    }

    /**
     * Attempt to run a graphql query when one of the pieces of data contains a SQL injection.
     * The graphql library should properly escape the string and complete the queries.
     */
    @Test
    void canSanitizeSQLInjectionSentAsKeyValue() {
        assertTimeout(Duration.ofMillis(TEST_TIMEOUT), () -> {
            // manually update the route_id key in routes and patterns
            String injection = "'' OR 1=1; Select ''99";
            Connection connection = testInjectionDataSource.getConnection();
            List<String> tablesToUpdate = Arrays.asList("routes", "fare_rules", "patterns", "trips");
            for (String table : tablesToUpdate) {
                connection.createStatement()
                    .execute(String.format("update %s.%s set route_id = '%s'", testInjectionNamespace, table, injection));
            }
            connection.commit();

            // make graphql query
            Map<String, Object> variables = new HashMap<>();
            variables.put("namespace", testInjectionNamespace);
            MatcherAssert.assertThat(queryGraphQL("feedRoutes.txt", variables, testInjectionDataSource), matchesSnapshot());
        });
    }


    /**
     * Helper method to make a query with default variables.
     *
     * @param queryFilename the filename that should be used to generate the GraphQL query.  This file must be present
     *                      in the `src/test/resources/graphql` folder
     */
    private Map<String, Object> queryGraphQL(String queryFilename) throws IOException {
        Map<String, Object> variables = new HashMap<>();
        variables.put("namespace", testNamespace);
        return queryGraphQL(queryFilename, variables, testDataSource);
    }

    /**
     * Helper method to make a fares V2 query with default variables.
     *
     * @param queryFilename the filename that should be used to generate the GraphQL query.  This file must be present
     *                      in the `src/test/resources/graphql` folder
     */
    private Map<String, Object> queryFaresV2GraphQL(String queryFilename) throws IOException {
        Map<String, Object> variables = new HashMap<>();
        variables.put("namespace", faresNamespace);
        return queryGraphQL(queryFilename, variables, faresDataSource);
    }

    /**
     * Helper method to execute a GraphQL query and return the result.
     *
     * @param queryFilename the filename that should be used to generate the GraphQL query.  This file must be present
     *                      in the `src/test/resources/graphql` folder
     * @param variables a Map of input variables to the graphql query about to be executed
     * @param dataSource the datasource to use when initializing GraphQL
     */
    private Map<String, Object> queryGraphQL(
        String queryFilename,
        Map<String,Object> variables,
        DataSource dataSource
    ) throws IOException {
        GTFSGraphQL.initialize(dataSource);
        FileInputStream inputStream = new FileInputStream(
            getResourceFileName(String.format("graphql/%s", queryFilename))
        );
        ExecutionInput executionInput = ExecutionInput.newExecutionInput()
            .query(IOUtils.toString(inputStream))
            .variables(variables)
            .build();
        return GTFSGraphQL.getGraphQl().execute(executionInput).toSpecification();
    }
}
