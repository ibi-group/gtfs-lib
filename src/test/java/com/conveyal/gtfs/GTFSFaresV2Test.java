package com.conveyal.gtfs;

import com.conveyal.gtfs.graphql.GTFSGraphQL;
import com.conveyal.gtfs.loader.FeedLoadResult;
import com.csvreader.CsvReader;
import graphql.ExecutionInput;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.BOMInputStream;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static com.conveyal.gtfs.GTFS.load;
import static com.conveyal.gtfs.GTFS.validate;
import static com.conveyal.gtfs.TestUtils.checkFileTestCases;
import static com.conveyal.gtfs.TestUtils.getResourceFileName;
import static com.zenika.snapshotmatcher.SnapshotMatcher.matchesSnapshot;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeout;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class GTFSFaresV2Test {

    private static String simpleGtfsZipFileName;
    public static String testDBName;
    private static DataSource testDataSource;
    private static String testNamespace;
    private static final int TEST_TIMEOUT = 5000;


    @BeforeAll
    public static void setUpClass() throws IOException {
        String folderName = "fake-agency-with-fares-v2";
        simpleGtfsZipFileName = TestUtils.zipFolderFiles(folderName, true);
        // create a new database
        testDBName = TestUtils.generateNewDB();
        String dbConnectionUrl = String.format("jdbc:postgresql://localhost/%s", testDBName);
        testDataSource = TestUtils.createTestDataSource(dbConnectionUrl);
        // zip up test folder into temp zip file
        String zipFileName = TestUtils.zipFolderFiles(folderName, true);
        // load feed into db
        FeedLoadResult feedLoadResult = load(zipFileName, testDataSource);
        testNamespace = feedLoadResult.uniqueIdentifier;
        // validate feed to create additional tables
        validate(testNamespace, testDataSource);
    }

    @AfterAll
    public static void tearDownClass() {
        TestUtils.dropDB(testDBName);
    }

    /** Tests that the graphQL schema can initialize. */
    @Test
    void canInitialize() {
        assertTimeout(Duration.ofMillis(TEST_TIMEOUT), () -> {
            GTFSGraphQL.initialize(testDataSource);
            GTFSGraphQL.getGraphQl();
        });
    }

    @Test
    void canFetchAreas() {
        assertTimeout(Duration.ofMillis(TEST_TIMEOUT), () -> {
            MatcherAssert.assertThat(queryGraphQL("feedAreas.txt"), matchesSnapshot());
        });
    }

    @Test
    void canFetchStopAreas() {
        assertTimeout(Duration.ofMillis(TEST_TIMEOUT), () -> {
            MatcherAssert.assertThat(queryGraphQL("feedStopAreas.txt"), matchesSnapshot());
        });
    }

    @Test
    void canFetchFareTransferRules() {
        assertTimeout(Duration.ofMillis(TEST_TIMEOUT), () -> {
            MatcherAssert.assertThat(queryGraphQL("feedFareTransferRules.txt"), matchesSnapshot());
        });
    }

    @Test
    void canFetchFareProducts() {
        assertTimeout(Duration.ofMillis(TEST_TIMEOUT), () -> {
            MatcherAssert.assertThat(queryGraphQL("feedFareProducts.txt"), matchesSnapshot());
        });
    }

    @Test
    void canFetchFareMedias() {
        assertTimeout(Duration.ofMillis(TEST_TIMEOUT), () -> {
            MatcherAssert.assertThat(queryGraphQL("feedFareMedias.txt"), matchesSnapshot());
        });
    }

    @Test
    void canFetchFareLegRules() {
        assertTimeout(Duration.ofMillis(TEST_TIMEOUT), () -> {
            MatcherAssert.assertThat(queryGraphQL("feedFareLegRules.txt"), matchesSnapshot());
        });
    }

    @Test
    void canFetchTimeFrames() {
        assertTimeout(Duration.ofMillis(TEST_TIMEOUT), () -> {
            MatcherAssert.assertThat(queryGraphQL("feedTimeFrames.txt"), matchesSnapshot());
        });
    }

    @Test
    void canFetchNetworks() {
        assertTimeout(Duration.ofMillis(TEST_TIMEOUT), () -> {
            MatcherAssert.assertThat(queryGraphQL("feedNetworks.txt"), matchesSnapshot());
        });
    }

    @Test
    void canFetchRouteNetworks() {
        assertTimeout(Duration.ofMillis(TEST_TIMEOUT), () -> {
            MatcherAssert.assertThat(queryGraphQL("feedRouteNetworks.txt"), matchesSnapshot());
        });
    }

    /**
     * Make sure a round-trip of loading fares v2 data and then writing this to another zip file can be performed.
     */
    @Test
    void canDoRoundTripLoadAndWriteToZipFile() throws IOException {
        // create a temp file for this test
        File outZip = File.createTempFile("fares-v2-output", ".zip");

        // delete file to make sure we can assert that this program created the file
        outZip.delete();

        GTFSFeed feed = GTFSFeed.fromFile(simpleGtfsZipFileName);
        feed.toFile(outZip.getAbsolutePath());
        feed.close();
        assertTrue(outZip.exists());

        // assert that rows of data were written to files within the zipfile
        ZipFile zip = new ZipFile(outZip);

        TestUtils.FileTestCase[] fileTestCases = {
            new TestUtils.FileTestCase(
                "areas.txt",
                new TestUtils.DataExpectation[] {
                    new TestUtils.DataExpectation("area_id", "area_bl"),
                    new TestUtils.DataExpectation("area_name", "Blue Line")
                }
            ),
            new TestUtils.FileTestCase(
                "fare_leg_rules.txt",
                new TestUtils.DataExpectation[] {
                    new TestUtils.DataExpectation("leg_group_id", "leg_airport_rapid_transit_quick_subway"),
                    new TestUtils.DataExpectation("network_id", "rapid_transit"),
                    new TestUtils.DataExpectation("from_area_id", "area_bl_airport")
                }
            ),
            new TestUtils.FileTestCase(
                "fare_media.txt",
                new TestUtils.DataExpectation[] {
                    new TestUtils.DataExpectation("fare_media_id", "cash"),
                    new TestUtils.DataExpectation("fare_media_name", "Cash"),
                    new TestUtils.DataExpectation("fare_media_type", "0")
                }
            ),
            new TestUtils.FileTestCase(
                "fare_products.txt",
                new TestUtils.DataExpectation[] {
                    new TestUtils.DataExpectation("fare_product_id", "prod_boat_zone_1"),
                    new TestUtils.DataExpectation("fare_product_name", "Ferry Zone 1 one-way fare"),
                    new TestUtils.DataExpectation("fare_media_id", "cash"),
                    new TestUtils.DataExpectation("amount", "6.5000000"),
                    new TestUtils.DataExpectation("currency", "USD")
                }
            ),
            new TestUtils.FileTestCase(
                "fare_transfer_rules.txt",
                new TestUtils.DataExpectation[] {
                    new TestUtils.DataExpectation("from_leg_group_id", "leg_airport_rapid_transit_quick_subway"),
                    new TestUtils.DataExpectation("to_leg_group_id", "leg_local_bus_quick_subway"),
                    new TestUtils.DataExpectation("transfer_count", ""),
                    new TestUtils.DataExpectation("duration_limit", "7200"),
                    new TestUtils.DataExpectation("duration_limit_type", "1"),
                    new TestUtils.DataExpectation("fare_transfer_type", "0"),
                    new TestUtils.DataExpectation("fare_product_id", "prod_rapid_transit_quick_subway")
                }
            ),
            new TestUtils.FileTestCase(
                "networks.txt",
                new TestUtils.DataExpectation[] {
                    new TestUtils.DataExpectation("network_id", "1"),
                    new TestUtils.DataExpectation("network_name", "Forbidden because network id is defined in routes")
                }
            ),
            new TestUtils.FileTestCase(
                "route_networks.txt",
                new TestUtils.DataExpectation[] {
                    new TestUtils.DataExpectation("network_id", "1"),
                    new TestUtils.DataExpectation("route_id", "1")
                }
            ),
            new TestUtils.FileTestCase(
                "stop_areas.txt",
                new TestUtils.DataExpectation[] {
                    new TestUtils.DataExpectation("stop_id", "4u6g"),
                    new TestUtils.DataExpectation("area_id", "area_route_426_downtown")
                }
            ),
            new TestUtils.FileTestCase(
                "timeframes.txt",
                new TestUtils.DataExpectation[] {
                    new TestUtils.DataExpectation("timeframe_group_id", "timeframe_sumner_tunnel_closure"),
                    new TestUtils.DataExpectation("start_time", "00:00:00"),
                    new TestUtils.DataExpectation("end_time", "02:30:00"),
                    new TestUtils.DataExpectation("service_id", "04100312-8fe1-46a5-a9f2-556f39478f57")
                }
            )
        };
        checkFileTestCases(zip, fileTestCases);
    }

    /**
     * Helper method to make a query with default variables.
     *
     * @param queryFilename the filename that should be used to generate the GraphQL query.  This file must be present
     *                      in the `src/test/resources/graphql` folder
     */
    private Map<String, Object> queryGraphQL(String queryFileName) throws IOException {
        Map<String, Object> variables = new HashMap<>();
        variables.put("namespace", testNamespace);
        return queryGraphQL(queryFileName, variables, testDataSource);
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
