package com.conveyal.gtfs;

import com.conveyal.gtfs.loader.FeedLoadResult;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.util.zip.ZipFile;

import static com.conveyal.gtfs.GTFS.load;
import static com.conveyal.gtfs.GTFS.makeSnapshot;
import static com.conveyal.gtfs.GTFS.validate;
import static com.conveyal.gtfs.TestUtils.checkFileTestCases;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class GTFSFaresV2Test {

    private static String faresZipFileName;
    public static String faresDBName;
    private static DataSource faresDataSource;
    private static String faresNamespace;

    @BeforeAll
    public static void setUpClass() throws IOException {
        String folderName = "fake-agency-with-fares-v2";
        faresZipFileName = TestUtils.zipFolderFiles(folderName, true);
        // create a new database
        faresDBName = TestUtils.generateNewDB();
        String dbConnectionUrl = String.format("jdbc:postgresql://localhost/%s", faresDBName);
        faresDataSource = TestUtils.createTestDataSource(dbConnectionUrl);
        // load feed into db
        FeedLoadResult feedLoadResult = load(faresZipFileName, faresDataSource);
        faresNamespace = feedLoadResult.uniqueIdentifier;
        // validate feed to create additional tables
        validate(faresNamespace, faresDataSource);
        // Create an empty snapshot to create a new namespace and all the tables
        FeedLoadResult result = makeSnapshot(null, faresDataSource, false);
        faresNamespace = result.uniqueIdentifier;
    }

    @AfterAll
    public static void tearDownClass() {
        TestUtils.dropDB(faresDBName);
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

        GTFSFeed feed = GTFSFeed.fromFile(faresZipFileName);
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
}