package com.conveyal.gtfs;

import com.conveyal.gtfs.loader.FeedLoadResult;
import com.conveyal.gtfs.model.Area;
import com.conveyal.gtfs.model.FareLegJoinRule;
import com.conveyal.gtfs.model.FareLegRule;
import com.conveyal.gtfs.model.FareMedia;
import com.conveyal.gtfs.model.FareProduct;
import com.conveyal.gtfs.model.FareTransferRule;
import com.conveyal.gtfs.model.Network;
import com.conveyal.gtfs.model.RiderCategory;
import com.conveyal.gtfs.model.RouteNetwork;
import com.conveyal.gtfs.model.StopArea;
import com.conveyal.gtfs.model.TimeFrame;
import graphql.Assert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.util.stream.Stream;
import java.util.zip.ZipFile;

import static com.conveyal.gtfs.GTFS.load;
import static com.conveyal.gtfs.GTFS.makeSnapshot;
import static com.conveyal.gtfs.GTFS.validate;
import static com.conveyal.gtfs.TestUtils.JDBC_URL;
import static com.conveyal.gtfs.TestUtils.checkFileTestCase;
import static com.conveyal.gtfs.model.RouteNetwork.ROUTE_NETWORK_FILE_NAME;
import static com.conveyal.gtfs.model.Stop.STOP_AREAS_FILE_NAME;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class GTFSFaresV2Test {

    private static final Logger LOG = LoggerFactory.getLogger(GTFSFaresV2Test.class);

    private static String faresZipFileName;
    public static String faresDBName;
    private static DataSource faresDataSource;
    private static String faresNamespace;

    @BeforeAll
    static void setUp() throws IOException {
        String folderName = "fake-agency-with-fares-v2";
        faresZipFileName = TestUtils.zipFolderFiles(folderName, true);
        faresDBName = TestUtils.generateNewDB();
        faresDataSource = TestUtils.createTestDataSource(String.format("%s/%s", JDBC_URL, faresDBName));
        // load feed into db
        FeedLoadResult feedLoadResult = load(faresZipFileName, faresDataSource);
        faresNamespace = feedLoadResult.uniqueIdentifier;
        // validate feed to create additional tables
        validate(faresNamespace, faresDataSource);
        // Create an empty snapshot to create a new namespace, all the tables and data.
        FeedLoadResult result = makeSnapshot(faresNamespace, faresDataSource, false);
        faresNamespace = result.uniqueIdentifier;
    }

    @AfterAll
    static void tearDown() {
        TestUtils.dropDB(faresDBName);
    }

    private static Stream<Arguments> createFileTestCases() throws IOException {
        // create a temp file for this test
        File outZip = File.createTempFile("fares-v2-output", ".zip");

        // delete file to make sure we can assert that this program created the file
        outZip.delete();

        GTFSFeed feed = GTFSFeed.fromFile(faresZipFileName);
        feed.toFile(outZip.getAbsolutePath());
        feed.close();
        assertTrue(outZip.exists());
        ZipFile zip = new ZipFile(outZip);

        return Stream.of(
            Arguments.of(
                zip,
                new TestUtils.FileTestCase(
                    "areas.txt",
                    new TestUtils.DataExpectation[] {
                        new TestUtils.DataExpectation(Area.AREA_ID_NAME, "area_bl"),
                        new TestUtils.DataExpectation(Area.AREA_NAME_NAME, "Blue Line")
                    }
                )
            ),
            Arguments.of(
                zip,
                new TestUtils.FileTestCase(
                "fare_leg_rules.txt",
                    new TestUtils.DataExpectation[] {
                        new TestUtils.DataExpectation(FareLegRule.LEG_GROUP_ID_NAME, "leg_airport_rapid_transit_quick_subway"),
                        new TestUtils.DataExpectation(FareLegRule.NETWORK_ID_NAME, "rapid_transit"),
                        new TestUtils.DataExpectation(FareLegRule.FROM_AREA_ID_NAME, "area_bl_airport")
                    }
                )
            ),
            Arguments.of(
                zip,
                new TestUtils.FileTestCase(
                    "fare_leg_join_rules.txt",
                    new TestUtils.DataExpectation[] {
                        new TestUtils.DataExpectation(FareLegJoinRule.FROM_NETWORK_ID_NAME, "1"),
                        new TestUtils.DataExpectation(FareLegJoinRule.TO_NETWORK_ID_NAME, "2"),
                        new TestUtils.DataExpectation(FareLegJoinRule.FROM_STOP_ID_NAME, "3"),
                        new TestUtils.DataExpectation(FareLegJoinRule.TO_STOP_ID_NAME, "4")
                    }
                )
            ),
            Arguments.of(
                zip,
                new TestUtils.FileTestCase(
                    "fare_media.txt",
                    new TestUtils.DataExpectation[] {
                        new TestUtils.DataExpectation(FareMedia.FARE_MEDIA_ID_NAME, "cash"),
                        new TestUtils.DataExpectation(FareMedia.FARE_MEDIA_NAME_NAME, "Cash"),
                        new TestUtils.DataExpectation(FareMedia.FARE_MEDIA_TYPE_NAME, "0")
                    }
                )
            ),
            Arguments.of(
                zip,
                new TestUtils.FileTestCase(
                    "fare_products.txt",
                    new TestUtils.DataExpectation[] {
                        new TestUtils.DataExpectation(FareProduct.FARE_PRODUCT_ID_NAME, "prod_boat_zone_1"),
                        new TestUtils.DataExpectation(FareProduct.FARE_PRODUCT_NAME_NAME, "Ferry Zone 1 one-way fare"),
                        new TestUtils.DataExpectation(FareProduct.RIDER_CATEGORY_ID_NAME, "HONORED_CITIZEN"),
                        new TestUtils.DataExpectation(FareProduct.FARE_MEDIA_ID_NAME, "cash"),
                        new TestUtils.DataExpectation(FareProduct.AMOUNT_NAME, "6.5000000"),
                        new TestUtils.DataExpectation(FareProduct.CURRENCY_NAME, "USD")
                    }
                )
            ),
            Arguments.of(
                zip,
                new TestUtils.FileTestCase(
                    "fare_transfer_rules.txt",
                    new TestUtils.DataExpectation[] {
                        new TestUtils.DataExpectation(FareTransferRule.FROM_LEG_GROUP_ID_NAME, "leg_airport_rapid_transit_quick_subway"),
                        new TestUtils.DataExpectation(FareTransferRule.TO_LEG_GROUP_ID_NAME, "leg_local_bus_quick_subway"),
                        new TestUtils.DataExpectation(FareTransferRule.TRANSFER_COUNT_NAME, ""),
                        new TestUtils.DataExpectation(FareTransferRule.DURATION_LIMIT_NAME, "7200"),
                        new TestUtils.DataExpectation(FareTransferRule.DURATION_LIMIT_TYPE_NAME, "1"),
                        new TestUtils.DataExpectation(FareTransferRule.FARE_TRANSFER_TYPE_NAME, "0"),
                        new TestUtils.DataExpectation(FareTransferRule.FARE_PRODUCT_ID_NAME, "prod_rapid_transit_quick_subway")
                    }
                )
            ),
            Arguments.of(
                zip,
                new TestUtils.FileTestCase(
                    "networks.txt",
                    new TestUtils.DataExpectation[] {
                        new TestUtils.DataExpectation(Network.NETWORK_ID_NAME, "1"),
                        new TestUtils.DataExpectation(Network.NETWORK_NAME_NAME, "Forbidden because network id is defined in routes")
                    }
                )
            ),
            Arguments.of(
                zip,
                new TestUtils.FileTestCase(
                    "route_networks.txt",
                    new TestUtils.DataExpectation[] {
                        new TestUtils.DataExpectation(RouteNetwork.NETWORK_ID_NAME, "1"),
                        new TestUtils.DataExpectation(RouteNetwork.ROUTE_ID_NAME, "1")
                    }
                )
            ),
            Arguments.of(
                zip,
                new TestUtils.FileTestCase(
                    "stop_areas.txt",
                    new TestUtils.DataExpectation[] {
                        new TestUtils.DataExpectation(StopArea.STOP_ID_NAME, "4u6g"),
                        new TestUtils.DataExpectation(StopArea.AREA_ID_NAME, "area_route_426_downtown")
                    }
                )
            ),
            Arguments.of(
                zip,
                new TestUtils.FileTestCase(
                    "timeframes.txt",
                    new TestUtils.DataExpectation[] {
                        new TestUtils.DataExpectation(TimeFrame.TIME_FRAME_GROUP_ID_NAME, "timeframe_sumner_tunnel_closure"),
                        new TestUtils.DataExpectation(TimeFrame.START_TIME_NAME, "00:00:00"),
                        new TestUtils.DataExpectation(TimeFrame.END_TIME_NAME, "02:30:00"),
                        new TestUtils.DataExpectation(TimeFrame.SERVICE_ID_NAME, "04100312-8fe1-46a5-a9f2-556f39478f57")
                    }
                )
            ),
            Arguments.of(
                zip,
                new TestUtils.FileTestCase(
                    "rider_categories.txt",
                    new TestUtils.DataExpectation[] {
                        new TestUtils.DataExpectation(RiderCategory.RIDER_CATEGORY_ID_NAME, "HONORED_CITIZEN"),
                        new TestUtils.DataExpectation(RiderCategory.RIDER_CATEGORY_NAME_NAME, "Honored Citizen"),
                        new TestUtils.DataExpectation(RiderCategory.RIDER_CATEGORY_ELIGIBILITY_URL_NAME, "https://trimet.org/fares/honoredcitizen.htm"),
                        new TestUtils.DataExpectation(RiderCategory.RIDER_CATEGORY_IS_DEFAULT_FARE_CATEGORY_NAME, "0")
                    }
                )
            )
        );
    }

    /**
     * Make sure a round-trip of loading fares v2 data and then writing this to another zip file can be performed.
     */
    @ParameterizedTest(name = "{1}")
    @MethodSource("createFileTestCases")
    void canDoRoundTripLoadAndWriteToZipFile(ZipFile zip, TestUtils.FileTestCase fileTestCase) throws IOException {
        checkFileTestCase(zip, fileTestCase);
    }

    /**
     * Confirm that the export contains the fares v2 files which have been merged into parent entities and then
     * extracted for writing to file.
     */
    @Test
    void canExportFaresV2Files() throws IOException {
        File tempFile = TestUtils.exportGtfs(faresNamespace, faresDataSource, false, true);
        try (ZipFile gtfsZipFile = new ZipFile(tempFile.getAbsolutePath())) {
            tempFile = TestUtils.exportGtfs(faresNamespace, faresDataSource, false, true);
            Stream
                .of(STOP_AREAS_FILE_NAME, ROUTE_NETWORK_FILE_NAME)
                .forEach(fileName -> Assert.assertNotNull(gtfsZipFile.getEntry(fileName)));
        } catch (IOException e) {
            Assert.assertShouldNeverHappen();
            LOG.error("An error occurred while attempting to test exporting of mandatory files.", e);
        } finally {
            tempFile.deleteOnExit();
        }
    }
}