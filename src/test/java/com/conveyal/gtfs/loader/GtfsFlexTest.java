package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.GTFSFeed;
import com.conveyal.gtfs.TestUtils;
import com.conveyal.gtfs.TestUtils.DataExpectation;
import com.conveyal.gtfs.TestUtils.FileTestCase;
import com.conveyal.gtfs.util.GeoJsonUtil;
import mil.nga.sf.geojson.Feature;
import mil.nga.sf.geojson.FeatureCollection;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static com.conveyal.gtfs.GTFS.load;
import static com.conveyal.gtfs.GTFS.validate;
import static com.conveyal.gtfs.TestUtils.assertThatSqlCountQueryYieldsExpectedCount;
import static com.conveyal.gtfs.TestUtils.getResourceFileName;
import static com.conveyal.gtfs.TestUtils.loadFeedAndValidate;
import static com.conveyal.gtfs.TestUtils.lookThroughFiles;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Load in a GTFS feed with GTFS flex features, and ensure all needed fields are imported correctly.
 * TODO: update feed to use more features, and test for these.
 */
public class GtfsFlexTest {
    private static String islandTransitTestDBName;
    private static DataSource islandTransitTestDataSource;
    private static String islandTransitTestNamespace;
    private static String islandTransitGtfsZipFileName;
    private static String unexpectedGeoJsonZipFileName;

    @BeforeAll
    public static void setUpClass() throws IOException {
        islandTransitTestDBName = TestUtils.generateNewDB();
        islandTransitTestDataSource = TestUtils.createTestDataSource(String.format("jdbc:postgresql://localhost/%s", islandTransitTestDBName));
        islandTransitGtfsZipFileName = getResourceFileName("real-world-gtfs-feeds/islandtransit-wa-us--flex-v2.zip");
        FeedLoadResult feedLoadResult = load(islandTransitGtfsZipFileName, islandTransitTestDataSource);
        islandTransitTestNamespace = feedLoadResult.uniqueIdentifier;
        validate(islandTransitTestNamespace, islandTransitTestDataSource);
        unexpectedGeoJsonZipFileName = TestUtils.zipFolderFiles("fake-agency-unexpected-geojson", true);
    }

    @AfterAll
    public static void tearDownClass() {
        TestUtils.dropDB(islandTransitTestDBName);
    }

    @Test
    void hasLoadedExpectedNumberOfBookingRules() {
        String query = buildQuery(islandTransitTestNamespace, "booking_rules","booking_rule_id","booking_route_32584");
        assertThatSqlCountQueryYieldsExpectedCount(islandTransitTestDataSource, query, 1);
    }

    @Test
    void hasLoadedExpectedNumberOfStopTimes() {
        String query = buildQuery(islandTransitTestNamespace, "stop_times","pickup_booking_rule_id","booking_route_32584");
        assertThatSqlCountQueryYieldsExpectedCount(islandTransitTestDataSource, query, 16);
    }

    @Test
    void hasLoadedExpectedNumberOfLocationGroups() {
        String query = buildQuery(islandTransitTestNamespace, "location_groups","location_group_id","4209757");
        assertThatSqlCountQueryYieldsExpectedCount(islandTransitTestDataSource, query, 1);
    }

    @Test
    void hasLoadedExpectedNumberOfLocationGroupStops() {
        // There are 16 rows for location_group_id 4209757 in the location_group_stops.txt file. These are compress into
        // a single database entry, one location_group_id with many stop ids.
        String query = buildQuery(islandTransitTestNamespace, "location_group_stops","location_group_id","4209757");
        assertThatSqlCountQueryYieldsExpectedCount(islandTransitTestDataSource, query, 1);
    }

    @Test
    void hasLoadedExpectedNumberOfLocations() {
        String query = buildQuery(islandTransitTestNamespace, "locations","geometry_type","polygon");
        assertThatSqlCountQueryYieldsExpectedCount(islandTransitTestDataSource, query, 3);
    }

    @Test
    void hasLoadedExpectedNumberOfPatternStops() {
        String query = buildQuery(islandTransitTestNamespace, "pattern_stops","pattern_id","1");
        assertThatSqlCountQueryYieldsExpectedCount(islandTransitTestDataSource, query, 67);
    }

    @ParameterizedTest
    @MethodSource("createLocationShapeChecks")
    void hasLoadedExpectedNumberOfLocationShapes(String namespace, String field, String value, int expectedCount) {
        String query = String.format("select count(*) from %s.location_shapes where %s = '%s'",
            namespace,
            field,
            value);
        assertThatSqlCountQueryYieldsExpectedCount(islandTransitTestDataSource, query, expectedCount);
    }

    private static Stream<Arguments> createLocationShapeChecks() {
        return Stream.of(
            Arguments.of(islandTransitTestNamespace, "location_id", "area_1136", 365),
            Arguments.of(islandTransitTestNamespace, "location_id", "area_1137", 348)
        );
    }

    private String buildQuery(String namespace, String tableName, String columnName, String columnValue) {
        return String.format("select count(*) from %s.%s where %s = '%s'",
                namespace,
                tableName,
                columnName,
                columnValue);
    }

    /**
     * Make sure that unexpected geo json values are handled gracefully.
     */
    @Test
    void canHandleUnexpectedGeoJsonValues() {
        GTFSFeed feed = GTFSFeed.fromFile(unexpectedGeoJsonZipFileName);
        assertEquals("loc_1", feed.locations.entrySet().iterator().next().getKey());
        assertEquals("Plymouth Metrolink", feed.locations.values().iterator().next().stop_name);
        assertEquals("743", feed.locations.values().iterator().next().zone_id);
        assertEquals("http://www.test.com", feed.locations.values().iterator().next().stop_url.toString());
        assertNull(feed.locations.values().iterator().next().stop_desc);
    }

    /**
     * Make sure a round trip of loading a GTFS zip file and then writing another zip file can be performed with flex
     * data.
     */
    @Test
    void canLoadAndWriteToFlexContentZipFile() throws IOException {
        // create a temp file for this test
        File outZip = File.createTempFile("islandtransit-wa-us--flex-v2", ".zip");
        GTFSFeed feed = GTFSFeed.fromFile(islandTransitGtfsZipFileName);
        feed.toFile(outZip.getAbsolutePath());
        feed.close();
        assertThat(outZip.exists(), is(true));

        // assert that rows of data were written to files within the zipfile
        try (ZipFile zip = new ZipFile(outZip)) {
            ZipEntry entry = zip.getEntry("locations.geojson");
            FeatureCollection featureCollection = GeoJsonUtil.getFeatureCollection(zip, entry);
            List<Feature> features = featureCollection.getFeatures();
            assertEquals("area_1136", features.get(0).getId());
            assertEquals("area_1137", features.get(1).getId());
            assertEquals("area_548", features.get(2).getId());

            FileTestCase[] fileTestCases = {
                new FileTestCase(
                    "booking_rules.txt",
                    new DataExpectation[]{
                        new DataExpectation("booking_rule_id", "booking_route_32584"),
                        new DataExpectation("booking_type", "2"),
                        new DataExpectation("prior_notice_start_time", "08:00:00"),
                        new DataExpectation("prior_notice_last_time", "16:00:00")
                    }
                ),
                new TestUtils.FileTestCase(
                    "location_group_stops.txt",
                    new DataExpectation[]{
                        new DataExpectation("location_group_id", "4209757"),
                        new DataExpectation("stop_id", "3449688"),
                    }
                ),
                new TestUtils.FileTestCase(
                    "location_groups.txt",
                    new DataExpectation[]{
                        new DataExpectation("location_group_id", "4209758"),
                        new DataExpectation("location_group_name", "Island Transit GO! - NASWI Residential"),
                    }
                ),
                new TestUtils.FileTestCase(
                    "stop_times.txt",
                    new DataExpectation[]{
                        new DataExpectation("trip_id", "t_5736064_b_81516_tn_0"),
                        new DataExpectation("arrival_time", ""),
                        new DataExpectation("departure_time", ""),
                        new DataExpectation("stop_id", ""),
                        new DataExpectation("location_group_id", "4209757"),
                        new DataExpectation("location_id", ""),
                        new DataExpectation("stop_sequence", "1"),
                        new DataExpectation("stop_headsign", ""),
                        new DataExpectation("start_pickup_drop_off_window", "08:00:00"),
                        new DataExpectation("end_pickup_drop_off_window", "18:00:00"),
                        new DataExpectation("pickup_type", "2"),
                        new DataExpectation("drop_off_type", "1"),
                        new DataExpectation("continuous_pickup", "1"),
                        new DataExpectation("continuous_drop_off", "1"),
                        new DataExpectation("shape_dist_traveled", "0.0000000"),
                        new DataExpectation("timepoint", "0"),
                        new DataExpectation("pickup_booking_rule_id", "booking_route_76270"),
                        new DataExpectation("drop_off_booking_rule_id", "booking_route_76270"),
                    }
                )
            };
            lookThroughFiles(fileTestCases, zip);
        }
        // delete file to make sure we can assert that this program created the file
        outZip.delete();
    }
}
