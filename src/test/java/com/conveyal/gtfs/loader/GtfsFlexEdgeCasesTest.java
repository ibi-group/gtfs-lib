package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.GTFS;
import com.conveyal.gtfs.GTFSFeed;
import com.conveyal.gtfs.TestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.stream.Stream;

import static com.conveyal.gtfs.TestUtils.getResourceFileName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Edge-case tests for GTFS flex feeds.
 */
class GtfsFlexEdgeCasesTest {
    /**
     * Make sure that unexpected geo json values are handled gracefully.
     */
    @Test
    void canHandleUnexpectedGeoJsonValues() throws IOException {
        try (GTFSFeed feed = GTFSFeed.fromFile(TestUtils.zipFolderFiles("fake-agency-unexpected-geojson", true))) {
            assertEquals("loc_1", feed.locations.entrySet().iterator().next().getKey());
            assertEquals("Plymouth Metrolink", feed.locations.values().iterator().next().stop_name);
            assertEquals("743", feed.locations.values().iterator().next().zone_id);
            assertEquals("http://www.test.com", feed.locations.values().iterator().next().stop_url.toString());
            assertNull(feed.locations.values().iterator().next().stop_desc);
        }
    }

    @ParameterizedTest
    @MethodSource("createSpecialCaseFeeds")
    void canLoadSpecialCaseFeed(String fileName, int errorCount) {
        try (TestFeed testFeed = new TestFeed(fileName)) {
            FeedLoadResult loadResult = GTFS.load(testFeed.fileName, testFeed.dataSource);
            assertEquals(errorCount, loadResult.errorCount);
        }
    }

    private static Stream<Arguments> createSpecialCaseFeeds() throws IOException {
        return Stream.of(
            // Empty location groups
            Arguments.of(getResourceFileName("real-world-gtfs-feeds/bellhop-flex.zip"), 0),
            // Bad location group refs
            Arguments.of(getResourceFileName("real-world-gtfs-feeds/bellhop-flex-comma-content.zip"), 0),
            // Geojson locations,
            // Keep 2 errors regarding unsupported geometry types MULTIPOLYGON and MULTILINESTRING.
            Arguments.of(TestUtils.zipFolderFiles("fake-agency-with-flex", true), 2)
        );
    }
}
