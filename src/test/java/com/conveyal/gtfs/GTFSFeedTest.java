package com.conveyal.gtfs;

import com.conveyal.gtfs.model.StopTime;
import org.hamcrest.comparator.ComparatorMatcherBuilder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.zip.ZipFile;

import static com.conveyal.gtfs.TestUtils.checkFileTestCases;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.number.IsCloseTo.closeTo;

/**
 * Test suite for the GTFSFeed class.
 */
public class GTFSFeedTest {
    private static String simpleGtfsZipFileName;

    @BeforeAll
    public static void setUpClass() {
        //executed only once, before the first test
        simpleGtfsZipFileName = null;
        try {
            simpleGtfsZipFileName = TestUtils.zipFolderFiles("fake-agency", true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Make sure a round-trip of loading a GTFS zip file and then writing another zip file can be performed.
     */
    @Test
    void canDoRoundTripLoadAndWriteToZipFile() throws IOException {
        // create a temp file for this test
        File outZip = File.createTempFile("fake-agency-output", ".zip");

        // delete file to make sure we can assert that this program created the file
        outZip.delete();

        GTFSFeed feed = GTFSFeed.fromFile(simpleGtfsZipFileName);
        feed.toFile(outZip.getAbsolutePath());
        feed.close();
        assertThat(outZip.exists(), is(true));

        // assert that rows of data were written to files within the zipfile
        ZipFile zip = new ZipFile(outZip);

        TestUtils.FileTestCase[] fileTestCases = {
            // agency.txt
            new TestUtils.FileTestCase(
                "agency.txt",
                new TestUtils.DataExpectation[] {
                    new TestUtils.DataExpectation("agency_id", "1"),
                    new TestUtils.DataExpectation("agency_name", "Fake Transit")
                }
            ),
            new TestUtils.FileTestCase(
                "calendar.txt",
                new TestUtils.DataExpectation[] {
                    new TestUtils.DataExpectation("service_id", "04100312-8fe1-46a5-a9f2-556f39478f57"),
                    new TestUtils.DataExpectation("start_date", "20170915"),
                    new TestUtils.DataExpectation("end_date", "20170917")
                }
            ),
            new TestUtils.FileTestCase(
                "calendar_dates.txt",
                new TestUtils.DataExpectation[] {
                    new TestUtils.DataExpectation("service_id", "calendar-date-service"),
                    new TestUtils.DataExpectation("date", "20170917"),
                    new TestUtils.DataExpectation("exception_type", "1")
                }
            ),
            new TestUtils.FileTestCase(
                "routes.txt",
                new TestUtils.DataExpectation[] {
                    new TestUtils.DataExpectation("agency_id", "1"),
                    new TestUtils.DataExpectation("route_id", "1"),
                    new TestUtils.DataExpectation("route_long_name", "Route 1")
                }
            ),
            new TestUtils.FileTestCase(
                "shapes.txt",
                new TestUtils.DataExpectation[] {
                    new TestUtils.DataExpectation("shape_id", "5820f377-f947-4728-ac29-ac0102cbc34e"),
                    new TestUtils.DataExpectation("shape_pt_lat", "37.0612132"),
                    new TestUtils.DataExpectation("shape_pt_lon", "-122.0074332")
                }
            ),
            new TestUtils.FileTestCase(
                "stop_times.txt",
                new TestUtils.DataExpectation[] {
                    new TestUtils.DataExpectation("trip_id", "a30277f8-e50a-4a85-9141-b1e0da9d429d"),
                    new TestUtils.DataExpectation("departure_time", "07:00:00"),
                    new TestUtils.DataExpectation("stop_id", "4u6g")
                }
            ),
            new TestUtils.FileTestCase(
                "trips.txt",
                new TestUtils.DataExpectation[] {
                    new TestUtils.DataExpectation("route_id", "1"),
                    new TestUtils.DataExpectation("trip_id", "a30277f8-e50a-4a85-9141-b1e0da9d429d"),
                    new TestUtils.DataExpectation("service_id", "04100312-8fe1-46a5-a9f2-556f39478f57")
                }
            ),
            new TestUtils.FileTestCase(
                "datatools_patterns.txt",
                new TestUtils.DataExpectation[] {
                    new TestUtils.DataExpectation("pattern_id", "1"),
                    new TestUtils.DataExpectation("route_id", "1"),
                    new TestUtils.DataExpectation("name", "2 stops from Butler Ln to Scotts Valley Dr & Victor Sq (1 trips)"),
                    new TestUtils.DataExpectation("direction_id", "0"),
                    new TestUtils.DataExpectation("shape_id", "5820f377-f947-4728-ac29-ac0102cbc34e")
                }
            )
        };
        checkFileTestCases(zip, fileTestCases);
    }

    /**
     * Make sure that a GTFS feed with interpolated stop times have calculated times after feed processing
     *
     * @throws GTFSFeed.FirstAndLastStopsDoNotHaveTimes
     */
    @Test
    void canGetInterpolatedTimes() throws GTFSFeed.FirstAndLastStopsDoNotHaveTimes, IOException {
        String tripId = "a30277f8-e50a-4a85-9141-b1e0da9d429d";

        String gtfsZipFileName = TestUtils.zipFolderFiles("fake-agency-interpolated-stop-times", true);

        GTFSFeed feed = GTFSFeed.fromFile(gtfsZipFileName);
        Iterable<StopTime> stopTimes = feed.getInterpolatedStopTimesForTrip(tripId);


        int i = 0;
        int lastStopSequence = -1;
        int lastDepartureTime = -1;
        for (StopTime st : stopTimes) {
            // assert that all stop times belong to same trip
            assertThat(st.trip_id, equalTo(tripId));

            // assert that stops in trip alternate
            if (i % 2 == 0) {
                assertThat(st.stop_id, equalTo("4u6g"));
            } else {
                assertThat(st.stop_id, equalTo("johv"));
            }

            // assert that sequence increases
            assertThat(
                st.stop_sequence,
                ComparatorMatcherBuilder.<Integer>usingNaturalOrdering().greaterThan(lastStopSequence)
            );
            lastStopSequence = st.stop_sequence;

            // assert that arrival and departure times are greater than the last ones
            assertThat(
                st.arrival_time,
                ComparatorMatcherBuilder.<Integer>usingNaturalOrdering().greaterThan(lastDepartureTime)
            );
            assertThat(
                st.departure_time,
                ComparatorMatcherBuilder.<Integer>usingNaturalOrdering().greaterThanOrEqualTo(st.arrival_time)
            );
            lastDepartureTime = st.departure_time;

            i++;
        }
    }

    /**
     * Make sure a spatial index of stops can be calculated
     */
    @Test
    void canGetSpatialIndex() {
        GTFSFeed feed = GTFSFeed.fromFile(simpleGtfsZipFileName);
        assertThat(
            feed.getSpatialIndex().size(),
            // This should reflect the number of stops in src/test/resources/fake-agency/stops.txt
            equalTo(5)
        );
    }

    /**
     * Make sure trip speed can be calculated using trip's shape.
     */
    @Test
    void canGetTripSpeedUsingShape() {
        GTFSFeed feed = GTFSFeed.fromFile(simpleGtfsZipFileName);
        assertThat(
            feed.getTripSpeed("a30277f8-e50a-4a85-9141-b1e0da9d429d"),
            is(closeTo(5.96, 0.01))
        );
    }

    /**
     * Make sure trip speed can be calculated using trip's shape.
     */
    @Test
    void canGetTripSpeedUsingStraightLine() {
        GTFSFeed feed = GTFSFeed.fromFile(simpleGtfsZipFileName);
        assertThat(
            feed.getTripSpeed("a30277f8-e50a-4a85-9141-b1e0da9d429d", true),
            is(closeTo(5.18, 0.01))
        );
    }
}
