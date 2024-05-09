package com.conveyal.gtfs.model;

import com.conveyal.gtfs.GTFSFeed;
import com.csvreader.CsvReader;
import org.apache.commons.io.input.BOMInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class LocationGroupStop extends Entity {

    private static final Logger LOG = LoggerFactory.getLogger(LocationGroupStop.class);
    private static final long serialVersionUID = 469687473399554677L;
    private static final int NUMBER_OF_HEADERS = 2;
    private static final int NUMBER_OF_COLUMNS = 2;
    private static final String CSV_HEADER = "location_group_id,stop_id" + System.lineSeparator();

    public String location_group_id;
    /**
     * A comma separated list of ids referencing stops.stop_id or id from locations.geojson. These are grouped by
     * {@link LocationGroupStop#getCsvReader(ZipFile, ZipEntry, List)}.
     */
    public String stop_id;

    public LocationGroupStop() {
    }

    public LocationGroupStop(String areaId, String stopId) {
        this.location_group_id = areaId;
        this.stop_id = stopId;
    }

    @Override
    public String getId() {
        return createId(location_group_id, stop_id);
    }

    /**
     * Sets the parameters for a prepared statement following the parameter order defined in
     * {@link com.conveyal.gtfs.loader.Table#LOCATION_GROUP_STOPS}. JDBC prepared statement parameters use a one-based index.
     */
    @Override
    public void setStatementParameters(PreparedStatement statement, boolean setDefaultId) throws SQLException {
        int oneBasedIndex = 1;
        if (!setDefaultId) statement.setInt(oneBasedIndex++, id);
        statement.setString(oneBasedIndex++, location_group_id);
        statement.setString(oneBasedIndex, stop_id);
    }

    public static class Loader extends Entity.Loader<LocationGroupStop> {

        public Loader(GTFSFeed feed) {
            super(feed, "location_group_stops");
        }

        @Override
        protected boolean isRequired() {
            return false;
        }

        @Override
        public void loadOneRow() throws IOException {
            LocationGroupStop locationGroupStop = new LocationGroupStop();
            locationGroupStop.id = row + 1; // offset line number by 1 to account for 0-based row index
            locationGroupStop.location_group_id = getStringField("location_group_id", true);
            locationGroupStop.stop_id = getStringField("stop_id", true);
            // Attempting to put a null key or value will cause an NPE in BTreeMap
            if (locationGroupStop.location_group_id != null && locationGroupStop.stop_id != null) {
                feed.locationGroupStops.put(createId(locationGroupStop.location_group_id, locationGroupStop.stop_id), locationGroupStop);
            }
        }
    }

    public static class Writer extends Entity.Writer<LocationGroupStop> {
        public Writer(GTFSFeed feed) {
            super(feed, "location_group_stops");
        }

        @Override
        public void writeHeaders() throws IOException {
            writer.writeRecord(new String[] {"location_group_id", "stop_id"});
        }

        @Override
        public void writeOneRow(LocationGroupStop locationGroupStop) throws IOException {
            writeStringField(locationGroupStop.location_group_id);
            writeStringField(locationGroupStop.stop_id);
            endRecord();
        }

        @Override
        public Iterator<LocationGroupStop> iterator() {
            return this.feed.locationGroupStops.values().iterator();
        }
    }

    public String toCsvRow() {
        return String.join(
            ",",
            location_group_id,
            (stop_id != null)
                ? stop_id.contains(",") ? "\"" + stop_id + "\"" : stop_id
                : ""
        ) + System.lineSeparator();
    }

    /**
     * Extract the stop areas from file and group by stop id. Multiple rows of stop areas with the
     * same stop id will be compressed into a single row with comma separated stop ids. This is to allow
     * for easier CRUD by the DT UI.
     *
     * E.g. 1,2 and 1,3, will become: 1,"2,3".
     *
     * If there are any issues grouping the stop areas or there are no stop areas, return the default CSV
     * reader. This is to prevent downstream processing from failing where a CSV reader is expected.
     */
    public static CsvReader getCsvReader(ZipFile zipFile, ZipEntry entry, List<String> errors) {
        CsvReader csvReader = new CsvReader(new StringReader(""));
        int stopAreaIdIndex = 0;
        int stopIdIndex = 1;
        HashMap<String, LocationGroupStop> multiStopAreas = new HashMap<>();
        try {
            InputStream zipInputStream = zipFile.getInputStream(entry);
            csvReader = new CsvReader(new BOMInputStream(zipInputStream), ',', StandardCharsets.UTF_8);
            csvReader.setSkipEmptyRecords(false);
            csvReader.readHeaders();
            String[] headers = csvReader.getHeaders();
            if (headers.length != NUMBER_OF_HEADERS) {
                String message = String.format(
                    "Wrong number of headers, expected=%d; found=%d in %s.",
                    NUMBER_OF_HEADERS,
                    headers.length,
                    entry.getName()
                );
                LOG.warn(message);
                if (errors != null) errors.add(message);
                return csvReader;
            }
            while (csvReader.readRecord()) {
                int lineNumber = ((int) csvReader.getCurrentRecord()) + 2;
                if (csvReader.getColumnCount() != NUMBER_OF_COLUMNS) {
                    String message = String.format("Wrong number of columns for line number=%d; expected=%d; found=%d.",
                        lineNumber,
                        NUMBER_OF_COLUMNS,
                        csvReader.getColumnCount()
                    );
                    LOG.warn(message);
                    if (errors != null) errors.add(message);
                    continue;
                }
                LocationGroupStop locationGroupStop = new LocationGroupStop(
                    csvReader.get(stopAreaIdIndex),
                    csvReader.get(stopIdIndex)
                );
                if (multiStopAreas.containsKey(locationGroupStop.location_group_id)) {
                    // Combine stop areas with matching stop areas ids.
                    LocationGroupStop multiLocationGroupStop = multiStopAreas.get(locationGroupStop.location_group_id);
                    multiLocationGroupStop.stop_id += "," + locationGroupStop.stop_id;
                } else {
                    multiStopAreas.put(locationGroupStop.location_group_id, locationGroupStop);
                }
            }
        } catch (IOException e) {
            return csvReader;
        }
        return (multiStopAreas.isEmpty())
            ? csvReader
            : produceCsvPayload(multiStopAreas);
    }

    /**
     * Convert the multiple stop areas back into CSV, with header and return a {@link CsvReader} representation.
     */
    private static CsvReader produceCsvPayload(HashMap<String, LocationGroupStop> multiStopAreaIds) {
        StringBuilder csvContent = new StringBuilder();
        csvContent.append(CSV_HEADER);
        multiStopAreaIds.forEach((key, value) -> csvContent.append(value.toCsvRow()));
        return new CsvReader(new StringReader(csvContent.toString()));
    }

    /**
     * Expand all location group stops which have multiple stop ids into a single row for each stop id. This is to
     * conform with the GTFS Flex standard.
     *
     * E.g.
     *
     * location_group_1,"stop_id_2,stop_id_3"
     *
     * will become:
     *
     * location_group_1,stop_id_2
     * location_group_1,stop_id_3
     *
     */
    public static String packLocationGroupStops(List<LocationGroupStop> locationGroupStops) {
        StringBuilder csvContent = new StringBuilder();
        csvContent.append(CSV_HEADER);
        locationGroupStops.forEach(locationGroupStop -> {
            if (locationGroupStop.stop_id == null || !locationGroupStop.stop_id.contains(",")) {
                // Single location id reference.
                csvContent.append(locationGroupStop.toCsvRow());
            } else {
                for (String stopId : locationGroupStop.stop_id.split(",")) {
                    csvContent.append(String.join(
                        ",",
                        locationGroupStop.location_group_id,
                        stopId
                    ));
                    csvContent.append(System.lineSeparator());
                }
            }
        });
        return csvContent.toString();
    }

    private static String createId(String areaId, String stopId) {
        return String.format("%s_%s", areaId, stopId);
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LocationGroupStop that = (LocationGroupStop) o;
        return Objects.equals(location_group_id, that.location_group_id) &&
            Objects.equals(stop_id, that.stop_id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(location_group_id, stop_id);
    }
}

