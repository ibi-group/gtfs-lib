package com.conveyal.gtfs.model;

import com.conveyal.gtfs.GTFSFeed;
import com.csvreader.CsvReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.net.URL;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static com.conveyal.gtfs.util.CsvReaderUtil.hasExpectedNumberOfColumns;

public class Stop extends Entity {

    private static final Logger LOG = LoggerFactory.getLogger(Stop.class);

    private static final long serialVersionUID = 464065335273514677L;
    public String stop_id;
    public String stop_code;
    public String stop_name;
    public String stop_desc;
    public double stop_lat;
    public double stop_lon;
    public String zone_id;
    public URL stop_url;
    public int location_type;
    public String parent_station;
    public String stop_timezone;
    public int wheelchair_boarding;
    public String feed_id;
    public String platform_code;
    public String stop_area_ids;

    public static final String STOP_ID_FIELD = "stop_id";
    public static final String STOP_CODE_FIELD = "stop_code";
    public static final String STOP_NAME_FIELD = "stop_name";
    public static final String STOP_DESC_FIELD = "stop_desc";
    public static final String STOP_LAT_FIELD = "stop_lat";
    public static final String STOP_LON_FIELD = "stop_lon";
    public static final String ZONE_ID_FIELD = "zone_id";
    public static final String STOP_URL_FIELD = "stop_url";
    public static final String LOCATION_TYPE_FIELD = "location_type";
    public static final String PARENT_STATION_FIELD = "parent_station";
    public static final String STOP_TIMEZONE_FIELD = "stop_timezone";
    public static final String WHEELCHAIR_BOARDING_FIELD = "wheelchair_boarding";
    public static final String PLATFORM_CODE_FIELD = "platform_code";
    public static final String STOP_AREA_IDS_FIELD = "stop_area_ids";

    public static final String STOPS_FILE_NAME = "stops.txt";
    public static final String AREA_ID_FIELD = "area_id";
    public static final String STOP_AREAS_FILE_NAME = "stop_areas.txt";
    public static final int STOP_AREAS_NUMBER_OF_HEADERS = 2;
    private static final String[] CSV_HEADER_FOR_WRITE = new String[] {
        STOP_ID_FIELD,
        STOP_CODE_FIELD,
        STOP_NAME_FIELD,
        STOP_DESC_FIELD,
        STOP_LAT_FIELD,
        STOP_LON_FIELD,
        ZONE_ID_FIELD,
        STOP_URL_FIELD,
        LOCATION_TYPE_FIELD,
        PARENT_STATION_FIELD,
        STOP_TIMEZONE_FIELD,
        WHEELCHAIR_BOARDING_FIELD,
        PLATFORM_CODE_FIELD
    };
    private static final String CSV_HEADER_FOR_MERGE = String.format(
        "%s,%s%s",
        String.join(",", CSV_HEADER_FOR_WRITE),
        STOP_AREA_IDS_FIELD,
        System.lineSeparator()
    );

    // "§" (section sign, U+00A7)
    private static final String STOP_AREAS_SEPARATOR = "§";

    @Override
    public String getId () {
        return stop_id;
    }

    /**
     * Sets the parameters for a prepared statement following the parameter order defined in
     * {@link com.conveyal.gtfs.loader.Table#STOPS}. JDBC prepared statement parameters use a one-based index.
     */
    @Override
    public void setStatementParameters(PreparedStatement statement, boolean setDefaultId) throws SQLException {
        int oneBasedIndex = 1;
        if (!setDefaultId) statement.setInt(oneBasedIndex++, id);
        statement.setString(oneBasedIndex++, stop_id);
        statement.setString(oneBasedIndex++, stop_code);
        statement.setString(oneBasedIndex++, stop_name);
        statement.setString(oneBasedIndex++, stop_desc);
        statement.setDouble(oneBasedIndex++, stop_lat);
        statement.setDouble(oneBasedIndex++, stop_lon);
        statement.setString(oneBasedIndex++, zone_id);
        statement.setString(oneBasedIndex++, stop_url != null ? stop_url.toString() : null);
        setIntParameter(statement, oneBasedIndex++, location_type);
        statement.setString(oneBasedIndex++, parent_station);
        statement.setString(oneBasedIndex++, stop_timezone);
        setIntParameter(statement, oneBasedIndex++, wheelchair_boarding);
        statement.setString(oneBasedIndex++, platform_code);
        statement.setString(oneBasedIndex, stop_area_ids);
    }

    public static class Loader extends Entity.Loader<Stop> {

        public Loader(GTFSFeed feed) {
            super(feed, "stops");
        }

        @Override
        protected boolean isRequired() {
            return true;
        }

        @Override
        public void loadOneRow() throws IOException {
            Stop s = new Stop();
            s.id = row + 1; // offset line number by 1 to account for 0-based row index
            s.stop_id   = getStringField(STOP_ID_FIELD, true);
            s.stop_code = getStringField(STOP_CODE_FIELD, false);
            s.stop_name = getStringField(STOP_NAME_FIELD, true);
            s.stop_desc = getStringField(STOP_DESC_FIELD, false);
            s.stop_lat  = getDoubleField(STOP_LAT_FIELD, true, -90D, 90D);
            s.stop_lon  = getDoubleField(STOP_LON_FIELD, true, -180D, 180D);
            s.zone_id   = getStringField(ZONE_ID_FIELD, false);
            s.stop_url  = getUrlField(STOP_URL_FIELD, false);
            s.location_type  = getIntField(LOCATION_TYPE_FIELD, false, 0, 1);
            s.parent_station = getStringField(PARENT_STATION_FIELD, false);
            s.stop_timezone  = getStringField(STOP_TIMEZONE_FIELD, false);
            s.wheelchair_boarding = getIntField(WHEELCHAIR_BOARDING_FIELD, false, 0, 2);
            s.feed = feed;
            s.feed_id = feed.feedId;
            s.platform_code = getStringField(PLATFORM_CODE_FIELD, false);
            s.stop_area_ids = getStringField(STOP_AREA_IDS_FIELD, false);
            /* TODO check ref integrity later, this table self-references via parent_station */
            // Attempting to put a null key or value will cause an NPE in BTreeMap
            if (s.stop_id != null) feed.stops.put(s.stop_id, s);
        }

    }

    public static class Writer extends Entity.Writer<Stop> {
        public Writer (GTFSFeed feed) {
            super(feed, "stops");
        }

        @Override
        public void writeHeaders() throws IOException {
            writer.writeRecord(CSV_HEADER_FOR_WRITE);
        }

        @Override
        public void writeOneRow(Stop s) throws IOException {
            writeStringField(s.stop_id);
            writeStringField(s.stop_code);
            writeStringField(s.stop_name);
            writeStringField(s.stop_desc);
            writeDoubleField(s.stop_lat);
            writeDoubleField(s.stop_lon);
            writeStringField(s.zone_id);
            writeUrlField(s.stop_url);
            writeIntField(s.location_type);
            writeStringField(s.parent_station);
            writeStringField(s.stop_timezone);
            writeIntField(s.wheelchair_boarding);
            writeStringField(s.platform_code);
            endRecord();
        }

        @Override
        public Iterator<Stop> iterator() {
            return feed.stops.values().iterator();
        }   	
    }

    /**
     * Merge stop areas into stops when loading from file.
     */
    public static void mergeStopAreasIntoStops(Map<String, Stop> stops, Map<String, StopArea> stopAreas) {
        Map<String, Set<String>> stopAreasByStopId = new HashMap<>();

        stopAreas.values().forEach(stopArea ->
            stopAreasByStopId
                .computeIfAbsent(stopArea.stop_id, id -> new HashSet<>())
                .add(stopArea.area_id)
        );

        stops.values().forEach(stop -> stop.stop_area_ids = getStopAreaIds(stopAreasByStopId, stop.stop_id));
    }

    /**
     * Merge stop areas into stops when loading into DB.
     */
    public static CsvReader mergeStopAreasIntoStops(
        CsvReader stopsReader,
        Map<String, Set<String>> stopAreasByStopId
    ) {
        List<String> rows = new ArrayList<>();
        try {
            while (stopsReader.readRecord()) {
                String stopId = stopsReader.get(STOP_ID_FIELD);
                rows.add(createRow(stopsReader, stopId, getStopAreaIds(stopAreasByStopId, stopId)));
            }
            return (rows.isEmpty())
                ? stopsReader
                : produceCsvPayload(rows);
        } catch (Exception e) {
            LOG.error("Error while merging stops", e);
            // Any issues, return the original stops reader (minus stop areas).
            return stopsReader;
        }
    }

    /**
     * Get all stop areas matching provided stop id.
     */
    private static String getStopAreaIds(Map<String, Set<String>> stopAreasByStopId, String stopId) {
        return Optional.ofNullable(stopAreasByStopId.get(stopId))
            .map(areas -> String.join(";", areas))
            .orElse("");
    }

    /**
     * Create a CSV row of original stop fields plus the stop area ids.
     */
    private static String createRow(CsvReader stopsReader, String stopId, String stopAreaIds) throws IOException {
        return
            stopId + "," +
            stopsReader.get(STOP_CODE_FIELD) + "," +
            stopsReader.get(STOP_NAME_FIELD) + "," +
            stopsReader.get(STOP_DESC_FIELD) + "," +
            stopsReader.get(STOP_LAT_FIELD) + "," +
            stopsReader.get(STOP_LON_FIELD) + "," +
            stopsReader.get(ZONE_ID_FIELD) + "," +
            stopsReader.get(STOP_URL_FIELD) + "," +
            stopsReader.get(LOCATION_TYPE_FIELD) + "," +
            stopsReader.get(PARENT_STATION_FIELD) + "," +
            stopsReader.get(STOP_TIMEZONE_FIELD) + "," +
            stopsReader.get(WHEELCHAIR_BOARDING_FIELD) + "," +
            stopsReader.get(PLATFORM_CODE_FIELD) + "," +
            stopAreaIds;
    }

    /**
     * Convert the multiple stops (with stop areas) back into CSV, with header and return a {@link CsvReader}
     * representation.
     */
    private static CsvReader produceCsvPayload(List<String> rows) {
        StringBuilder csvContent = new StringBuilder();
        csvContent.append(CSV_HEADER_FOR_MERGE);
        rows.forEach(row -> csvContent.append(row).append(System.lineSeparator()));
        return new CsvReader(new StringReader(csvContent.toString()));
    }

    /**
     * Extract the stop areas from file and group by stop id. This is to allow for easier CRUD by the DT UI.
     */
    public static Map<String, Set<String>> groupStopAreaIds(CsvReader csvReader, List<String> errors) {
        Map<String, Set<String>> stopAreasGroupedByStopId = new HashMap<>();

        try {
            while (csvReader.readRecord()) {
                if (!hasExpectedNumberOfColumns(csvReader, errors, 2)) {
                    continue;
                }
                String stopAreaId = csvReader.get(AREA_ID_FIELD);
                String stopId = csvReader.get(STOP_ID_FIELD);
                stopAreasGroupedByStopId.computeIfAbsent(stopId, k -> new HashSet<>()).add(stopAreaId);
            }
            return stopAreasGroupedByStopId;
        } catch (IOException e) {
            return Collections.emptyMap();
        }
    }

    /**
     * Expand the stop area ids and write to zip file.
     */
    public static void writeStopAreasToFile(ZipOutputStream zipOutputStream, List<Stop> stops) throws IOException {
        // Create entry for table.
        zipOutputStream.putNextEntry(new ZipEntry(STOP_AREAS_FILE_NAME));
        // Create and use PrintWriter, but don't close. This is done when the zip entry is closed.
        PrintWriter p = new PrintWriter(zipOutputStream);
        p.print(packStopAreas(stops));
        p.flush();
        zipOutputStream.closeEntry();
    }

    /**
     * Expand all stop area ids into a single row for each area id. This is to conform with the GTFS Fares v2 standard.
     */
    public static String packStopAreas(List<Stop> stops) {
        StringBuilder csvContent = new StringBuilder(AREA_ID_FIELD)
            .append(",")
            .append(STOP_ID_FIELD)
            .append(System.lineSeparator());

        stops.stream()
            .filter(stop -> stop.stop_area_ids != null)
            .forEach(stop -> {
                String[] areaIds = stop.stop_area_ids.split(STOP_AREAS_SEPARATOR);
                for (String areaId : areaIds) {
                    csvContent
                        .append(areaId)
                        .append(",")
                        .append(stop.stop_id)
                        .append(System.lineSeparator());
                }
            });
        return csvContent.toString();
    }
}
