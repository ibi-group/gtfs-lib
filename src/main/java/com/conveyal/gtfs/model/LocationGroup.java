package com.conveyal.gtfs.model;

import com.conveyal.gtfs.GTFSFeed;
import com.conveyal.gtfs.graphql.fetchers.MapFetcher;
import com.conveyal.gtfs.util.GeoJsonUtil;
import com.csvreader.CsvReader;
import graphql.schema.GraphQLObjectType;

import java.io.IOException;
import java.io.StringReader;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import static com.conveyal.gtfs.util.CsvReaderUtil.hasExpectedNumberOfColumns;
import static graphql.Scalars.GraphQLInt;
import static graphql.schema.GraphQLObjectType.newObject;

public class LocationGroup extends Entity {

    private static final long serialVersionUID = -7958476364523575940L;

    public String location_group_id;
    public String location_group_name;

    public static final String TABLE_NAME = "location_groups";
    public static final String LOCATION_GROUP_ID_NAME = "location_group_id";
    public static final String LOCATION_GROUP_NAME_NAME = "location_group_name";

    public static final int NUMBER_OF_HEADERS = 2;
    private static final int NUMBER_OF_COLUMNS = 2;
    private static final String CSV_HEADER = String.format("%s,%s%s", LOCATION_GROUP_ID_NAME, LOCATION_GROUP_NAME_NAME, System.lineSeparator());

    public LocationGroup() {
    }

    public LocationGroup(String locationGroupId, String locationGroupName) {
        this.location_group_id = locationGroupId;
        this.location_group_name = locationGroupName;
    }

    @Override
    public String getId () {
        return location_group_id;
    }

    /**
     * Sets the parameters for a prepared statement following the parameter order defined in
     * {@link com.conveyal.gtfs.loader.Table#LOCATION_GROUP}. JDBC prepared statement parameters use a one-based index.
     */
    @Override
    public void setStatementParameters(PreparedStatement statement, boolean setDefaultId) throws SQLException {
        int oneBasedIndex = 1;
        if (!setDefaultId) statement.setInt(oneBasedIndex++, id);
        statement.setString(oneBasedIndex++, location_group_id);
        statement.setString(oneBasedIndex, location_group_name);
    }

    public static class Loader extends Entity.Loader<LocationGroup> {

        public Loader(GTFSFeed feed) {
            super(feed, TABLE_NAME);
        }

        @Override
        protected boolean isRequired() {
            return false;
        }

        @Override
        public void loadOneRow() throws IOException {
            LocationGroup locationGroup = new LocationGroup();
            locationGroup.id = row + 1; // offset line number by 1 to account for 0-based row index
            locationGroup.location_group_id = getStringField(LOCATION_GROUP_ID_NAME, false);
            locationGroup.location_group_name = getStringField(LOCATION_GROUP_NAME_NAME, false);
            // Attempting to put a null key or value will cause an NPE in BTreeMap
            if (locationGroup.location_group_id != null) {
                feed.locationGroup.put(locationGroup.location_group_id, locationGroup);
            }
        }
    }

    public static class Writer extends Entity.Writer<LocationGroup> {
        public Writer(GTFSFeed feed) {
            super(feed, TABLE_NAME);
        }

        @Override
        public void writeHeaders() throws IOException {
            writer.writeRecord(new String[] {LOCATION_GROUP_ID_NAME, LOCATION_GROUP_NAME_NAME});
        }

        @Override
        public void writeOneRow(LocationGroup locationGroup) throws IOException {
            writeStringField(locationGroup.location_group_id);
            writeStringField(locationGroup.location_group_name);
            endRecord();
        }

        @Override
        public Iterator<LocationGroup> iterator() {
            return this.feed.locationGroup.values().iterator();
        }
    }

    /**
     * Extract all data from the original CSV, order by location group id and create a new {@link CsvReader} with the
     * data in the required order.
     */
    public static CsvReader getOrderedData(CsvReader csvReader, List<String> errors) {
        int locationGroupIdIndex = 0;
        int locationGroupNameIndex = 1;
        SortedMap<String, String> locationGroups = new TreeMap<>();
        try {
            while (csvReader.readRecord()) {
                if (!hasExpectedNumberOfColumns(csvReader, errors, NUMBER_OF_COLUMNS)) {
                    continue;
                }
                LocationGroup locationGroup = new LocationGroup(
                    csvReader.get(locationGroupIdIndex),
                    csvReader.get(locationGroupNameIndex)
                );
                locationGroups.put(locationGroup.location_group_id, locationGroup.location_group_name);
            }
            return (locationGroups.isEmpty())
                ? csvReader
                : produceCsvPayload(locationGroups);
        } catch (IOException e) {
            return csvReader;
        }
    }

    /**
     * Convert the multiple location group stops back into CSV, with header and return a {@link CsvReader} representation.
     */
    private static CsvReader produceCsvPayload(SortedMap<String, String> locationGroups) {
        StringBuilder csvContent = new StringBuilder();
        csvContent.append(CSV_HEADER);
        locationGroups.forEach((key, value) -> csvContent.append(GeoJsonUtil.createCSVRow(key, value)));
        return new CsvReader(new StringReader(csvContent.toString()));
    }

    public static final GraphQLObjectType locationGroupType = newObject().name(TABLE_NAME)
        .description("A GTFS location group object.")
        .field(MapFetcher.field("id", GraphQLInt))
        .field(MapFetcher.field(LOCATION_GROUP_ID_NAME))
        .field(MapFetcher.field(LOCATION_GROUP_NAME_NAME))
        .build();
}
