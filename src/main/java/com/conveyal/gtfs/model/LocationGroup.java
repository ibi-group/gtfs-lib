package com.conveyal.gtfs.model;

import com.conveyal.gtfs.GTFSFeed;
import com.conveyal.gtfs.graphql.fetchers.MapFetcher;
import graphql.schema.GraphQLObjectType;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;

import static graphql.Scalars.GraphQLInt;
import static graphql.schema.GraphQLObjectType.newObject;

public class LocationGroup extends Entity {

    private static final long serialVersionUID = -7958476364523575940L;
    public String location_group_id;
    public String location_group_name;

    public static final String TABLE_NAME = "location_groups";
    public static final String LOCATION_GROUP_ID_COLUMN_NAME = "location_group_id";
    public static final String LOCATION_GROUP_NAME_COLUMN_NAME = "location_group_name";

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
            locationGroup.location_group_id = getStringField(LOCATION_GROUP_ID_COLUMN_NAME, false);
            locationGroup.location_group_name = getStringField(LOCATION_GROUP_NAME_COLUMN_NAME, false);
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
            writer.writeRecord(new String[] {LOCATION_GROUP_ID_COLUMN_NAME, LOCATION_GROUP_NAME_COLUMN_NAME});
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

    public static final GraphQLObjectType locationGroupType = newObject().name(TABLE_NAME)
        .description("A GTFS location group object.")
        .field(MapFetcher.field("id", GraphQLInt))
        .field(MapFetcher.field(LOCATION_GROUP_ID_COLUMN_NAME))
        .field(MapFetcher.field(LOCATION_GROUP_NAME_COLUMN_NAME))
        .build();
}
