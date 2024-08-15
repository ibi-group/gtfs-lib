package com.conveyal.gtfs.model;

import com.conveyal.gtfs.GTFSFeed;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;

public class StopArea extends Entity {

    private static final long serialVersionUID = -2825890165823575940L;
    public String area_id;
    public String stop_id;
    public String feed_id;

    public static final String TABLE_NAME = "stop_areas";
    public static final String AREA_ID_COLUMN_NAME = "area_id";
    public static final String STOP_ID_COLUMN_NAME = "stop_id";

    @Override
    public String getId () {
        return createPrimaryKey(area_id, stop_id);
    }

    /**
     * Sets the parameters for a prepared statement following the parameter order defined in
     * {@link com.conveyal.gtfs.loader.Table#STOP_AREAS}. JDBC prepared statement parameters use a one-based index.
     */
    @Override
    public void setStatementParameters(PreparedStatement statement, boolean setDefaultId) throws SQLException {
        int oneBasedIndex = 1;
        if (!setDefaultId) statement.setInt(oneBasedIndex++, id);
        statement.setString(oneBasedIndex++, area_id);
        statement.setString(oneBasedIndex, stop_id);
    }

    public static class Loader extends Entity.Loader<Area> {

        public Loader(GTFSFeed feed) {
            super(feed, TABLE_NAME);
        }

        @Override
        protected boolean isRequired() {
            return false;
        }

        @Override
        public void loadOneRow() throws IOException {
            StopArea stopArea = new StopArea();
            stopArea.id = row + 1; // offset line number by 1 to account for 0-based row index
            stopArea.area_id = getStringField(AREA_ID_COLUMN_NAME, true);
            stopArea.stop_id = getStringField(STOP_ID_COLUMN_NAME, true);
            stopArea.feed = feed;
            stopArea.feed_id = feed.feedId;
            feed.stop_areas.put(stopArea.getId(), stopArea);

            /*
              Check referential integrity without storing references.
             */
            getRefField(AREA_ID_COLUMN_NAME, true, feed.areas);
            getRefField(STOP_ID_COLUMN_NAME, true, feed.stops);

        }

    }

    public static class Writer extends Entity.Writer<StopArea> {
        public Writer(GTFSFeed feed) {
            super(feed, TABLE_NAME);
        }

        @Override
        public void writeHeaders() throws IOException {
            writer.writeRecord(new String[] {AREA_ID_COLUMN_NAME, STOP_ID_COLUMN_NAME});
        }

        @Override
        public void writeOneRow(StopArea stopArea) throws IOException {
            writeStringField(stopArea.area_id);
            writeStringField(stopArea.stop_id);
            endRecord();
        }

        @Override
        public Iterator<StopArea> iterator() {
            return this.feed.stop_areas.values().iterator();
        }
    }
}


