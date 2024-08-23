package com.conveyal.gtfs.model;

import com.conveyal.gtfs.GTFSFeed;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;

public class Area extends Entity {

    private static final long serialVersionUID = -2825890165823575940L;
    public String area_id;
    public String area_name;
    public String feed_id;

    public static final String TABLE_NAME = "areas";
    public static final String AREA_ID_NAME = "area_id";
    public static final String AREA_NAME_NAME = "area_name";


    @Override
    public String getId () {
        return area_id;
    }

    /**
     * Sets the parameters for a prepared statement following the parameter order defined in
     * {@link com.conveyal.gtfs.loader.Table#AREAS}. JDBC prepared statement parameters use a one-based index.
     */
    @Override
    public void setStatementParameters(PreparedStatement statement, boolean setDefaultId) throws SQLException {
        int oneBasedIndex = 1;
        if (!setDefaultId) statement.setInt(oneBasedIndex++, id);
        statement.setString(oneBasedIndex++, area_id);
        statement.setString(oneBasedIndex, area_name);
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
            Area area = new Area();
            area.id = row + 1; // offset line number by 1 to account for 0-based row index
            area.area_id    = getStringField(AREA_ID_NAME, true);
            area.area_name  = getStringField(AREA_NAME_NAME, false);
            area.feed = feed;
            area.feed_id = feed.feedId;
            feed.areas.put(area.getId(), area);
        }

    }

    public static class Writer extends Entity.Writer<Area> {
        public Writer(GTFSFeed feed) {
            super(feed, TABLE_NAME);
        }

        @Override
        public void writeHeaders() throws IOException {
            writer.writeRecord(new String[] {AREA_ID_NAME, AREA_NAME_NAME});
        }

        @Override
        public void writeOneRow(Area area) throws IOException {
            writeStringField(area.area_id);
            writeStringField(area.area_name);
            endRecord();
        }

        @Override
        public Iterator<Area> iterator() {
            return this.feed.areas.values().iterator();
        }
    }

}

