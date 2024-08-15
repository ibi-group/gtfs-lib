package com.conveyal.gtfs.model;

import com.conveyal.gtfs.GTFSFeed;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;

public class FareMedia extends Entity {

    private static final long serialVersionUID = -4968771968571945940L;

    public String fare_media_id;
    public String fare_media_name;
    public int fare_media_type;
    public String feed_id;

    public static final String TABLE_NAME = "fare_media";
    public static final String FARE_MEDIA_ID_COLUMN_NAME = "fare_media_id";
    public static final String FARE_MEDIA_NAME_COLUMN_NAME = "fare_media_name";
    public static final String FARE_MEDIA_TYPE_COLUMN_NAME = "fare_media_type";

    @Override
    public String getId () {
        return fare_media_id;
    }

    /**
     * Sets the parameters for a prepared statement following the parameter order defined in
     * {@link com.conveyal.gtfs.loader.Table#FARE_MEDIAS}. JDBC prepared statement parameters use a one-based index.
     */
    @Override
    public void setStatementParameters(PreparedStatement statement, boolean setDefaultId) throws SQLException {
        int oneBasedIndex = 1;
        if (!setDefaultId) statement.setInt(oneBasedIndex++, id);
        statement.setString(oneBasedIndex++, fare_media_id);
        statement.setString(oneBasedIndex++, fare_media_name);
        setIntParameter(statement, oneBasedIndex, fare_media_type);
    }

    public static class Loader extends Entity.Loader<FareMedia> {

        public Loader(GTFSFeed feed) {
            super(feed, TABLE_NAME);
        }

        @Override
        protected boolean isRequired() {
            return false;
        }

        @Override
        public void loadOneRow() throws IOException {
            FareMedia fareMedia = new FareMedia();
            fareMedia.id = row + 1; // offset line number by 1 to account for 0-based row index
            fareMedia.fare_media_id = getStringField(FARE_MEDIA_ID_COLUMN_NAME, true);
            fareMedia.fare_media_name = getStringField(FARE_MEDIA_NAME_COLUMN_NAME, false);
            fareMedia.fare_media_type = getIntField(FARE_MEDIA_TYPE_COLUMN_NAME, true, 0, 4);
            fareMedia.feed = feed;
            fareMedia.feed_id = feed.feedId;
            feed.fare_medias.put(fareMedia.getId(), fareMedia);
        }

    }

    public static class Writer extends Entity.Writer<FareMedia> {
        public Writer(GTFSFeed feed) {
            super(feed, TABLE_NAME);
        }

        @Override
        public void writeHeaders() throws IOException {
            writer.writeRecord(new String[] {
                FARE_MEDIA_ID_COLUMN_NAME,
                FARE_MEDIA_NAME_COLUMN_NAME,
                FARE_MEDIA_TYPE_COLUMN_NAME
            });
        }

        @Override
        public void writeOneRow(FareMedia fareMedia) throws IOException {
            writeStringField(fareMedia.fare_media_id);
            writeStringField(fareMedia.fare_media_name);
            writeIntField(fareMedia.fare_media_type);
            endRecord();
        }

        @Override
        public Iterator<FareMedia> iterator() {
            return this.feed.fare_medias.values().iterator();
        }
    }
}



