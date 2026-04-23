package com.conveyal.gtfs.model;

import com.conveyal.gtfs.GTFSFeed;

import java.io.IOException;
import java.net.URL;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;

public class RiderCategory extends Entity {

    private static final long serialVersionUID = -2825589065823575940L;
    public String rider_category_id;
    public String rider_category_name;
    public int is_default_fare_category;
    public URL eligibility_url;
    public String feed_id;

    public static final String TABLE_NAME = "rider_categories";
    public static final String RIDER_CATEGORY_ID_NAME = "rider_category_id";
    public static final String RIDER_CATEGORY_NAME_NAME = "rider_category_name";
    public static final String RIDER_CATEGORY_IS_DEFAULT_FARE_CATEGORY_NAME = "is_default_fare_category";
    public static final String RIDER_CATEGORY_ELIGIBILITY_URL_NAME = "eligibility_url";

    @Override
    public String getId () {
        return rider_category_id;
    }

    /**
     * Sets the parameters for a prepared statement following the parameter order defined in
     * {@link com.conveyal.gtfs.loader.Table#RIDER_CATEGORIES}. JDBC prepared statement parameters use a one-based index.
     */
    @Override
    public void setStatementParameters(PreparedStatement statement, boolean setDefaultId) throws SQLException {
        int oneBasedIndex = 1;
        if (!setDefaultId) statement.setInt(oneBasedIndex++, id);
        statement.setString(oneBasedIndex++, rider_category_id);
        statement.setString(oneBasedIndex++, rider_category_name);
        setIntParameter(statement, oneBasedIndex++, is_default_fare_category);
        statement.setString(oneBasedIndex, eligibility_url != null ? eligibility_url.toString() : null);

    }

    public static class Loader extends Entity.Loader<RiderCategory> {

        public Loader(GTFSFeed feed) {
            super(feed, TABLE_NAME);
        }

        @Override
        protected boolean isRequired() {
            return false;
        }

        @Override
        public void loadOneRow() throws IOException {
            RiderCategory riderCategory = new RiderCategory();
            riderCategory.id = row + 1; // offset line number by 1 to account for 0-based row index
            riderCategory.rider_category_id  = getStringField(RIDER_CATEGORY_ID_NAME, true);
            riderCategory.rider_category_name  = getStringField(RIDER_CATEGORY_NAME_NAME, true);
            riderCategory.is_default_fare_category = getIntField(RIDER_CATEGORY_IS_DEFAULT_FARE_CATEGORY_NAME, true, 0, 1);
            riderCategory.eligibility_url = getUrlField(RIDER_CATEGORY_ELIGIBILITY_URL_NAME, false);
            riderCategory.feed = feed;
            riderCategory.feed_id = feed.feedId;
            feed.rider_categories.put(riderCategory.getId(), riderCategory);
        }
    }

    public static class Writer extends Entity.Writer<RiderCategory> {
        public Writer(GTFSFeed feed) {
            super(feed, TABLE_NAME);
        }

        @Override
        public void writeHeaders() throws IOException {
            writer.writeRecord(new String[] {
                RIDER_CATEGORY_ID_NAME,
                RIDER_CATEGORY_NAME_NAME,
                RIDER_CATEGORY_IS_DEFAULT_FARE_CATEGORY_NAME,
                RIDER_CATEGORY_ELIGIBILITY_URL_NAME
            });
        }

        @Override
        public void writeOneRow(RiderCategory riderCategory) throws IOException {
            writeStringField(riderCategory.rider_category_id);
            writeStringField(riderCategory.rider_category_name);
            writeIntField(riderCategory.is_default_fare_category);
            writeUrlField(riderCategory.eligibility_url);
            endRecord();
        }

        @Override
        public Iterator<RiderCategory> iterator() {
            return feed.rider_categories.values().iterator();
        }
    }

}

