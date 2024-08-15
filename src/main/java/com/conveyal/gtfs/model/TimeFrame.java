package com.conveyal.gtfs.model;

import com.conveyal.gtfs.GTFSFeed;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;

public class TimeFrame extends Entity {

    private static final long serialVersionUID = -194783727784855940L;

    public String timeframe_group_id;
    public int start_time = INT_MISSING;
    public int end_time = INT_MISSING;
    public String service_id;
    public String feed_id;

    public static final String TABLE_NAME = "timeframes";
    public static final String TIME_FRAME_GROUP_ID_COLUMN_NAME = "timeframe_group_id";
    public static final String START_TIME_COLUMN_NAME = "start_time";
    public static final String END_TIME_COLUMN_NAME = "end_time";
    public static final String SERVICE_ID_COLUMN_NAME = "service_id";

    @Override
    public String getId () {
        return createPrimaryKey(timeframe_group_id, start_time, end_time, service_id);
    }

    /**
     * Sets the parameters for a prepared statement following the parameter order defined in
     * {@link com.conveyal.gtfs.loader.Table#TIME_FRAMES}. JDBC prepared statement parameters use a one-based index.
     */
    @Override
    public void setStatementParameters(PreparedStatement statement, boolean setDefaultId) throws SQLException {
        int oneBasedIndex = 1;
        if (!setDefaultId) statement.setInt(oneBasedIndex++, id);
        statement.setString(oneBasedIndex++, timeframe_group_id);
        setIntParameter(statement, oneBasedIndex++, start_time);
        setIntParameter(statement, oneBasedIndex++, end_time);
        statement.setString(oneBasedIndex, service_id);
    }

    public static class Loader extends Entity.Loader<TimeFrame> {

        public Loader(GTFSFeed feed) {
            super(feed, TABLE_NAME);
        }

        @Override
        protected boolean isRequired() {
            return false;
        }

        @Override
        public void loadOneRow() throws IOException {
            TimeFrame timeFrame = new TimeFrame();
            timeFrame.id = row + 1; // offset line number by 1 to account for 0-based row index
            timeFrame.timeframe_group_id = getStringField(TIME_FRAME_GROUP_ID_COLUMN_NAME, true);
            timeFrame.start_time = getTimeField(START_TIME_COLUMN_NAME, false);
            if (timeFrame.start_time == INT_MISSING) {
                // An empty value is considered the start of the day (00:00:00).
                timeFrame.start_time = 0;
            }
            timeFrame.end_time = getTimeField(END_TIME_COLUMN_NAME, false);
            if (timeFrame.end_time == INT_MISSING) {
                // An empty value is considered the end of the day (24:00:00).
                timeFrame.end_time = 86400;
            }
            timeFrame.service_id = getStringField(SERVICE_ID_COLUMN_NAME, true);
            timeFrame.feed = feed;
            timeFrame.feed_id = feed.feedId;
            feed.time_frames.put(timeFrame.getId(), timeFrame);
        }

    }

    public static class Writer extends Entity.Writer<TimeFrame> {
        public Writer(GTFSFeed feed) {
            super(feed, TABLE_NAME);
        }

        @Override
        public void writeHeaders() throws IOException {
            writer.writeRecord(new String[] {
                TIME_FRAME_GROUP_ID_COLUMN_NAME,
                START_TIME_COLUMN_NAME,
                END_TIME_COLUMN_NAME,
                SERVICE_ID_COLUMN_NAME
            });
        }

        @Override
        public void writeOneRow(TimeFrame timeFrame) throws IOException {
            writeStringField(timeFrame.timeframe_group_id);
            writeTimeField(timeFrame.start_time);
            writeTimeField(timeFrame.end_time);
            writeStringField(timeFrame.service_id);
            endRecord();
        }

        @Override
        public Iterator<TimeFrame> iterator() {
            return this.feed.time_frames.values().iterator();
        }
    }
}


