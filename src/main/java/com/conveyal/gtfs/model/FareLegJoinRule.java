package com.conveyal.gtfs.model;

import com.conveyal.gtfs.GTFSFeed;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;

public class FareLegJoinRule extends Entity {

    private static final long serialVersionUID = -2825890165823575940L;
    public String from_network_id;
    public String to_network_id;
    public String from_stop_id;
    public String to_stop_id;
    public String feed_id;

    public static final String TABLE_NAME = "fare_leg_join_rules";
    public static final String FROM_NETWORK_ID_NAME = "from_network_id";
    public static final String TO_NETWORK_ID_NAME = "to_network_id";
    public static final String FROM_STOP_ID_NAME = "from_stop_id";
    public static final String TO_STOP_ID_NAME = "to_stop_id";


    @Override
    public String getId () {
        return createPrimaryKey(
            from_network_id,
            to_network_id,
            from_stop_id,
            to_stop_id
        );
    }

    /**
     * Sets the parameters for a prepared statement following the parameter order defined in
     * {@link com.conveyal.gtfs.loader.Table#FARE_LEG_JOIN_RULES}. JDBC prepared statement parameters use a one-based index.
     */
    @Override
    public void setStatementParameters(PreparedStatement statement, boolean setDefaultId) throws SQLException {
        int oneBasedIndex = 1;
        if (!setDefaultId) statement.setInt(oneBasedIndex++, id);
        statement.setString(oneBasedIndex++, from_network_id);
        statement.setString(oneBasedIndex++, to_network_id);
        statement.setString(oneBasedIndex++, from_stop_id);
        statement.setString(oneBasedIndex, to_stop_id);
    }

    public static class Loader extends Entity.Loader<FareLegJoinRule> {

        public Loader(GTFSFeed feed) {
            super(feed, TABLE_NAME);
        }

        @Override
        protected boolean isRequired() {
            return false;
        }

        @Override
        public void loadOneRow() throws IOException {
            FareLegJoinRule fareLegJoinRule = new FareLegJoinRule();
            fareLegJoinRule.id = row + 1; // offset line number by 1 to account for 0-based row index
            fareLegJoinRule.from_network_id = getStringField(FROM_NETWORK_ID_NAME, true);
            fareLegJoinRule.to_network_id = getStringField(TO_NETWORK_ID_NAME, true);
            fareLegJoinRule.from_stop_id = getStringField(FROM_STOP_ID_NAME, false);
            fareLegJoinRule.to_stop_id = getStringField(TO_STOP_ID_NAME, false);
            fareLegJoinRule.feed = feed;
            fareLegJoinRule.feed_id = feed.feedId;
            feed.fare_leg_join_rules.put(fareLegJoinRule.getId(), fareLegJoinRule);
        }
    }

    public static class Writer extends Entity.Writer<FareLegJoinRule> {
        public Writer(GTFSFeed feed) {
            super(feed, TABLE_NAME);
        }

        @Override
        public void writeHeaders() throws IOException {
            writer.writeRecord(new String[] {
                FROM_NETWORK_ID_NAME,
                TO_NETWORK_ID_NAME,
                FROM_STOP_ID_NAME,
                TO_STOP_ID_NAME
            });
        }

        @Override
        public void writeOneRow(FareLegJoinRule fareLegJoinRule) throws IOException {
            writeStringField(fareLegJoinRule.from_network_id);
            writeStringField(fareLegJoinRule.to_network_id);
            writeStringField(fareLegJoinRule.from_stop_id);
            writeStringField(fareLegJoinRule.to_stop_id);
            endRecord();
        }

        @Override
        public Iterator<FareLegJoinRule> iterator() {
            return feed.fare_leg_join_rules.values().iterator();
        }
    }
}