package com.conveyal.gtfs.model;

import com.conveyal.gtfs.GTFSFeed;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;

public class FareLegRule extends Entity {

    private static final long serialVersionUID = -795847376633855940L;

    public String leg_group_id;
    public String network_id;
    public String from_area_id;
    public String to_area_id;
    public String from_timeframe_group_id;
    public String to_timeframe_group_id;
    public String fare_product_id;
    public int rule_priority = INT_MISSING;

    public String feed_id;

    public static final String TABLE_NAME = "fare_leg_rules";
    public static final String LEG_GROUP_ID_NAME = "leg_group_id";
    public static final String NETWORK_ID_NAME = "network_id";
    public static final String FROM_AREA_ID_NAME = "from_area_id";
    public static final String TO_AREA_ID_NAME = "to_area_id";
    public static final String FROM_TIMEFRAME_GROUP_ID_NAME = "from_timeframe_group_id";
    public static final String TO_TIMEFRAME_GROUP_ID_NAME = "to_timeframe_group_id";
    public static final String FARE_PRODUCT_ID_NAME = "fare_product_id";
    public static final String RULE_PRIORITY_NAME = "rule_priority";

    @Override
    public String getId () {
        return createPrimaryKey(
            network_id,
            from_area_id,
            to_area_id,
            from_timeframe_group_id,
            to_timeframe_group_id,
            fare_product_id
        );
    }

    /**
     * Sets the parameters for a prepared statement following the parameter order defined in
     * {@link com.conveyal.gtfs.loader.Table#FARE_LEG_RULES}. JDBC prepared statement parameters use a one-based index.
     */
    @Override
    public void setStatementParameters(PreparedStatement statement, boolean setDefaultId) throws SQLException {
        int oneBasedIndex = 1;
        if (!setDefaultId) statement.setInt(oneBasedIndex++, id);
        statement.setString(oneBasedIndex++, leg_group_id);
        statement.setString(oneBasedIndex++, network_id);
        statement.setString(oneBasedIndex++, from_area_id);
        statement.setString(oneBasedIndex++, to_area_id);
        statement.setString(oneBasedIndex++, from_timeframe_group_id);
        statement.setString(oneBasedIndex++, to_timeframe_group_id);
        statement.setString(oneBasedIndex++, fare_product_id);
        setIntParameter(statement, oneBasedIndex, rule_priority);
    }

    public static class Loader extends Entity.Loader<FareLegRule> {

        public Loader(GTFSFeed feed) {
            super(feed, TABLE_NAME);
        }

        @Override
        protected boolean isRequired() {
            return false;
        }

        @Override
        public void loadOneRow() throws IOException {
            FareLegRule fareLegRule = new FareLegRule();
            fareLegRule.id = row + 1; // offset line number by 1 to account for 0-based row index
            fareLegRule.leg_group_id = getStringField(LEG_GROUP_ID_NAME, false);
            fareLegRule.network_id = getStringField(NETWORK_ID_NAME, false);
            fareLegRule.from_area_id = getStringField(FROM_AREA_ID_NAME, false);
            fareLegRule.to_area_id = getStringField(TO_AREA_ID_NAME, false);
            fareLegRule.from_timeframe_group_id = getStringField(FROM_TIMEFRAME_GROUP_ID_NAME, false);
            fareLegRule.to_timeframe_group_id = getStringField(TO_TIMEFRAME_GROUP_ID_NAME, false);
            fareLegRule.fare_product_id = getStringField(FARE_PRODUCT_ID_NAME, true);
            fareLegRule.rule_priority = getIntField(RULE_PRIORITY_NAME, false, 0, Integer.MAX_VALUE);
            if (fareLegRule.rule_priority == INT_MISSING) {
                // An empty value for rule_priority is treated as zero.
                fareLegRule.rule_priority = 0;
            }
            fareLegRule.feed = feed;
            fareLegRule.feed_id = feed.feedId;
            feed.fare_leg_rules.put(fareLegRule.getId(), fareLegRule);
        }

    }

    public static class Writer extends Entity.Writer<FareLegRule> {
        public Writer(GTFSFeed feed) {
            super(feed, TABLE_NAME);
        }

        @Override
        public void writeHeaders() throws IOException {
            writer.writeRecord(new String[] {
                LEG_GROUP_ID_NAME,
                NETWORK_ID_NAME,
                FROM_AREA_ID_NAME,
                TO_AREA_ID_NAME,
                FROM_TIMEFRAME_GROUP_ID_NAME,
                TO_TIMEFRAME_GROUP_ID_NAME,
                FARE_PRODUCT_ID_NAME,
                RULE_PRIORITY_NAME
            });
        }

        @Override
        public void writeOneRow(FareLegRule fareLegRule) throws IOException {
            writeStringField(fareLegRule.leg_group_id);
            writeStringField(fareLegRule.network_id);
            writeStringField(fareLegRule.from_area_id);
            writeStringField(fareLegRule.to_area_id);
            writeStringField(fareLegRule.from_timeframe_group_id);
            writeStringField(fareLegRule.to_timeframe_group_id);
            writeStringField(fareLegRule.fare_product_id);
            writeIntField(fareLegRule.rule_priority);
            endRecord();
        }

        @Override
        public Iterator<FareLegRule> iterator() {
            return this.feed.fare_leg_rules.values().iterator();
        }
    }
}


