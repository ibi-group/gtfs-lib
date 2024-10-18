package com.conveyal.gtfs.model;

import com.conveyal.gtfs.GTFSFeed;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;

public class FareTransferRule extends Entity {

    private static final long serialVersionUID = -958672649111736468L;

    public String from_leg_group_id;
    public String to_leg_group_id;
    public int transfer_count = INT_MISSING;
    public int duration_limit;
    public int duration_limit_type;
    public int fare_transfer_type;
    public String fare_product_id;

    public String feed_id;

    public static final String TABLE_NAME = "fare_transfer_rules";
    public static final String FROM_LEG_GROUP_ID_NAME = "from_leg_group_id";
    public static final String TO_LEG_GROUP_ID_NAME = "to_leg_group_id";
    public static final String TRANSFER_COUNT_NAME = "transfer_count";
    public static final String DURATION_LIMIT_NAME = "duration_limit";
    public static final String DURATION_LIMIT_TYPE_NAME = "duration_limit_type";
    public static final String FARE_TRANSFER_TYPE_NAME = "fare_transfer_type";
    public static final String FARE_PRODUCT_ID_NAME = "fare_product_id";

    @Override
    public String getId () {
        return createPrimaryKey(
            from_leg_group_id,
            to_leg_group_id,
            fare_product_id,
            transfer_count,
            duration_limit
        );
    }

    /**
     * Sets the parameters for a prepared statement following the parameter order defined in
     * {@link com.conveyal.gtfs.loader.Table#FARE_TRANSFER_RULES}. JDBC prepared statement parameters use a one-based index.
     */
    @Override
    public void setStatementParameters(PreparedStatement statement, boolean setDefaultId) throws SQLException {
        int oneBasedIndex = 1;
        if (!setDefaultId) statement.setInt(oneBasedIndex++, id);
        statement.setString(oneBasedIndex++, from_leg_group_id);
        statement.setString(oneBasedIndex++, to_leg_group_id);
        setIntParameter(statement, oneBasedIndex++, transfer_count);
        setIntParameter(statement, oneBasedIndex++, duration_limit);
        setIntParameter(statement, oneBasedIndex++, duration_limit_type);
        setIntParameter(statement, oneBasedIndex++, fare_transfer_type);
        statement.setString(oneBasedIndex, fare_product_id);
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
            FareTransferRule fareTransferRule = new FareTransferRule();
            fareTransferRule.id = row + 1; // offset line number by 1 to account for 0-based row index
            fareTransferRule.from_leg_group_id = getStringField(FROM_LEG_GROUP_ID_NAME, false);
            fareTransferRule.to_leg_group_id = getStringField(TO_LEG_GROUP_ID_NAME, false);
            fareTransferRule.transfer_count = getIntField(TRANSFER_COUNT_NAME, false, -1, Integer.MAX_VALUE, INT_MISSING);
            fareTransferRule.duration_limit = getIntField(DURATION_LIMIT_NAME, false, 0, Integer.MAX_VALUE);
            fareTransferRule.duration_limit_type = getIntField(DURATION_LIMIT_TYPE_NAME, false, 0, 3);
            fareTransferRule.fare_transfer_type = getIntField(FARE_TRANSFER_TYPE_NAME, true, 0, 2);
            fareTransferRule.fare_product_id = getStringField(FARE_PRODUCT_ID_NAME, false);
            fareTransferRule.feed = feed;
            fareTransferRule.feed_id = feed.feedId;
            feed.fare_transfer_rules.put(fareTransferRule.getId(), fareTransferRule);
        }

    }

    public static class Writer extends Entity.Writer<FareTransferRule> {
        public Writer(GTFSFeed feed) {
            super(feed, TABLE_NAME);
        }

        @Override
        public void writeHeaders() throws IOException {
            writer.writeRecord(new String[] {
                FROM_LEG_GROUP_ID_NAME,
                TO_LEG_GROUP_ID_NAME,
                TRANSFER_COUNT_NAME,
                DURATION_LIMIT_NAME,
                DURATION_LIMIT_TYPE_NAME,
                FARE_TRANSFER_TYPE_NAME,
                FARE_PRODUCT_ID_NAME,
            });
        }

        @Override
        public void writeOneRow(FareTransferRule fareTransferRule) throws IOException {
            writeStringField(fareTransferRule.from_leg_group_id);
            writeStringField(fareTransferRule.to_leg_group_id);
            writeIntField(fareTransferRule.transfer_count);
            writeIntField(fareTransferRule.duration_limit);
            writeIntField(fareTransferRule.duration_limit_type);
            writeIntField(fareTransferRule.fare_transfer_type);
            writeStringField(fareTransferRule.fare_product_id);
            endRecord();
        }

        @Override
        public Iterator<FareTransferRule> iterator() {
            return this.feed.fare_transfer_rules.values().iterator();
        }
    }
}



