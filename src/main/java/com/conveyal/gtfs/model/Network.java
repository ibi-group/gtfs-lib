package com.conveyal.gtfs.model;

import com.conveyal.gtfs.GTFSFeed;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;

public class Network extends Entity {

    private static final long serialVersionUID = -4739475958736362940L;
    public String network_id;
    public String network_name;
    public String feed_id;

    public static final String TABLE_NAME = "networks";
    public static final String NETWORK_ID_NAME = "network_id";
    public static final String NETWORK_NAME_NAME = "network_name";

    @Override
    public String getId () {
        return network_id;
    }

    /**
     * Sets the parameters for a prepared statement following the parameter order defined in
     * {@link com.conveyal.gtfs.loader.Table#NETWORKS}. JDBC prepared statement parameters use a one-based index.
     */
    @Override
    public void setStatementParameters(PreparedStatement statement, boolean setDefaultId) throws SQLException {
        int oneBasedIndex = 1;
        if (!setDefaultId) statement.setInt(oneBasedIndex++, id);
        statement.setString(oneBasedIndex++, network_id);
        statement.setString(oneBasedIndex, network_name);
    }

    public static class Loader extends Entity.Loader<Network> {

        public Loader(GTFSFeed feed) {
            super(feed, TABLE_NAME);
        }

        @Override
        protected boolean isRequired() {
            return false;
        }

        @Override
        public void loadOneRow() throws IOException {
            Network network = new Network();
            network.id = row + 1; // offset line number by 1 to account for 0-based row index
            network.network_id    = getStringField(NETWORK_ID_NAME, true);
            network.network_name  = getStringField(NETWORK_NAME_NAME, false);
            network.feed = feed;
            network.feed_id = feed.feedId;
            feed.networks.put(network.getId(), network);
        }

    }

    public static class Writer extends Entity.Writer<Network> {
        public Writer(GTFSFeed feed) {
            super(feed, TABLE_NAME);
        }

        @Override
        public void writeHeaders() throws IOException {
            writer.writeRecord(new String[] {NETWORK_ID_NAME, NETWORK_NAME_NAME});
        }

        @Override
        public void writeOneRow(Network network) throws IOException {
            writeStringField(network.network_id);
            writeStringField(network.network_name);
            endRecord();
        }

        @Override
        public Iterator<Network> iterator() {
            return this.feed.networks.values().iterator();
        }
    }

}

