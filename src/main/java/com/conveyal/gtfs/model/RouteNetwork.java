package com.conveyal.gtfs.model;

import com.conveyal.gtfs.GTFSFeed;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * This is only included to read the values from file. No route networks are saved directly to the database. Instead,
 * they are merged with the appropriate route (and then saved to the database).
 */
public class RouteNetwork extends Entity {

    private static final long serialVersionUID = -4739475958736362940L;
    public String network_id;
    public String route_id;
    public String feed_id;

    public static final String TABLE_NAME = "route_networks";
    public static final String NETWORK_ID_NAME = "network_id";
    public static final String ROUTE_ID_NAME = "route_id";
    public static final String ROUTE_NETWORK_FILE_NAME = "route_networks.txt";
    public static final int ROUTE_NETWORK_NUMBER_OF_HEADERS = 2;

    @Override
    public String getId () {
        return route_id;
    }

    @Override
    public void setStatementParameters(PreparedStatement statement, boolean setDefaultId) throws SQLException {
        int oneBasedIndex = 1;
        if (!setDefaultId) statement.setInt(oneBasedIndex++, id);
        statement.setString(oneBasedIndex++, network_id);
        statement.setString(oneBasedIndex, route_id);
    }

    public static class Loader extends Entity.Loader<RouteNetwork> {

        public Loader(GTFSFeed feed) {
            super(feed, TABLE_NAME);
        }

        @Override
        protected boolean isRequired() {
            return false;
        }

        @Override
        public void loadOneRow() throws IOException {
            RouteNetwork routeNetwork = new RouteNetwork();
            routeNetwork.id = row + 1; // offset line number by 1 to account for 0-based row index
            routeNetwork.network_id  = getStringField(NETWORK_ID_NAME, true);
            routeNetwork.route_id = getStringField(ROUTE_ID_NAME, true);
            routeNetwork.feed = feed;
            routeNetwork.feed_id = feed.feedId;
            feed.route_networks.put(routeNetwork.getId(), routeNetwork);
            getRefField(NETWORK_ID_NAME, true, feed.networks);
            getRefField(ROUTE_ID_NAME, true, feed.routes);
        }
    }
}

