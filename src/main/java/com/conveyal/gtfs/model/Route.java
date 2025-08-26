package com.conveyal.gtfs.model;

import com.conveyal.gtfs.GTFSFeed;
import com.conveyal.gtfs.error.NoAgencyInFeedError;
import com.conveyal.gtfs.loader.EntityPopulator;
import com.conveyal.gtfs.loader.JDBCTableReader;
import com.conveyal.gtfs.loader.Table;
import com.conveyal.gtfs.loader.TableLoadResult;
import com.conveyal.gtfs.loader.TableReader;
import com.csvreader.CsvReader;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.net.URL;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import java.util.zip.ZipOutputStream;

import static com.conveyal.gtfs.model.RouteNetwork.ROUTE_NETWORK_FILE_NAME;
import static com.conveyal.gtfs.util.CsvReaderUtil.hasExpectedNumberOfColumns;

public class Route extends Entity {

    private static final Logger LOG = LoggerFactory.getLogger(Route.class);

    private static final long serialVersionUID = -819444896818029068L;

    public static final int TRAM = 0;
    public static final int SUBWAY = 1;
    public static final int RAIL = 2;
    public static final int BUS = 3;
    public static final int FERRY = 4;
    public static final int CABLE_CAR = 5;
    public static final int GONDOLA = 6;
    public static final int FUNICULAR = 7;

    public String route_id;
    public String agency_id;
    public String route_short_name;
    public String route_long_name;
    public String route_desc;
    public int    route_type;
    public URL    route_url;
    public String route_color;
    public int route_sort_order;
    public String route_text_color;
    public URL route_branding_url;
    public String feed_id;
    public int continuous_pickup = INT_MISSING;
    public int continuous_drop_off = INT_MISSING;

    /** Used to directly link a route to a network. Multiple routes can have the same network id. This is forbidden if
     * route network ids are defined. */
    public String network_id;
    public String route_network_ids;

    public static final String ROUTE_ID_FIELD = "route_id";
    public static final String AGENCY_ID_FIELD = "agency_id";
    public static final String ROUTE_SHORT_NAME_FIELD = "route_short_name";
    public static final String ROUTE_LONG_NAME_FIELD = "route_long_name";
    public static final String ROUTE_DESC_FIELD = "route_desc";
    public static final String ROUTE_TYPE_FIELD = "route_type";
    public static final String ROUTE_URL_FIELD = "route_url";
    public static final String ROUTE_COLOR_FIELD = "route_color";
    public static final String ROUTE_SORT_ORDER_FIELD = "route_sort_order";
    public static final String ROUTE_TEXT_COLOR_FIELD = "route_text_color";
    public static final String ROUTE_BRANDING_URL_FIELD = "route_branding_url";
    public static final String CONTINUOUS_PICKUP_FIELD = "continuous_pickup";
    public static final String CONTINUOUS_DROP_OFF_FIELD = "continuous_drop_off";
    public static final String NETWORK_ID_FIELD = "network_id";
    public static final String ROUTE_NETWORK_IDS_FIELD = "route_network_ids";

    public static final String ROUTE_FILE_NAME = "routes.txt";

    public static final String TABLE_NAME = "routes";

    private static final String[] CSV_FIELDS = new String[] {
        ROUTE_ID_FIELD,
        AGENCY_ID_FIELD,
        ROUTE_SHORT_NAME_FIELD,
        ROUTE_LONG_NAME_FIELD,
        ROUTE_DESC_FIELD,
        ROUTE_TYPE_FIELD,
        ROUTE_URL_FIELD,
        ROUTE_COLOR_FIELD,
        ROUTE_SORT_ORDER_FIELD,
        ROUTE_TEXT_COLOR_FIELD,
        ROUTE_BRANDING_URL_FIELD,
        CONTINUOUS_PICKUP_FIELD,
        CONTINUOUS_DROP_OFF_FIELD,
        NETWORK_ID_FIELD
    };

    private static final String CSV_HEADER_FOR_MERGE = String.format(
        "%s,%s%n",
        String.join(",", CSV_FIELDS),
        ROUTE_NETWORK_IDS_FIELD
    );

    private static final String CSV_HEADER_FOR_EXPORT = String.join(",", CSV_FIELDS);

    @Override
    public String getId () {
        return route_id;
    }

    /**
     * Sets the parameters for a prepared statement following the parameter order defined in
     * {@link com.conveyal.gtfs.loader.Table#ROUTES}. JDBC prepared statement parameters use a one-based index.
     */
    @Override
    public void setStatementParameters(PreparedStatement statement, boolean setDefaultId) throws SQLException {
        int oneBasedIndex = 1;
        if (!setDefaultId) statement.setInt(oneBasedIndex++, id);
        statement.setString(oneBasedIndex++, route_id);
        statement.setString(oneBasedIndex++, agency_id);
        statement.setString(oneBasedIndex++, route_short_name);
        statement.setString(oneBasedIndex++, route_long_name);
        statement.setString(oneBasedIndex++, route_desc);
        setIntParameter(statement, oneBasedIndex++, route_type);
        statement.setString(oneBasedIndex++, route_url != null ? route_url.toString() : null);
        statement.setString(oneBasedIndex++, route_branding_url != null ? route_branding_url.toString() : null);
        statement.setString(oneBasedIndex++, route_color);
        statement.setString(oneBasedIndex++, route_text_color);
        // Editor-specific fields publicly_visible, wheelchair_accessible, route_sort_order, and status.
        setIntParameter(statement, oneBasedIndex++, 0);
        setIntParameter(statement, oneBasedIndex++, 0);
        // route_sort_order
        setIntParameter(statement, oneBasedIndex++, route_sort_order);
        setIntParameter(statement, oneBasedIndex++, 0);
        setIntParameter(statement, oneBasedIndex++, continuous_pickup);
        setIntParameter(statement, oneBasedIndex++, continuous_drop_off);
        statement.setString(oneBasedIndex++, network_id);
        statement.setString(oneBasedIndex, route_network_ids);

    }

    public static class Loader extends Entity.Loader<Route> {

        public Loader(GTFSFeed feed) {
            super(feed, TABLE_NAME);
        }

        @Override
        protected boolean isRequired() {
            return true;
        }

        @Override
        public void loadOneRow() throws IOException {
            Route r = new Route();
            r.id = row + 1; // offset line number by 1 to account for 0-based row index
            r.route_id = getStringField(ROUTE_ID_FIELD, true);
            Agency agency = getRefField("agency_id", false, feed.agency);

            if (agency == null) {
                // if there is only one agency, associate with it automatically
                if (feed.agency.size() == 1) {
                    r.agency_id = feed.agency.values().iterator().next().agency_id;
                } else if (feed.agency.isEmpty()) {
                    feed.errors.add(new NoAgencyInFeedError());
                }
            } else {
                r.agency_id = agency.agency_id;
            }

            r.route_short_name = getStringField(ROUTE_SHORT_NAME_FIELD, false); // one or the other required, needs a special validator
            r.route_long_name = getStringField(ROUTE_LONG_NAME_FIELD, false);
            r.route_desc = getStringField(ROUTE_DESC_FIELD, false);
            r.route_type = getIntField(ROUTE_TYPE_FIELD, true, 0, 7);
            r.route_sort_order = getIntField(ROUTE_SORT_ORDER_FIELD, false, 0, Integer.MAX_VALUE);
            r.route_url = getUrlField(ROUTE_URL_FIELD, false);
            r.route_color = getStringField(ROUTE_COLOR_FIELD, false);
            r.route_text_color = getStringField(ROUTE_TEXT_COLOR_FIELD, false);
            r.route_branding_url = getUrlField(ROUTE_BRANDING_URL_FIELD, false);
            r.continuous_pickup = getIntField(CONTINUOUS_PICKUP_FIELD, false, 0, 3, INT_MISSING);
            r.continuous_drop_off = getIntField(CONTINUOUS_DROP_OFF_FIELD, false, 0, 3, INT_MISSING);
            r.network_id = getStringField(NETWORK_ID_FIELD, false);
            r.route_network_ids = getStringField(ROUTE_NETWORK_IDS_FIELD, false);
            r.feed = feed;
            r.feed_id = feed.feedId;
            // Attempting to put a null key or value will cause an NPE in BTreeMap
            if (r.route_id != null) feed.routes.put(r.route_id, r);
        }

    }

    public static class Writer extends Entity.Writer<Route> {    	
        public Writer (GTFSFeed feed) {
            super(feed, TABLE_NAME);
        }

        @Override
        public void writeHeaders() throws IOException {
            writer.writeRecord(CSV_FIELDS);
        }

        @Override
        public void writeOneRow(Route r) throws IOException {
            writeStringField(r.agency_id);
            writeStringField(r.route_id);
            writeStringField(r.route_short_name);
            writeStringField(r.route_long_name);
            writeStringField(r.route_desc);
            writeIntField(r.route_type);
            writeUrlField(r.route_url);
            writeStringField(r.route_color);
            writeStringField(r.route_text_color);
            writeUrlField(r.route_branding_url);
            writeIntField(r.route_sort_order);
            writeIntField(r.continuous_pickup);
            writeIntField(r.continuous_drop_off);
            writeStringField(r.network_id);
            endRecord();
        }

        @Override
        public Iterator<Route> iterator() {
            return feed.routes.values().iterator();
        }   	
    }

    /**
     * Merge route networks into routes when loading from file.
     */
    public static void mergeRouteNetworks(Map<String, Route> routes, Map<String, RouteNetwork> routeNetworks) {
        Map<String, Set<String>> routeNetworksByRouteId = new HashMap<>();

        routeNetworks.values().forEach(routeNetwork ->
            routeNetworksByRouteId
                .computeIfAbsent(routeNetwork.route_id, id -> new HashSet<>())
                .add(routeNetwork.network_id)
        );

        routes.values().forEach(route -> route.route_network_ids = getChildIdsMatchingParentId(routeNetworksByRouteId, route.route_id));
    }

    /**
     * Merge route networks into routes when loading into DB.
     */
    public static CsvReader getCsvReaderForRoutesWithRouteNetworks(
        CsvReader routesReader,
        Map<String, Set<String>> routeNetworksByRouteId
    ) {
        List<String> rows = new ArrayList<>();
        try {
            while (routesReader.readRecord()) {
                String routeId = routesReader.get(ROUTE_ID_FIELD);
                rows.add(createRow(routesReader, getChildIdsMatchingParentId(routeNetworksByRouteId, routeId), CSV_FIELDS));
            }
            return (rows.isEmpty())
                ? routesReader
                : produceCsvPayload(rows, CSV_HEADER_FOR_MERGE);
        } catch (Exception e) {
            LOG.error("Error while merging routes", e);
            // Any issues, return the original routes reader (minus route networks).
            return routesReader;
        }
    }

    /**
     * Extract the route networks from file and group by route id. This is to allow for easier CRUD by the DT UI.
     */
    public static Map<String, Set<String>> groupRouteNetworkIds(CsvReader csvReader, List<String> errors) {
        Map<String, Set<String>> routeNetworksGroupedByRouteId = new HashMap<>();

        try {
            while (csvReader.readRecord()) {
                if (!hasExpectedNumberOfColumns(csvReader, errors, 2)) {
                    continue;
                }
                String routeNetworkId = csvReader.get(RouteNetwork.NETWORK_ID_FIELD);
                String routeId = csvReader.get(RouteNetwork.ROUTE_ID_FIELD);
                routeNetworksGroupedByRouteId.computeIfAbsent(routeId, k -> new HashSet<>()).add(routeNetworkId);
            }
            return routeNetworksGroupedByRouteId;
        } catch (IOException e) {
            return Collections.emptyMap();
        }
    }

    /**
     * Expand all route network ids into a single row for each route id. This is to conform with the GTFS Fares v2 standard.
     */
    public static String packRouteNetworks(List<Route> routes) {
        StringBuilder csvContent = new StringBuilder(createRow(RouteNetwork.NETWORK_ID_FIELD, RouteNetwork.ROUTE_ID_FIELD));
        routes
            .stream()
            .filter(route -> route.route_network_ids != null)
            .forEach(route -> {
                String[] routeNetworkIds = route.route_network_ids.split(SEPARATOR);
                for (String routeNetworkId : routeNetworkIds) {
                    csvContent.append(createRow(routeNetworkId, route.route_id));
                }
            });
        return csvContent.toString();
    }

    /**
     * Export routes, minus route networks.
     */
    public static TableLoadResult exportRoutes(
        DataSource dataSource,
        String feedIdToExport,
        ZipOutputStream zipOutputStream,
        String whereRouteIsApproved
    ) {
        long startTime = System.currentTimeMillis();
        TableLoadResult tableLoadResult = new TableLoadResult();

        try {
            final TableReader<Route> routeIterator = new JDBCTableReader<>(
                Table.ROUTES,
                dataSource,
                feedIdToExport + ".",
                EntityPopulator.ROUTE,
                whereRouteIsApproved
            );

            List<Route> routes = Lists.newArrayList(routeIterator);
            tableLoadResult.rowCount = routes.size();
            writeEntityToFile(zipOutputStream, routes, ROUTE_FILE_NAME);

            long duration = System.currentTimeMillis() - startTime;
            LOG.info("Copied {} {} in {} ms.", tableLoadResult.rowCount, ROUTE_FILE_NAME, duration);

        } catch (IOException e) {
            tableLoadResult.fatalException = e.toString();
            LOG.error("Exception while exporting {}", ROUTE_FILE_NAME, e);
        }

        return tableLoadResult;
    }

    /**
     * Expand all stops into a single row.
     */
    public static String packRoutes(List<Route> routes) {
        StringBuilder csvContent = new StringBuilder(createRow(CSV_HEADER_FOR_EXPORT));
        routes.forEach(route -> csvContent.append(createRow(
            computeCsvValue(route.route_id),
            computeCsvValue(route.agency_id),
            computeCsvValue(route.route_short_name),
            computeCsvValue(route.route_long_name),
            computeCsvValue(route.route_desc),
            computeCsvValue(route.route_type),
            computeCsvValue(route.route_url),
            computeCsvValue(route.route_color),
            computeCsvValue(route.route_sort_order),
            computeCsvValue(route.route_text_color),
            computeCsvValue(route.route_branding_url),
            computeCsvValue(route.continuous_pickup),
            computeCsvValue(route.continuous_drop_off),
            computeCsvValue(route.network_id)
        )));
        return csvContent.toString();
    }

    /**
     * Export route networks.
     */
    public static TableLoadResult exportRouteNetworks(
        DataSource dataSource,
        String feedIdToExport,
        ZipOutputStream zipOutputStream
    ) {
        long startTime = System.currentTimeMillis();
        TableLoadResult tableLoadResult = new TableLoadResult();

        try {
            final TableReader<Route> routeIterator = new JDBCTableReader<>(
                Table.ROUTES,
                dataSource,
                feedIdToExport + ".",
                EntityPopulator.ROUTE
            );

            List<Route> routesWithRouteNetworks = StreamSupport
                .stream(routeIterator.spliterator(), false)
                .filter(route -> !StringUtils.isBlank(route.route_network_ids))
                .collect(Collectors.toList());

            // Only export if data is available.
            if (routesWithRouteNetworks.isEmpty()) {
                LOG.warn("No route networks exported as none have been defined!");
                return tableLoadResult;
            }

            tableLoadResult.rowCount = routesWithRouteNetworks.size();
            writeEntityToFile(zipOutputStream, routesWithRouteNetworks, ROUTE_NETWORK_FILE_NAME);

            long duration = System.currentTimeMillis() - startTime;
            LOG.info("Copied {} {} in {} ms.", tableLoadResult.rowCount, ROUTE_NETWORK_FILE_NAME, duration);

        } catch (IOException e) {
            tableLoadResult.fatalException = e.toString();
            LOG.error("Exception while exporting {}", ROUTE_NETWORK_FILE_NAME, e);
        }

        return tableLoadResult;
    }
}
