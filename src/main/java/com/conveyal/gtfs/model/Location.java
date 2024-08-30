package com.conveyal.gtfs.model;

import com.conveyal.gtfs.GTFSFeed;

import java.io.IOException;
import java.net.URL;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class Location extends Entity {

    private static final long serialVersionUID = -3961639608144161095L;

    public String location_id;
    public String stop_name;
    public String stop_desc;
    public String zone_id;
    public URL stop_url;
    public String geometry_type;

    public static final String TABLE_NAME = "locations";
    public static final String LOCATION_ID_NAME = "location_id";
    public static final String STOP_NAME_NAME = "stop_name";
    public static final String STOP_DESC_NAME = "stop_desc";
    public static final String ZONE_ID_NAME = "zone_id";
    public static final String STOP_URL_NAME = "stop_url";
    public static final String GEOMETRY_TYPE_NAME = "geometry_type";


    @Override
    public String getId() {
        return location_id;
    }

    /**
     * Sets the parameters for a prepared statement following the parameter order defined in
     * {@link com.conveyal.gtfs.loader.Table#LOCATIONS}. JDBC prepared statement parameters use a one-based index.
     */
    @Override
    public void setStatementParameters(PreparedStatement statement, boolean setDefaultId) throws SQLException {
        int oneBasedIndex = 1;
        if (!setDefaultId) statement.setInt(oneBasedIndex++, id);
        statement.setString(oneBasedIndex++, location_id);
        statement.setString(oneBasedIndex++, stop_name);
        statement.setString(oneBasedIndex++, stop_desc);
        statement.setString(oneBasedIndex++, zone_id);
        statement.setString(oneBasedIndex++, stop_url != null ? stop_url.toString() : null);
        statement.setString(oneBasedIndex, geometry_type);
    }

    /**
     * Required by {@link com.conveyal.gtfs.util.GeoJsonUtil#getCsvReaderFromGeoJson(String, ZipFile, ZipEntry, List)} as part
     * of the unpacking of GeoJSON data to CSV.
     */
    public static String header() {
        return String.format(
            "%s,%s,%s,%s,%s,%s%n",
            LOCATION_ID_NAME,
            STOP_NAME_NAME,
            STOP_DESC_NAME,
            ZONE_ID_NAME,
            STOP_URL_NAME,
            GEOMETRY_TYPE_NAME
        );
    }

    /**
     * Required by {@link com.conveyal.gtfs.util.GeoJsonUtil#getCsvReaderFromGeoJson(String, ZipFile, ZipEntry, List)} as part
     * of the unpacking of GeoJSON data to CSV.
     */
    public String toCsvRow() {
        String stopName = "", stopDesc = "";
        if (stop_name != null) {
            stopName = stop_name.contains(",") ? "\"" + stop_name + "\"" : stop_name;
        }
        if (stop_desc != null) {
            stopDesc = stop_desc.contains(",") ? "\"" + stop_desc + "\"" : stop_desc;
        }
        String stopUrl = stop_url == null ? "" : stop_url.toString();
        String zoneId = zone_id == null ? "" : zone_id;
        return String.join(
            ",",
            location_id,
            stopName,
            stopDesc,
            zoneId,
            stopUrl,
            geometry_type
        ) + System.lineSeparator();
    }

    public static class Loader extends Entity.Loader<Location> {

        public Loader(GTFSFeed feed) {
            super(feed, TABLE_NAME);
        }

        @Override
        protected boolean isRequired() {
            return false;
        }

        @Override
        public void loadOneRow() throws IOException {
            Location location = new Location();

            location.id = row + 1;
            location.location_id = getStringField(LOCATION_ID_NAME, true);
            location.stop_name = getStringField(STOP_NAME_NAME, false);
            location.stop_desc = getStringField(STOP_DESC_NAME, false);
            location.zone_id = getStringField(ZONE_ID_NAME, false);
            location.stop_url = getUrlField(STOP_URL_NAME, false);
            // Must be a geometry associated w/ a location
            location.geometry_type = getStringField(GEOMETRY_TYPE_NAME, true);

            // Attempting to put a null key or value will cause an NPE in BTreeMap
            if (location.location_id != null) {
                feed.locations.put(location.location_id, location);
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Location that = (Location) o;
        return Objects.equals(stop_name, that.stop_name) &&
            Objects.equals(zone_id, that.zone_id) &&
            Objects.equals(stop_desc, that.stop_desc) &&
            Objects.equals(stop_url, that.stop_url) &&
            Objects.equals(location_id, that.location_id) &&
            Objects.equals(geometry_type, that.geometry_type
        );
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            location_id,
            stop_name,
            stop_desc,
            stop_url,
            zone_id,
            geometry_type
        );
    }
}
