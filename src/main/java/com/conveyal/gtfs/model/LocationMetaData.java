package com.conveyal.gtfs.model;

import com.conveyal.gtfs.GTFSFeed;
import mil.nga.sf.geojson.FeatureCollection;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Objects;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class LocationMetaData extends Entity {

    private static final long serialVersionUID = -3961619607147161195L;

    public String location_meta_data_id;
    public String properties;
    public String geometry_type;

    @Override
    public String getId() {
        return location_meta_data_id;
    }

    /**
     * Sets the parameters for a prepared statement following the parameter order defined in
     * {@link com.conveyal.gtfs.loader.Table#LOCATION_META_DATA}. JDBC prepared statement parameters use a one-based index.
     */
    @Override
    public void setStatementParameters(PreparedStatement statement, boolean setDefaultId) throws SQLException {
        int oneBasedIndex = 1;
        if (!setDefaultId) statement.setInt(oneBasedIndex++, id);
        statement.setString(oneBasedIndex++, location_meta_data_id);
        statement.setString(oneBasedIndex++, properties);
        statement.setString(oneBasedIndex, geometry_type);
    }

    public static class Loader extends Entity.Loader<LocationMetaData> {

        public Loader(GTFSFeed feed) {
            super(feed, "location_meta_data");
        }

        @Override
        protected boolean isRequired() {
            return false;
        }

        @Override
        public void loadOneRow() throws IOException {
            LocationMetaData locationMetaData = new LocationMetaData();
            locationMetaData.id = row + 1; // offset line number by 1 to account for 0-based row index
            locationMetaData.location_meta_data_id = getStringField("location_meta_data_id", true);
            locationMetaData.properties = getStringField("properties", true);
            locationMetaData.geometry_type = getStringField("geometry_type", true);

            // Attempting to put a null key or value will cause an NPE in BTreeMap
            if (locationMetaData.location_meta_data_id != null) {
                feed.locationMetaData.put(locationMetaData.location_meta_data_id, locationMetaData);
            }
        }
    }

    /**
     * Required by {@link com.conveyal.gtfs.util.GeoJsonUtil#getCsvReaderFromGeoJson(String, ZipFile, ZipEntry)} as part
     * of the unpacking of GeoJson data to CSV.
     */
    public static String header() {
        return "location_meta_data_id,properties,geometry_type\n";
    }

    /**
     * Required by {@link com.conveyal.gtfs.util.GeoJsonUtil#getCsvReaderFromGeoJson(String, ZipFile, ZipEntry)} as part
     * of the unpacking of GeoJson data to CSV.
     */
    public String toCsvRow() {
        return location_meta_data_id + "," + properties + "," + geometry_type + "\n";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LocationMetaData that = (LocationMetaData) o;
        return
            Objects.equals(location_meta_data_id, that.location_meta_data_id) &&
            Objects.equals(properties, that.properties) &&
            Objects.equals(geometry_type, that.geometry_type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(location_meta_data_id, properties, geometry_type);
    }

    @Override
    public String toString() {
        return "LocationMetaData{" +
            "location_meta_data_id='" + location_meta_data_id + '\'' +
            ", properties='" + properties + '\'' +
            ", geometry_type='" + geometry_type + '\'' +
            '}';
    }
}