package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.model.Agency;
import com.conveyal.gtfs.model.Area;
import com.conveyal.gtfs.model.Calendar;
import com.conveyal.gtfs.model.CalendarDate;
import com.conveyal.gtfs.model.Entity;
import com.conveyal.gtfs.model.FareAttribute;
import com.conveyal.gtfs.model.FareLegRule;
import com.conveyal.gtfs.model.FareMedia;
import com.conveyal.gtfs.model.FareProduct;
import com.conveyal.gtfs.model.FareTransferRule;
import com.conveyal.gtfs.model.Frequency;
import com.conveyal.gtfs.model.Network;
import com.conveyal.gtfs.model.Pattern;
import com.conveyal.gtfs.model.PatternStop;
import com.conveyal.gtfs.model.Route;
import com.conveyal.gtfs.model.RouteNetwork;
import com.conveyal.gtfs.model.ScheduleException;
import com.conveyal.gtfs.model.ShapePoint;
import com.conveyal.gtfs.model.Stop;
import com.conveyal.gtfs.model.StopArea;
import com.conveyal.gtfs.model.StopTime;
import com.conveyal.gtfs.model.TimeFrame;
import com.conveyal.gtfs.model.Trip;
import gnu.trove.map.TObjectIntMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URL;
import java.time.LocalDate;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.conveyal.gtfs.model.ScheduleException.exemplarFromInt;

/**
 * For now we will copy all available fields into Java model objects.
 *
 * We could also have entity cursors with accessor functions that move along the results, and only look at
 * the fields we need. We could even select only the required fields from the backend database but that gets
 * messy. Anecdotal evidence suggests this would give about a 1/3 speedup. But other observations show no speedup at all.
 * The main benefit would be grabbing arbitrary extension columns.
 *
 * TODO associate EntityPopulator more closely with Entity types and Table instances, so you can get one from the other.
 * e.g. getEntityPopulator() and getTableSpec() on Entity classes.
 *
 * TODO maybe instantiate EntityCreators with a columnForName map to avoid passing them around and make them implement Iterable<T>
 * Could even initialize with the resultSet and call it a Factory
 *
 * // FIXME URLs?
 *
 * This might also be useable with Commons DBUtils as a result row processor.
 *
 * // FIXME add this interface to Entity itself. And add reader / writer functions to entity as well?
 */
public interface EntityPopulator<T> {
    Logger LOG = LoggerFactory.getLogger(EntityPopulator.class);
    EntityPopulator<PatternStop> PATTERN_STOP = (result, columnForName) -> {
        PatternStop patternStop = new PatternStop();
        patternStop.stop_id = getStringIfPresent(result, "stop_id", columnForName);
        patternStop.default_dwell_time = getIntIfPresent(result, "default_dwell_time", columnForName);
        patternStop.default_travel_time = getIntIfPresent(result, "default_travel_time", columnForName);
        patternStop.pattern_id = getStringIfPresent(result, "pattern_id", columnForName);
        patternStop.drop_off_type = getIntIfPresent(result, "drop_off_type", columnForName);
        patternStop.stop_headsign = getStringIfPresent(result, "stop_headsign", columnForName);
        patternStop.pickup_type = getIntIfPresent(result, "pickup_type", columnForName);
        patternStop.stop_sequence = getIntIfPresent(result, "stop_sequence", columnForName);
        patternStop.timepoint = getIntIfPresent(result, "timepoint", columnForName);
        patternStop.shape_dist_traveled = getDoubleIfPresent(result, "shape_dist_traveled", columnForName);
        patternStop.continuous_pickup   = getIntIfPresent   (result, "continuous_pickup",   columnForName);
        patternStop.continuous_drop_off = getIntIfPresent   (result, "continuous_drop_off", columnForName);
        return patternStop;
    };

    EntityPopulator<Pattern> PATTERN = (result, columnForName) -> {
        Pattern pattern = new Pattern();
        pattern.pattern_id = getStringIfPresent(result, "pattern_id", columnForName);
        pattern.route_id = getStringIfPresent(result, "route_id", columnForName);
        pattern.name = getStringIfPresent(result, "name", columnForName);
        pattern.direction_id = getIntIfPresent(result, "direction_id", columnForName);
        pattern.use_frequency = getIntIfPresent(result, "use_frequency", columnForName);
        pattern.shape_id = getStringIfPresent(result, "shape_id", columnForName);
        return pattern;
    };

    T populate (ResultSet results, TObjectIntMap<String> columnForName) throws SQLException;

    EntityPopulator<Agency> AGENCY = (result, columnForName) -> {
        Agency agency              = new Agency();
        agency.agency_id           = getStringIfPresent(result, "agency_id", columnForName);
        agency.agency_name         = getStringIfPresent(result, "agency_name", columnForName);
        agency.agency_url          = getUrlIfPresent   (result, "agency_url", columnForName);
        agency.agency_timezone     = getStringIfPresent(result, "agency_timezone", columnForName);
        agency.agency_lang         = getStringIfPresent(result, "agency_lang", columnForName);
        agency.agency_phone        = getStringIfPresent(result, "agency_phone", columnForName);
        agency.agency_fare_url     = getUrlIfPresent   (result, "agency_fare_url", columnForName);
        agency.agency_email        = getStringIfPresent(result, "agency_email", columnForName);
        agency.agency_branding_url = getUrlIfPresent   (result, "agency_branding_url", columnForName);
        return agency;
    };

    EntityPopulator<Calendar> CALENDAR = (result, columnForName) -> {
        Calendar calendar   = new Calendar();
        calendar.service_id = getStringIfPresent(result, "service_id", columnForName);
        calendar.start_date = getDateIfPresent  (result, "start_date", columnForName);
        calendar.end_date   = getDateIfPresent  (result, "end_date",   columnForName);
        calendar.monday     = getIntIfPresent   (result, "monday",     columnForName);
        calendar.tuesday    = getIntIfPresent   (result, "tuesday",    columnForName);
        calendar.wednesday  = getIntIfPresent   (result, "wednesday",  columnForName);
        calendar.thursday   = getIntIfPresent   (result, "thursday",   columnForName);
        calendar.friday     = getIntIfPresent   (result, "friday",     columnForName);
        calendar.saturday   = getIntIfPresent   (result, "saturday",   columnForName);
        calendar.sunday     = getIntIfPresent   (result, "sunday",     columnForName);
        return calendar;
    };

    EntityPopulator<CalendarDate> CALENDAR_DATE = (result, columnForName) -> {
        CalendarDate calendarDate   = new CalendarDate();
        calendarDate.service_id     = getStringIfPresent(result, "service_id",     columnForName);
        calendarDate.date           = getDateIfPresent  (result, "date",           columnForName);
        calendarDate.exception_type = getIntIfPresent   (result, "exception_type", columnForName);
        return calendarDate;
    };

    EntityPopulator<FareAttribute> FARE_ATTRIBUTE = (result, columnForName) -> {
        FareAttribute fareAttribute     = new FareAttribute();
        fareAttribute.fare_id           = getStringIfPresent(result, "fare_id",     columnForName);
        fareAttribute.agency_id         = getStringIfPresent(result, "agency_id",     columnForName);
        fareAttribute.price             = getDoubleIfPresent(result, "price",           columnForName);
        fareAttribute.payment_method    = getIntIfPresent   (result, "payment_method", columnForName);
        fareAttribute.transfers         = getIntIfPresent   (result, "transfers", columnForName);
        fareAttribute.transfer_duration = getIntIfPresent   (result, "transfer_duration", columnForName);
        return fareAttribute;
    };

    EntityPopulator<Frequency> FREQUENCY = (result, columnForName) -> {
        Frequency frequency    = new Frequency();
        frequency.trip_id      = getStringIfPresent(result, "trip_id",     columnForName);
        frequency.start_time   = getIntIfPresent   (result, "start_time", columnForName);
        frequency.end_time     = getIntIfPresent   (result, "end_time", columnForName);
        frequency.headway_secs = getIntIfPresent   (result, "headway_secs", columnForName);
        frequency.exact_times  = getIntIfPresent   (result, "exact_times", columnForName);
        return frequency;
    };

    EntityPopulator<ScheduleException> SCHEDULE_EXCEPTION = (result, columnForName) -> {
        ScheduleException scheduleException = new ScheduleException();
        scheduleException.name              = getStringIfPresent    (result, "name", columnForName);
        scheduleException.dates             = getDateListIfPresent  (result, "dates", columnForName);
        scheduleException.exemplar          = exemplarFromInt(getIntIfPresent(result, "exemplar", columnForName));
        scheduleException.customSchedule    = getStringListIfPresent(result, "custom_schedule", columnForName);
        scheduleException.addedService      = getStringListIfPresent(result, "added_service", columnForName);
        scheduleException.removedService    = getStringListIfPresent(result, "removed_service", columnForName);
        return scheduleException;
    };

    EntityPopulator<Route> ROUTE = (result, columnForName) -> {
        Route route               = new Route();
        route.route_id            = getStringIfPresent(result, "route_id",            columnForName);
        route.agency_id           = getStringIfPresent(result, "agency_id",           columnForName);
        route.route_short_name    = getStringIfPresent(result, "route_short_name",    columnForName);
        route.route_long_name     = getStringIfPresent(result, "route_long_name",     columnForName);
        route.route_desc          = getStringIfPresent(result, "route_desc",          columnForName);
        route.route_type          = getIntIfPresent   (result, "route_type",          columnForName);
        route.route_color         = getStringIfPresent(result, "route_color",         columnForName);
        route.route_text_color    = getStringIfPresent(result, "route_text_color",    columnForName);
        route.route_url           = getUrlIfPresent   (result, "route_url",           columnForName);
        route.route_branding_url  = getUrlIfPresent   (result, "route_branding_url",  columnForName);
        route.continuous_pickup   = getIntIfPresent   (result, "continuous_pickup",   columnForName);
        route.continuous_drop_off = getIntIfPresent   (result, "continuous_drop_off", columnForName);
        return route;
    };

    EntityPopulator<Stop> STOP = (result, columnForName) -> {
        Stop stop           = new Stop();
        stop.stop_id        = getStringIfPresent(result, "stop_id",        columnForName);
        stop.stop_code      = getStringIfPresent(result, "stop_code",      columnForName);
        stop.stop_name      = getStringIfPresent(result, "stop_name",      columnForName);
        stop.stop_desc      = getStringIfPresent(result, "stop_desc",      columnForName);
        stop.stop_lat       = getDoubleIfPresent(result, "stop_lat",       columnForName);
        stop.stop_lon       = getDoubleIfPresent(result, "stop_lon",       columnForName);
        stop.zone_id        = getStringIfPresent(result, "zone_id",        columnForName);
        stop.parent_station = getStringIfPresent(result, "parent_station", columnForName);
        stop.stop_timezone  = getStringIfPresent(result, "stop_timezone",  columnForName);
        stop.stop_url       = getUrlIfPresent   (result, "stop_url",       columnForName);
        stop.location_type  = getIntIfPresent   (result, "location_type",  columnForName);
        stop.wheelchair_boarding = getIntIfPresent(result, "wheelchair_boarding", columnForName);
        stop.platform_code  = getStringIfPresent(result, "platform_code",  columnForName);
        return stop;
    };

    EntityPopulator<Trip> TRIP = (result, columnForName) -> {
        Trip trip            = new Trip();
        trip.trip_id         = getStringIfPresent(result, "trip_id", columnForName);
        trip.route_id        = getStringIfPresent(result, "route_id", columnForName);
        trip.service_id      = getStringIfPresent(result, "service_id", columnForName);
        trip.trip_headsign   = getStringIfPresent(result, "trip_headsign", columnForName);
        trip.trip_short_name = getStringIfPresent(result, "trip_short_name", columnForName);
        trip.block_id        = getStringIfPresent(result, "block_id", columnForName);
        trip.shape_id        = getStringIfPresent(result, "shape_id", columnForName);
        trip.direction_id    = getIntIfPresent   (result, "direction_id", columnForName);
        trip.bikes_allowed   = getIntIfPresent   (result, "bikes_allowed", columnForName);
        trip.wheelchair_accessible = getIntIfPresent(result, "wheelchair_accessible", columnForName);
        return trip;
    };

    EntityPopulator<ShapePoint> SHAPE_POINT = (result, columnForName) -> {
        ShapePoint shapePoint          = new ShapePoint();
        shapePoint.shape_id            = getStringIfPresent(result, "shape_id", columnForName);
        shapePoint.shape_pt_lat        = getDoubleIfPresent(result, "shape_pt_lat", columnForName);
        shapePoint.shape_pt_lon        = getDoubleIfPresent(result, "shape_pt_lon", columnForName);
        shapePoint.shape_pt_sequence   = getIntIfPresent   (result, "shape_pt_sequence", columnForName);
        shapePoint.shape_dist_traveled = getDoubleIfPresent(result, "shape_dist_traveled", columnForName);
        return shapePoint;
    };

    EntityPopulator<StopTime> STOP_TIME = (result, columnForName) -> {
        StopTime stopTime            = new StopTime();
        stopTime.trip_id             = getStringIfPresent(result, "trip_id", columnForName);
        stopTime.arrival_time        = getIntIfPresent   (result, "arrival_time", columnForName);
        stopTime.departure_time      = getIntIfPresent   (result, "departure_time", columnForName);
        stopTime.stop_id             = getStringIfPresent(result, "stop_id", columnForName);
        stopTime.stop_sequence       = getIntIfPresent   (result, "stop_sequence", columnForName);
        stopTime.stop_headsign       = getStringIfPresent(result, "stop_headsign", columnForName);
        stopTime.pickup_type         = getIntIfPresent   (result, "pickup_type", columnForName);
        stopTime.drop_off_type       = getIntIfPresent   (result, "drop_off_type", columnForName);
        stopTime.continuous_pickup   = getIntIfPresent   (result, "continuous_pickup", columnForName);
        stopTime.continuous_drop_off = getIntIfPresent   (result, "continuous_drop_off", columnForName);
        stopTime.timepoint           = getIntIfPresent   (result, "timepoint", columnForName);
        stopTime.shape_dist_traveled = getDoubleIfPresent(result, "shape_dist_traveled", columnForName);
        return stopTime;
    };

    EntityPopulator<Area> AREA = (result, columnForName) -> {
        Area area = new Area();
        area.area_id = getStringIfPresent(result, Area.AREA_ID_COLUMN_NAME, columnForName);
        area.area_name = getStringIfPresent(result, Area.AREA_NAME_COLUMN_NAME, columnForName);
        return area;
    };

    EntityPopulator<StopArea> STOP_AREA = (result, columnForName) -> {
        StopArea stopArea = new StopArea();
        stopArea.area_id = getStringIfPresent(result, StopArea.AREA_ID_COLUMN_NAME, columnForName);
        stopArea.stop_id = getStringIfPresent(result, StopArea.STOP_ID_COLUMN_NAME, columnForName);
        return stopArea;
    };

    EntityPopulator<FareMedia> FARE_MEDIA = (result, columnForName) -> {
        FareMedia fareMedia = new FareMedia();
        fareMedia.fare_media_id = getStringIfPresent(result, FareMedia.FARE_MEDIA_ID_COLUMN_NAME, columnForName);
        fareMedia.fare_media_name = getStringIfPresent(result, FareMedia.FARE_MEDIA_NAME_COLUMN_NAME, columnForName);
        fareMedia.fare_media_type = getIntIfPresent(result, FareMedia.FARE_MEDIA_TYPE_COLUMN_NAME, columnForName);
        return fareMedia;
    };

    EntityPopulator<FareProduct> FARE_PRODUCT = (result, columnForName) -> {
        FareProduct fareProduct = new FareProduct();
        fareProduct.fare_product_id = getStringIfPresent(result, FareProduct.FARE_PRODUCT_ID_COLUMN_NAME, columnForName);
        fareProduct.fare_product_name = getStringIfPresent(result, FareProduct.FARE_PRODUCT_NAME_COLUMN_NAME, columnForName);
        fareProduct.fare_media_id = getStringIfPresent(result, FareProduct.FARE_MEDIA_ID_COLUMN_NAME, columnForName);
        fareProduct.amount = getDoubleIfPresent(result, FareProduct.AMOUNT_COLUMN_NAME, columnForName);
        fareProduct.currency = getStringIfPresent(result, FareProduct.CURRENCY_COLUMN_NAME, columnForName);
        return fareProduct;
    };

    EntityPopulator<TimeFrame> TIME_FRAME = (result, columnForName) -> {
        TimeFrame timeFrame = new TimeFrame();
        timeFrame.timeframe_group_id = getStringIfPresent(result, TimeFrame.TIME_FRAME_GROUP_ID_COLUMN_NAME, columnForName);
        timeFrame.start_time = getIntIfPresent(result, TimeFrame.START_TIME_COLUMN_NAME, columnForName);
        timeFrame.end_time = getIntIfPresent(result, TimeFrame.END_TIME_COLUMN_NAME, columnForName);
        timeFrame.service_id = getStringIfPresent(result, FareProduct.FARE_MEDIA_ID_COLUMN_NAME, columnForName);
        return timeFrame;
    };

    EntityPopulator<FareLegRule> FARE_LEG_RULE = (result, columnForName) -> {
        FareLegRule fareLegRule = new FareLegRule();
        fareLegRule.leg_group_id = getStringIfPresent(result, FareLegRule.LEG_GROUP_ID_COLUMN_NAME, columnForName);
        fareLegRule.from_timeframe_group_id = getStringIfPresent(result, FareLegRule.FROM_AREA_ID_COLUMN_NAME, columnForName);
        fareLegRule.to_timeframe_group_id = getStringIfPresent(result, FareLegRule.TO_AREA_ID_COLUMN_NAME, columnForName);
        fareLegRule.from_timeframe_group_id = getStringIfPresent(result, FareLegRule.FROM_TIMEFRAME_GROUP_ID_COLUMN_NAME, columnForName);
        fareLegRule.to_timeframe_group_id = getStringIfPresent(result, FareLegRule.TO_TIMEFRAME_GROUP_ID_COLUMN_NAME, columnForName);
        fareLegRule.fare_product_id = getStringIfPresent(result, FareLegRule.FARE_PRODUCT_ID_COLUMN_NAME, columnForName);
        fareLegRule.rule_priority = getIntIfPresent(result, FareLegRule.RULE_PRIORITY_COLUMN_NAME, columnForName);
        return fareLegRule;
    };

    EntityPopulator<FareTransferRule> FARE_TRANSFER_RULE = (result, columnForName) -> {
        FareTransferRule fareTransferRule = new FareTransferRule();
        fareTransferRule.from_leg_group_id = getStringIfPresent(result, FareTransferRule.FROM_LEG_GROUP_ID_COLUMN_NAME, columnForName);
        fareTransferRule.to_leg_group_id = getStringIfPresent(result, FareTransferRule.TO_LEG_GROUP_ID_COLUMN_NAME, columnForName);
        fareTransferRule.transfer_count = getIntIfPresent(result, FareTransferRule.TRANSFER_COUNT_COLUMN_NAME, columnForName);
        fareTransferRule.duration_limit = getIntIfPresent(result, FareTransferRule.DURATION_LIMIT_COLUMN_NAME, columnForName);
        fareTransferRule.duration_limit_type = getIntIfPresent(result, FareTransferRule.DURATION_LIMIT_TYPE_COLUMN_NAME, columnForName);
        fareTransferRule.fare_transfer_type = getIntIfPresent(result, FareTransferRule.FARE_TRANSFER_TYPE_COLUMN_NAME, columnForName);
        fareTransferRule.fare_product_id = getStringIfPresent(result, FareTransferRule.FARE_PRODUCT_ID_COLUMN_NAME, columnForName);
        return fareTransferRule;
    };

    EntityPopulator<Network> NETWORK = (result, columnForName) -> {
        Network network = new Network();
        network.network_id = getStringIfPresent(result, Network.NETWORK_ID_COLUMN_NAME, columnForName);
        network.network_name = getStringIfPresent(result, Network.NETWORK_NAME_COLUMN_NAME, columnForName);
        return network;
    };

    EntityPopulator<RouteNetwork> ROUTE_NETWORK = (result, columnForName) -> {
        RouteNetwork routeNetwork = new RouteNetwork();
        routeNetwork.network_id = getStringIfPresent(result, RouteNetwork.NETWORK_ID_COLUMN_NAME, columnForName);
        routeNetwork.route_id = getStringIfPresent(result, RouteNetwork.ROUTE_ID_COLUMN_NAME, columnForName);
        return routeNetwork;
    };

    // The reason we're passing in the columnForName map is that resultSet.getX(columnName) throws an exception
    // when the column is not present.
    // Exceptions should only be used in exceptional circumstances (ones that should be logged as errors).
    // Conceivably we could iterate over the fields present using ResultSetMetaData and set the object fields only
    // for those fields present. Or we could create cursor objects that allow accessing the fields of the ResultSet
    // in a typed way. Those cursor objects would make their own columnForName map when constructed.

    static String getStringIfPresent (ResultSet resultSet, String columnName,
                                             TObjectIntMap<String> columnForName) throws SQLException {
        int columnIndex = columnForName.get(columnName);
        if (columnIndex == 0) return null;
        else return resultSet.getString(columnIndex);
    }

    static LocalDate getDateIfPresent (ResultSet resultSet, String columnName,
                                             TObjectIntMap<String> columnForName) throws SQLException {
        int columnIndex = columnForName.get(columnName);
        if (columnIndex == 0) return null;
        else {
            try {
                String dateString = resultSet.getString(columnIndex);
                return dateString != null ? LocalDate.parse(dateString, DateField.GTFS_DATE_FORMATTER) : null;
            } catch (DateTimeParseException ex) {
                // We're reading out of the database here, not loading from GFTS CSV, so just return null for bad values.
                return null;
            }
        }
    }

    static List<String> getStringListIfPresent(ResultSet resultSet, String columnName,
                                       TObjectIntMap<String> columnForName) throws SQLException {
        int columnIndex = columnForName.get(columnName);
        if (columnIndex == 0) return new ArrayList<>();
        try {
            List<String> strings = Arrays.asList((String[]) resultSet.getArray(columnIndex).getArray());
            return strings;

        } catch (Exception e) {
            return new ArrayList<>();
        }
    }

    static List<LocalDate> getDateListIfPresent(ResultSet resultSet, String columnName,
                                        TObjectIntMap<String> columnForName) throws SQLException {
        int columnIndex = columnForName.get(columnName);
        if (columnIndex == 0) return new ArrayList<>();
        try {
            String[] dateStrings = (String[]) resultSet.getArray(columnIndex).getArray();
            return Arrays.stream(dateStrings)
                    .map(date -> date != null ? LocalDate.parse(date, DateField.GTFS_DATE_FORMATTER) : null)
                    .collect(Collectors.toList());
        } catch (Exception e) {
            return new ArrayList<>();
        }
    }

    static URL getUrlIfPresent (ResultSet resultSet, String columnName,
                                       TObjectIntMap<String> columnForName) throws SQLException {
        int columnIndex = columnForName.get(columnName);
        if (columnIndex == 0) return null;
        try {
            URL url = new URL(resultSet.getString(columnIndex));
            return url;
        } catch (MalformedURLException e) {
            return null;
        }
    }

    static double getDoubleIfPresent (ResultSet resultSet, String columnName,
                                             TObjectIntMap<String> columnForName) throws SQLException {
        int columnIndex = columnForName.get(columnName);
        if (columnIndex == 0) return Entity.DOUBLE_MISSING;
        double doubleValue = resultSet.getDouble(columnIndex);
        // If SQL value for column was null, resultSet.getDouble will return 0.0. If this is the case, override value with
        // DOUBLE_MISSING.
        if (resultSet.wasNull()) return Entity.DOUBLE_MISSING;
        else return doubleValue;
    }

    static int getIntIfPresent (ResultSet resultSet, String columnName,
                                       TObjectIntMap<String> columnForName) throws SQLException {
        int columnIndex = columnForName.get(columnName);
        if (columnIndex == 0) return Entity.INT_MISSING;
        int intValue = resultSet.getInt(columnIndex);
        // If SQL value for column was null, resultSet.getInt will return 0. If this is the case, override value with
        // INT_MISSING.
        if (resultSet.wasNull()) return Entity.INT_MISSING;
        else return intValue;
    }
}
