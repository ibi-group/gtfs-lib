package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.error.SQLErrorStorage;
import com.conveyal.gtfs.loader.Feed;
import com.conveyal.gtfs.model.Location;
import com.conveyal.gtfs.model.LocationGroup;
import com.conveyal.gtfs.model.Route;
import com.conveyal.gtfs.model.Stop;
import com.conveyal.gtfs.model.StopTime;
import com.conveyal.gtfs.model.Trip;
import com.google.common.collect.Lists;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.conveyal.gtfs.error.NewGTFSErrorType.*;

/**
 * This validator checks for unused entities in the dataset.
 *
 * It iterates over each trip and collects all of the route, trip, and stop IDs referenced by all of the trips found
 * within the feed. In the completion stage of the validator it verifies that there are no stops, trips, or routes in
 * the feed that do not actually get used by at least one trip.
 *
 * Created by abyrd on 2017-04-18
 */
public class ReferencedTripValidator extends TripValidator {

    Set<String> referencedStops = new HashSet<>();
    Set<String> referencedTrips = new HashSet<>();
    Set<String> referencedRoutes = new HashSet<>();
    Set<String> referencedLocations = new HashSet<>();
    Set<String> referencedLocationGroups = new HashSet<>();

    public ReferencedTripValidator(Feed feed, SQLErrorStorage errorStorage) {
        super(feed, errorStorage);
    }

    @Override
    public void validateTrip(
        Trip trip,
        Route route,
        List<StopTime> stopTimes,
        List<Stop> stops,
        List<Location> locations,
        List<LocationGroup> locationGroups
    ) {
        if (trip != null) referencedTrips.add(trip.trip_id);
        if (route != null) referencedRoutes.add(route.route_id);
        for (Stop stop : stops) {
            if (stop == null) {
                continue;
            }
            referencedStops.add(stop.stop_id);
            // If a stop used by the trip has a parent station, count this among the referenced stops, too. While the
            // parent station may not be referenced directly, the relationship is functioning correctly and there is
            // not an issue with this stop being unreferenced.
            if (stop.parent_station != null) referencedStops.add(stop.parent_station);
        }
        locations.forEach(location -> {
            if (location != null) {
                referencedLocations.add(location.location_id);
            }
        });
        locationGroups.forEach(locationGroup -> {
            if (locationGroup != null) {
                referencedLocationGroups.add(locationGroup.location_group_id);
            }
        });
    }

    @Override
    public void complete (ValidationResult validationResult) {
        for (Stop stop : feed.stops) {
            if (!referencedStops.contains(stop.stop_id)) {
                registerError(stop, STOP_UNUSED, stop.stop_id);
            }
        }
        for (Trip trip : feed.trips) {
            if (!referencedTrips.contains(trip.trip_id)) {
                registerError(trip, TRIP_EMPTY);
            }
        }
        for (Route route : feed.routes) {
            if (!referencedRoutes.contains(route.route_id)) {
                registerError(route, ROUTE_UNUSED);
            }
        }

        // If a stop time references a location, make sure that the referenced location is used.
        List<Location> locations = Lists.newArrayList(feed.locations);
        feed.stopTimes.forEach(stopTime -> {
            if (stopTime.location_id != null && !referencedLocations.contains(stopTime.location_id)) {
                registerError(getLocationById(locations, stopTime.location_id), LOCATION_UNUSED, stopTime.location_id);
            }
        });

        // If a stop time references a location group, make sure that the referenced location group is used.
        List<LocationGroup> locationGroups = Lists.newArrayList(feed.locationGroups);
        feed.stopTimes.forEach(stopTime -> {
            if (stopTime.location_group_id != null && !referencedLocationGroups.contains(stopTime.location_group_id)) {
                registerError(
                    getLocationGroupById(locationGroups, stopTime.location_group_id),
                    LOCATION_GROUP_UNUSED,
                    stopTime.stop_id
                );
            }
        });
    }

    /**
     * Get location by location id or return null if there is no match.
     */
    private Location getLocationById(List<Location> locations, String locationId) {
        return locations.stream()
            .filter(location -> locationId.equals(location.location_id))
            .findAny()
            .orElse(null);
    }

    /**
     * Get location group by location group id or return null if there is no match.
     */
    private LocationGroup getLocationGroupById(List<LocationGroup> locationGroups, String locationGroupId) {
        return locationGroups.stream()
            .filter(locationGroup -> locationGroupId.equals(locationGroup.location_group_id))
            .findAny()
            .orElse(null);
    }

}
