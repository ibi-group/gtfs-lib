package com.conveyal.gtfs;

import com.conveyal.gtfs.error.NewGTFSError;
import com.conveyal.gtfs.error.NewGTFSErrorType;
import com.conveyal.gtfs.error.SQLErrorStorage;
import com.conveyal.gtfs.model.LocationGroup;
import com.conveyal.gtfs.model.Location;
import com.conveyal.gtfs.model.Pattern;
import com.conveyal.gtfs.model.Stop;
import com.conveyal.gtfs.model.LocationGroupStop;
import com.conveyal.gtfs.model.StopTime;
import com.conveyal.gtfs.model.Trip;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static com.conveyal.gtfs.util.Util.human;

/**
 * This abstracts out the logic for finding stop sequences ("journey patterns" in Transmodel parlance) based on trips.
 * Placing this logic in a separate class allows us to use it on GTFS data from multiple sources.
 * Our two specific use cases are finding patterns in stop_times that have already been loaded into an RDBMS, and
 * finding patterns while loading Java objects directly into a MapDB database.
 *
 * Created by abyrd on 2017-10-08
 */
public class PatternFinder {

    private static final Logger LOG = LoggerFactory.getLogger(PatternFinder.class);

    // A multi-map that groups trips together by their sequence of stops
    private Multimap<TripPatternKey, Trip> tripsForPattern = LinkedHashMultimap.create();

    private int nTripsProcessed = 0;

    public void processTrip(Trip trip, Iterable<StopTime> orderedStopTimes) {
        if (++nTripsProcessed % 100000 == 0) {
            LOG.info("trip {}", human(nTripsProcessed));
        }
        // No need to scope the route ID here, patterns are built within the context of a single feed.
        // Create a key that might already be in the map (by semantic equality)
        TripPatternKey key = new TripPatternKey(trip.route_id);
        for (StopTime st : orderedStopTimes) {
            key.addStopTime(st);
        }
        // Add the current trip to the map, possibly extending an existing list of trips on this pattern.
        tripsForPattern.put(key, trip);
    }

    /**
     * Once all trips have been processed, call this method to produce the final Pattern objects representing all the
     * unique sequences of stops encountered. Returns map of patterns to their keys so that downstream functions can
     * make use of trip pattern keys for constructing pattern stops or other derivative objects.
     *
     * There is no viable relationship between patterns that are loaded from a feed (patternsFromFeed) and patterns
     * generated here. Process ordering is used to update the pattern id and name if patterns from a feed are available.
     * E.g. The first pattern loaded from a feed will be used to updated the first pattern created here.
     */
    public Map<TripPatternKey, Pattern> createPatternObjects(
        Map<String, Stop> stopById,
        Map<String, Location> locationById,
        Map<String, LocationGroupStop> locationGroupStopById,
        Map<String, LocationGroup> locationGroupById,
        List<Pattern> patternsFromFeed,
        SQLErrorStorage errorStorage
    ) {
        // Make pattern ID one-based to avoid any JS type confusion between an ID of zero vs. null value.
        int nextPatternId = 1;
        int patternsFromFeedIndex = 0;
        boolean usePatternsFromFeed = canUsePatternsFromFeed(patternsFromFeed);
        // Create an in-memory list of Patterns because we will later rename them before inserting them into storage.
        // Use a LinkedHashMap so we can retrieve the entrySets later in the order of insertion.
        Map<TripPatternKey, Pattern> patterns = new LinkedHashMap<>();
        // TODO assign patterns sequential small integer IDs (may include route)
        for (TripPatternKey key : tripsForPattern.keySet()) {
            Collection<Trip> trips = tripsForPattern.get(key);
            Pattern pattern = new Pattern(key.orderedHalts, trips, null);
            if (usePatternsFromFeed) {
                pattern.pattern_id = patternsFromFeed.get(patternsFromFeedIndex).pattern_id;
                pattern.name = patternsFromFeed.get(patternsFromFeedIndex).name;
            } else {
                // Overwrite long UUID with sequential integer pattern ID
                pattern.pattern_id = Integer.toString(nextPatternId++);
            }
            // FIXME: Should associated shapes be a single entry?
            pattern.associatedShapes = new HashSet<>();
            trips.stream().forEach(trip -> pattern.associatedShapes.add(trip.shape_id));
            if (pattern.associatedShapes.size() > 1 && errorStorage != null) {
                // Store an error if there is more than one shape per pattern. Note: error storage is null if called via
                // MapDB implementation.
                // TODO: Should shape ID be added to trip pattern key?
                errorStorage.storeError(NewGTFSError.forEntity(
                        pattern,
                        NewGTFSErrorType.MULTIPLE_SHAPES_FOR_PATTERN)
                            .setBadValue(pattern.associatedShapes.toString()));
            }
            patterns.put(key, pattern);
            patternsFromFeedIndex++;
        }
        if (!usePatternsFromFeed) {
            // Name patterns before storing in SQL database if they have not already been provided with a feed.
            renamePatterns(patterns.values(), stopById, locationById, locationGroupStopById, locationGroupById);
        }
        LOG.info("Total patterns: {}", tripsForPattern.keySet().size());
        return patterns;
    }

    /**
     * If there is a difference in the number of patterns provided by a feed and the number of patterns generated here,
     * the patterns provided by the feed are rejected.
     */
    public boolean canUsePatternsFromFeed(List<Pattern> patternsFromFeed) {
        boolean usePatternsFromFeed = patternsFromFeed != null && patternsFromFeed.size() == tripsForPattern.keySet().size();
        LOG.info("Using patterns from feed: {}",  usePatternsFromFeed);
        return usePatternsFromFeed;
    }

    /**
     * Destructively rename the supplied collection of patterns.
     * This process requires access to all the stops in the feed.
     * Some validators already cache a map of all the stops. There's probably a cleaner way to do this.
     */
    public static void renamePatterns(
        Collection<Pattern> patterns,
        Map<String, Stop> stopById,
        Map<String, Location> locationById,
        Map<String, LocationGroupStop> locationGroupStopById,
        Map<String, LocationGroup> locationGroupById
    ) {
        LOG.info("Generating unique names for patterns");

        Map<String, PatternNamingInfo> namingInfoForRoute = new HashMap<>();

        for (Pattern pattern : patterns) {
            if (pattern.associatedTrips.isEmpty() || pattern.orderedHalts.isEmpty()) continue;

            // Each pattern within a route has a unique name (within that route, not across the entire feed)

            PatternNamingInfo namingInfo = namingInfoForRoute.get(pattern.route_id);
            if (namingInfo == null) {
                namingInfo = new PatternNamingInfo();
                namingInfoForRoute.put(pattern.route_id, namingInfo);
            }

            // Pattern names are built using stop names rather than stop IDs.
            // Stop names, unlike IDs, are not guaranteed to be unique.
            // Therefore we must track used names carefully to avoid duplicates.

            String fromName = getTerminusName(pattern, stopById, locationById, locationGroupStopById, locationGroupById, true);
            String toName = getTerminusName(pattern, stopById, locationById, locationGroupStopById, locationGroupById, false);

            namingInfo.fromStops.put(fromName, pattern);
            namingInfo.toStops.put(toName, pattern);

            for (String stopId : pattern.orderedHalts) {
                Stop stop = stopById.get(stopId);
                // If the stop doesn't exist, it's probably a location or location group stop and can be ignored for renaming.
                if (stop == null || fromName.equals(stop.stop_name) || toName.equals(stop.stop_name)) continue;
                namingInfo.vias.put(stop.stop_name, pattern);
            }
            namingInfo.patternsOnRoute.add(pattern);
        }

        // name the patterns on each route
        for (PatternNamingInfo info : namingInfoForRoute.values()) {
            for (Pattern pattern : info.patternsOnRoute) {
                pattern.name = null; // clear this now so we don't get confused later on
                String fromName = getTerminusName(pattern, stopById, locationById, locationGroupStopById, locationGroupById, true);
                String toName = getTerminusName(pattern, stopById, locationById, locationGroupStopById, locationGroupById, false);

                // check if combination from, to is unique
                Set<Pattern> intersection = new HashSet<>(info.fromStops.get(fromName));
                intersection.retainAll(info.toStops.get(toName));

                if (intersection.size() == 1) {
                    pattern.name = String.format(Locale.US, "from %s to %s", fromName, toName);
                    continue;
                }

                // check for unique via stop
                pattern.orderedHalts.stream()
                    .map(haltId -> getStopType(haltId, stopById, locationById, locationGroupStopById))
                    .forEach(entity -> {
                        Set<Pattern> viaIntersection = new HashSet<>(intersection);
                        String stopName = getStopName(entity, locationGroupById);
                        viaIntersection.retainAll(info.vias.get(stopName));
                        if (viaIntersection.size() == 1) {
                            pattern.name = String.format(Locale.US, "from %s to %s via %s", fromName, toName, stopName);
                        }
                    });

                if (pattern.name == null) {
                    // no unique via, one pattern is subset of other.
                    if (intersection.size() == 2) {
                        Iterator<Pattern> it = intersection.iterator();
                        Pattern p0 = it.next();
                        Pattern p1 = it.next();
                        if (p0.orderedHalts.size() > p1.orderedHalts.size()) {
                            p1.name = String.format(Locale.US, "from %s to %s express", fromName, toName);
                            p0.name = String.format(Locale.US, "from %s to %s local", fromName, toName);
                        } else if (p1.orderedHalts.size() > p0.orderedHalts.size()){
                            p0.name = String.format(Locale.US, "from %s to %s express", fromName, toName);
                            p1.name = String.format(Locale.US, "from %s to %s local", fromName, toName);
                        }
                    }
                }

                if (pattern.name == null) {
                    // give up
                    pattern.name = String.format(Locale.US, "from %s to %s like trip %s", fromName, toName, pattern.associatedTrips.get(0));
                }
            }

            // attach a stop and trip count to each
            for (Pattern pattern : info.patternsOnRoute) {
                pattern.name = String.format(Locale.US, "%s stops %s (%s trips)",
                        pattern.orderedHalts.size(), pattern.name, pattern.associatedTrips.size());
            }
        }
    }

    /**
     * Using the ordered stop or location id, return the object it actually relates to. Under flex, a stop can either be a
     * stop, location or location group stop, this method decides which.
     */
    private static Object getStopType(
        String orderedHaltId,
        Map<String, Stop> stopById,
        Map<String, Location> locationById,
        Map<String, LocationGroupStop> locationGroupStopById
    ) {
        Object stop = stopById.get(orderedHaltId);
        Object location = locationById.get(orderedHaltId);
        Object locationGroupStop = locationGroupStopById.get(orderedHaltId);
        if (stop != null) {
            return stop;
        } else if (location != null) {
            return location;
        } else {
            return locationGroupStop;
        }
    }

    /**
     * Extract the 'stop name' from either a stop, location or location group stop depending on the entity type.
     */
    private static String getStopName(Object entity, Map<String, LocationGroup> locationGroupById) {
        if (entity != null) {
            if (entity instanceof Stop) {
                return ((Stop) entity).stop_name;
            } else if (entity instanceof Location) {
                return ((Location) entity).stop_name;
            } else if (entity instanceof LocationGroupStop) {
                LocationGroupStop locationGroupStop = (LocationGroupStop) entity;
                LocationGroup locationGroup = locationGroupById.get(locationGroupStop.location_group_id);
                if (locationGroup != null) {
                    return locationGroup.location_group_name;
                }
            }
        }
        return "stopNameUnknown";
    }

    /**
     * Return either the 'from' or 'to' terminus name. Check the stops followed by locations and then location group
     * stops. If a match is found return the name (or id if this is no available). If there are no matches return the
     * default value.
     */
    private static String getTerminusName(
        Pattern pattern,
        Map<String, Stop> stopById,
        Map<String, Location> locationById,
        Map<String, LocationGroupStop> locationGroupStopById,
        Map<String, LocationGroup> locationGroupById,
        boolean isFrom
    ) {
        int id = isFrom ? 0 : pattern.orderedHalts.size() - 1;
        String haltId = pattern.orderedHalts.get(id);
        if (stopById.containsKey(haltId)) {
            Stop stop = stopById.get(haltId);
            return stop.stop_name != null ? stop.stop_name : stop.stop_id;
        } else if (locationById.containsKey(haltId)) {
            Location location = locationById.get(haltId);
            return location.stop_name != null ? location.stop_name : location.location_id;
        } else if (locationGroupStopById.containsKey(haltId)) {
            LocationGroup locationGroup = locationGroupById.get(haltId);
            return locationGroup.location_group_name != null ? locationGroup.location_group_name : locationGroup.location_group_id;
        }
        return isFrom ? "fromTerminusNameUnknown" : "toTerminusNameUnknown";
    }

    /**
     * Holds information about all pattern names on a particular route,
     * modeled on https://github.com/opentripplanner/OpenTripPlanner/blob/master/src/main/java/org/opentripplanner/routing/edgetype/TripPattern.java#L379
     */
    private static class PatternNamingInfo {
        // These are all maps from ?
        // FIXME For type safety and clarity maybe we should have a parameterized ID type, i.e. EntityId<Stop> stopId.
        Multimap<String, Pattern> fromStops = HashMultimap.create();
        Multimap<String, Pattern> toStops = HashMultimap.create();
        Multimap<String, Pattern> vias = HashMultimap.create();
        List<Pattern> patternsOnRoute = new ArrayList<>();
    }

}
