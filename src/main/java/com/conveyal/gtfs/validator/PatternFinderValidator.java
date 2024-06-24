package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.PatternBuilder;
import com.conveyal.gtfs.PatternFinder;
import com.conveyal.gtfs.TripPatternKey;
import com.conveyal.gtfs.error.SQLErrorStorage;
import com.conveyal.gtfs.loader.Feed;
import com.conveyal.gtfs.model.LocationGroup;
import com.conveyal.gtfs.model.Location;
import com.conveyal.gtfs.model.Pattern;
import com.conveyal.gtfs.model.Route;
import com.conveyal.gtfs.model.Stop;
import com.conveyal.gtfs.model.LocationGroupStop;
import com.conveyal.gtfs.model.StopTime;
import com.conveyal.gtfs.model.Trip;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Groups trips together into "patterns" that share the same sequence of stops.
 * This is not a normal validator in the sense that it does not check for bad data.
 * It's taking advantage of the fact that we're already iterating over the trips one by one to build up the patterns.
 */
public class PatternFinderValidator extends TripValidator {

    private static final Logger LOG = LoggerFactory.getLogger(PatternFinderValidator.class);

    PatternFinder patternFinder;
    PatternBuilder patternBuilder;

    public PatternFinderValidator(Feed feed, SQLErrorStorage errorStorage) {
        super(feed, errorStorage);
        patternFinder = new PatternFinder();
        try {
            patternBuilder = new PatternBuilder(feed);
        } catch (SQLException e) {
            throw new RuntimeException("Unable to construct pattern builder.", e);
        }
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
        // As we hit each trip, accumulate them into the wrapped PatternFinder object.
        patternFinder.processTrip(trip, stopTimes);
    }

    /**
     * Store patterns and pattern stops in the database. Also, update the trips table with a pattern_id column.
     */
    @Override
    public void complete(ValidationResult validationResult) {
        List<Pattern> patternsFromFeed = new ArrayList<>();
        for(Pattern pattern :  feed.patterns) {
            patternsFromFeed.add(pattern);
        }
        LOG.info("Finding patterns...");
        Map<String, Stop> stopById = new HashMap<>();
        Map<String, Location> locationById = new HashMap<>();
        Map<String, LocationGroupStop> locationGroupStopById = new HashMap<>();
        Map<String, LocationGroup> locationGroupById = new HashMap<>();
        for (Stop stop : feed.stops) {
            stopById.put(stop.stop_id, stop);
        }
        for (Location location : feed.locations) {
            locationById.put(location.location_id, location);
        }
        for (LocationGroupStop locationGroupStop : feed.locationGroupStops) {
            locationGroupStopById.put(locationGroupStop.location_group_id, locationGroupStop);
        }
        for (LocationGroup locationGroup : feed.locationGroups) {
            locationGroupById.put(locationGroup.location_group_id, locationGroup);
        }
        // Although patterns may have already been loaded from file, the trip patterns are still required.
        Map<TripPatternKey, Pattern> patterns = patternFinder.createPatternObjects(
            stopById,
            locationById,
            locationGroupStopById,
            locationGroupById,
            patternsFromFeed,
            errorStorage
        );
        patternBuilder.create(
            patterns,
            patternFinder.canUsePatternsFromFeed(patternsFromFeed)
        );
    }
}
