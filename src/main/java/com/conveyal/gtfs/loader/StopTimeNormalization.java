package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.model.Entity;
import com.conveyal.gtfs.model.PatternHalt;
import com.conveyal.gtfs.model.PatternStop;
import com.google.common.collect.Iterators;
import org.apache.commons.dbutils.DbUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.conveyal.gtfs.loader.JdbcTableWriter.getValueForId;

public class StopTimeNormalization {
    private static final Logger LOG = LoggerFactory.getLogger(StopTimeNormalization.class);
    final DataSource dataSource;
    final String tablePrefix;
    final Connection connection;
    final StatementTracker flexStatementTracker;
    final StatementTracker normalStatementTracker;

    StopTimeNormalization(DataSource dataSource, Connection connection, String tablePrefix) throws SQLException {
        this.dataSource = dataSource;
        // The connection already established is required so that the state of uncommitted changes are considered.
        this.connection = connection;
        this.tablePrefix = tablePrefix;
        this.flexStatementTracker = new StatementTracker(true);
        this.normalStatementTracker = new StatementTracker(false);
    }

    /**
     * For a given pattern id and starting stop sequence (inclusive), normalize all stop times to match the pattern
     * stops' travel times.
     *
     * @return number of stop times updated.
     */
    public int normalizeStopTimesForPattern(int beginWithSequence, String patternId) throws SQLException {
        JDBCTableReader<PatternStop> patternStops = new JDBCTableReader(
            Table.PATTERN_STOP,
            dataSource,
            tablePrefix + ".",
            EntityPopulator.PATTERN_STOP
        );
        List<PatternHalt> patternHaltsToNormalize = new ArrayList<>();
        Iterator<PatternHalt> patternHalts = Iterators.concat(
            patternStops.getOrdered(patternId).iterator()
        );
        while (patternHalts.hasNext()) {
            PatternHalt patternHalt = patternHalts.next();
            if (patternHalt.stop_sequence >= beginWithSequence) {
                patternHaltsToNormalize.add(patternHalt);
            }
        }
        // Use PatternHalt superclass to extract shared fields to be able to compare stops and locations.
        patternHaltsToNormalize = patternHaltsToNormalize
            .stream()
            .sorted(Comparator.comparingInt(patternHalt -> patternHalt.stop_sequence))
            .collect(Collectors.toList());
        PatternHalt firstPatternHalt = patternHaltsToNormalize.iterator().next();
        int firstStopSequence = firstPatternHalt.stop_sequence;
        Map<String, Integer> timesForTripIds = getPreviousTravelTimes(firstStopSequence, firstPatternHalt.pattern_id);

        for (Map.Entry<String, Integer> timesForTripId : timesForTripIds.entrySet()) {
            // Initialize travel time with previous stop time value.
            int cumulativeTravelTime = timesForTripId.getValue();
            for (PatternHalt patternHalt : patternHaltsToNormalize) {
                cumulativeTravelTime += cumulateTravelTime(cumulativeTravelTime, patternHalt, timesForTripId.getKey());
            }
        }
        return executeAllStatementTrackers();
    }

    private Map<String, Integer> getPreviousTravelTimes(int firstStopSequence, String patternId) throws SQLException {
        Map<String, Integer> timesForTripIds = new HashMap<>();
        String timeField = firstStopSequence > 0 ? "departure_time" : "arrival_time";
        // Prepare SQL query to determine the time that should form the basis for adding the travel time values.
        int previousStopSequence = firstStopSequence > 0 ? firstStopSequence - 1 : 0;
        String getPrevTravelTimeSql = String.format(
            "select t.trip_id, %s from %s.stop_times st, %s.trips t where stop_sequence = ? " +
                "and t.pattern_id = ? " +
                "and t.trip_id = st.trip_id",
            timeField,
            tablePrefix,
            tablePrefix
        );
        try (PreparedStatement statement = connection.prepareStatement(getPrevTravelTimeSql)) {
            statement.setInt(1, previousStopSequence);
            statement.setString(2, patternId);
            LOG.info("Get previous travel time sql: {}", statement);
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                timesForTripIds.put(resultSet.getString(1), resultSet.getInt(2));
            }
        }
        return timesForTripIds;
    }

    /**
     * This MUST be called _after_ pattern reconciliation has happened. The pattern stops must be processed based on
     * stop sequence so the correct cumulative travel time is calculated.
     */
    public void updatePatternFrequencies(PatternReconciliation reconciliation) throws SQLException {
        // Convert to generic stops to order pattern stops/locations by stop sequence.
        List<PatternReconciliation.GenericStop> genericStops = reconciliation.getGenericStops();
        int cumulativeTravelTime = 0;
        for (PatternReconciliation.GenericStop genericStop : genericStops) {
            // Update stop times linked to pattern stop and accumulate time.
            // Default travel and dwell time behave as "linked fields" for associated stop times. In other
            // words, frequency trips in the editor must match the pattern stop travel times.
            PatternHalt patternHalt = reconciliation.getPatternStop(genericStop.referenceId);
            cumulativeTravelTime += cumulateTravelTime(cumulativeTravelTime, patternHalt, null);
        }
        executeAllStatementTrackers();
    }

    /**
     * Cumulate travel time based on generic pattern halt values and update the appropriate stop times.
     */
    private int cumulateTravelTime(int cumulativeTravelTime, PatternHalt patternHalt, String tripId) throws SQLException {
        int travelTime = patternHalt.getTravelTime();
        int dwellTime = patternHalt.getDwellTime();
        updateStopTimes(
            cumulativeTravelTime,
            travelTime,
            dwellTime,
            patternHalt.pattern_id,
            patternHalt.stop_sequence,
            tripId,
            patternHalt.isFlex()
        );
        return travelTime + dwellTime;
    }

    private int executeAllStatementTrackers() throws SQLException {
        int stopTimesUpdated =
            flexStatementTracker.executeRemaining() +
            normalStatementTracker.executeRemaining();
        LOG.info("Updated {} stop times.", stopTimesUpdated);
        return stopTimesUpdated;
    }

    /**
     * Update stop time values depending on flex. If not flex, update the arrival_time and departure_time. If flex,
     * update the start_pickup_drop_off_window and end_pickup_drop_off_window.
     */
    private void updateStopTimes(
        int previousTravelTime,
        int travelTime,
        int dwellTime,
        String patternId,
        int stopSequence,
        String tripId,
        boolean isFlex
    ) throws SQLException {

        PreparedStatement statement = getPreparedStatement(isFlex, tripId);
        BatchTracker batchTracker = getBatchTracker(isFlex, tripId);
        int oneBasedIndex = 1;
        int arrivalTime = previousTravelTime + travelTime;
        statement.setInt(oneBasedIndex++, arrivalTime);
        statement.setInt(oneBasedIndex++, arrivalTime + dwellTime);
        if (tripId != null) statement.setString(oneBasedIndex++, tripId);

        // Set "where clause" with value for pattern_id and stop_sequence
        statement.setString(oneBasedIndex++, patternId);
        // In the editor, we can depend on stop_times#stop_sequence matching pattern_stop/pattern_locations#stop_sequence
        // because we normalize stop sequence values for stop times during snapshotting for the editor.
        statement.setInt(oneBasedIndex, stopSequence);
        // Log query, execute statement, and log result.
        LOG.debug("Adding statement {} to tracker {}", statement, batchTracker);
        batchTracker.addBatch();
    }

    private PreparedStatement getPreparedStatement(boolean isFlex, String tripId) {
        return (isFlex)
            ? flexStatementTracker.getPreparedStatement(tripId)
            : normalStatementTracker.getPreparedStatement(tripId);
    }

    private BatchTracker getBatchTracker(boolean isFlex, String tripId) {
        return (isFlex)
            ? flexStatementTracker.getBatchTracker(tripId)
            : normalStatementTracker.getBatchTracker(tripId);
    }

    /**
     * Class used to pair prepared statements with batch trackers. It also defines prepared statements based on flex
     * and number of trips.
     */
    private class StatementTracker {
        final BatchTracker singleTripBatchTracker;

        final PreparedStatement singleTripPreparedStatement;

        final BatchTracker allTripsBatchTracker;

        final PreparedStatement allTripsPreparedStatement;

        public StatementTracker(boolean isFlex) throws SQLException {
            // Match the prepared statements with batch trackers.
            String recordTypePrefix = (isFlex) ? "Flex stop" : "Normal stop";
            String sqlForSingleTrip = (isFlex) ? getFlexStopSql(true) : getNormalStopSql(true);
            String sqlForAllTrips = (isFlex) ? getFlexStopSql(false) : getNormalStopSql(false);
            singleTripPreparedStatement = connection.prepareStatement(sqlForSingleTrip);
            singleTripBatchTracker = new BatchTracker(recordTypePrefix + ", single trip", singleTripPreparedStatement);
            allTripsPreparedStatement = connection.prepareStatement(sqlForAllTrips);
            allTripsBatchTracker = new BatchTracker(recordTypePrefix + ", all trips", allTripsPreparedStatement);
        }

        private String getNormalStopSql(boolean isSingleTrip) {
            return String.format(
                "update %s.stop_times st set arrival_time = ?, departure_time = ? from %s.trips t " +
                    "where st.trip_id = %s AND t.pattern_id = ? AND st.stop_sequence = ?",
                tablePrefix,
                tablePrefix,
                getTripIdReference(isSingleTrip)
            );
        }

        private String getFlexStopSql(boolean isSingleTrip) {
            return String.format(
                "update %s.stop_times st set start_pickup_drop_off_window = ?, end_pickup_drop_off_window = ? from %s.trips t " +
                    "where st.trip_id = %s AND t.pattern_id = ? AND st.stop_sequence = ?",
                tablePrefix,
                tablePrefix,
                getTripIdReference(isSingleTrip)
            );
        }

        private String getTripIdReference(boolean isSingleTrip) {
            return (isSingleTrip) ? "?" : "t.trip_id";
        }

        private PreparedStatement getPreparedStatement(String tripId) {
            return (tripId != null) ? singleTripPreparedStatement : allTripsPreparedStatement;
        }

        private BatchTracker getBatchTracker(String tripId) {
            return (tripId != null) ? singleTripBatchTracker : allTripsBatchTracker;
        }

        /**
         * Execute remaining prepared statements in batches.
         */
        public int executeRemaining() throws SQLException{
            return singleTripBatchTracker.executeRemaining() + allTripsBatchTracker.executeRemaining();
        }
    }

    /**
     * For a given pattern id and starting stop sequence (inclusive), normalize all stop times to match the pattern
     * stops' travel times.
     *
     * @return number of stop times updated
     */
    public int normalizeStopTimesForPattern(int id, int beginWithSequence, boolean interpolateStopTimes, boolean ignoreNonBlankStopTimes) throws SQLException {
        try {
            JDBCTableReader<PatternStop> patternStops = new JDBCTableReader(
                Table.PATTERN_STOP,
                dataSource,
                tablePrefix + ".",
                EntityPopulator.PATTERN_STOP
            );
            String patternId = getValueForId(id, "pattern_id", tablePrefix, Table.PATTERNS, connection);
            List<PatternStop> patternStopsToNormalize = new ArrayList<>();
            for (PatternStop patternStop : patternStops.getOrdered(patternId)) {
                // Update stop times for any pattern stop with matching stop sequence (or for all pattern stops if the list
                // is null).
                if (patternStop.stop_sequence >= beginWithSequence) {
                    patternStopsToNormalize.add(patternStop);
                }
            }
            int stopTimesUpdated = updateStopTimesForPatternStops(patternStopsToNormalize, interpolateStopTimes, ignoreNonBlankStopTimes);
            connection.commit();
            return stopTimesUpdated;
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            DbUtils.closeQuietly(connection);
        }
    }

    private boolean checkIfArrivalTimeExists(int stopSequence, String trip_id) throws SQLException {
        String sql = String.format("select arrival_time from %s.stop_times where stop_sequence = ? and trip_id = ?", tablePrefix);
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setInt(1, stopSequence);
        statement.setString(2, trip_id);
        try (ResultSet rs = statement.executeQuery()) {
            if (!rs.next()) {
                return false;
            }
            int arrivalTime = rs.getInt(1);
            if (rs.wasNull()) {
                return false;
            }
            return arrivalTime > 0;
        }

    }

    /**
     * Normalizes all stop times' arrivals and departures for an ordered set of pattern stops. This set can be the full
     * set of stops for a pattern or just a subset. Typical usage for this method would be to overwrite the arrival and
     * departure times for existing trips after a pattern stop has been added or inserted into a pattern or if a
     * pattern stop's default travel or dwell time were updated and the stop times need to reflect this update.
     *
     * @param patternStops list of pattern stops for which to update stop times (ordered by increasing stop_sequence)
     * @throws SQLException
     *
     */
    private int updateStopTimesForPatternStops(
        List<PatternStop> patternStops,
        boolean interpolateStopTimes,
        boolean ignoreNonBlankStopTimes
    ) throws SQLException {
        PatternStop firstPatternStop = patternStops.iterator().next();
        List<PatternStop> timepoints = patternStops.stream().filter(ps -> ps.timepoint == 1).collect(Collectors.toList());
        int firstStopSequence = firstPatternStop.stop_sequence;
        // Prepare SQL query to determine the time that should form the basis for adding the travel time values.
        int previousStopSequence = firstStopSequence > 0 ? firstStopSequence - 1 : 0;
        String timeField = firstStopSequence > 0 ? "departure_time" : "arrival_time";
        String getFirstTravelTimeSql = String.format(
            "select t.trip_id, %s from %s.stop_times st, %s.trips t where stop_sequence = ? " +
                "and t.pattern_id = ? " +
                "and t.trip_id = st.trip_id",
            timeField,
            tablePrefix,
            tablePrefix
        );
        PreparedStatement statement = connection.prepareStatement(getFirstTravelTimeSql);
        statement.setInt(1, previousStopSequence);
        statement.setString(2, firstPatternStop.pattern_id);
        LOG.info(statement.toString());
        ResultSet resultSet = statement.executeQuery();
        Map<String, Integer> timesForTripIds = new HashMap<>();
        while (resultSet.next()) {
            timesForTripIds.put(resultSet.getString(1), resultSet.getInt(2));
        }
        // Update stop times for individual trips with normalized travel times.
        String updateTravelTimeSql = String.format(
            "update %s.stop_times set arrival_time = ?, departure_time = ? where trip_id = ? and stop_sequence = ?",
            tablePrefix
        );
        PreparedStatement updateStopTimeStatement = connection.prepareStatement(updateTravelTimeSql);
        LOG.info(updateStopTimeStatement.toString());
        final BatchTracker stopTimesTracker = new BatchTracker("stop_times", updateStopTimeStatement);
        for (String tripId : timesForTripIds.keySet()) {
            // Initialize travel time with previous stop time value.
            int cumulativeTravelTime = timesForTripIds.get(tripId);
            int timepointCumulativeTravelTime = cumulativeTravelTime;
            int timepointNumber = 0;
            PatternStop prevPatternStop = null;
            for (PatternStop patternStop : patternStops) {
                boolean isTimepoint = patternStop.timepoint == 1;
                if (isTimepoint) timepointNumber++;
                boolean hasStopTime = !ignoreNonBlankStopTimes || checkIfArrivalTimeExists(patternStop.stop_sequence, tripId);
                // Gather travel/dwell time for pattern stop (being sure to check for missing values).
                int travelTime = patternStop.default_travel_time == Entity.INT_MISSING ? 0 : patternStop.default_travel_time;
                if (interpolateStopTimes || ignoreNonBlankStopTimes) {
                    if (patternStop.shape_dist_traveled == Entity.DOUBLE_MISSING) {
                        throw new IllegalStateException("Shape_dist_traveled must be defined for all stops in order to perform interpolation");
                    }
                    // Override travel time if we're interpolating between timepoints, or generating blank stop times
                    if ((!ignoreNonBlankStopTimes && !isTimepoint) || (ignoreNonBlankStopTimes && !hasStopTime && patternStop.stop_sequence != 0)) {
                        travelTime = interpolateTimesFromTimepoints(patternStop, timepoints, timepointNumber, prevPatternStop);
                    }
                }
                // When interpolating, assume that *all* non-timepoint data is incorrect/missing
                // The travel and dwell times defined for stops between timepoints will be ignored.
                int dwellTime = patternStop.default_dwell_time == Entity.INT_MISSING || (interpolateStopTimes && !isTimepoint) ?
                    0 : patternStop.default_dwell_time;

                // In non-blank stop time mode, only update if our stop time is blank
                if (ignoreNonBlankStopTimes) {
                    if (hasStopTime) {
                        cumulativeTravelTime += travelTime + dwellTime;
                        prevPatternStop = patternStop;
                        continue;
                    }

                    updateStopTimeStatement.setInt(1, cumulativeTravelTime + travelTime);
                    updateStopTimeStatement.setInt(2, cumulativeTravelTime + travelTime);
                    updateStopTimeStatement.setString(3, tripId);
                    updateStopTimeStatement.setInt(4, patternStop.stop_sequence);
                    stopTimesTracker.addBatch();
                }


                // In interpolation mode, don't write changes to db for timepoints
                // We don't write the travel time, since it is interpolated
                if (interpolateStopTimes && isTimepoint) {
                    timepointCumulativeTravelTime += travelTime + dwellTime;
                    // Reset the cumulativeTravelTime to the schedule truth
                    cumulativeTravelTime = timepointCumulativeTravelTime;
                    prevPatternStop = patternStop;
                    continue;
                }
                // Increase travel time by current pattern stop's travel and dwell times (and set values for update).
                cumulativeTravelTime += travelTime;
                updateStopTimeStatement.setInt(1, cumulativeTravelTime);
                cumulativeTravelTime += dwellTime;
                updateStopTimeStatement.setInt(2, cumulativeTravelTime);
                updateStopTimeStatement.setString(3, tripId);
                updateStopTimeStatement.setInt(4, patternStop.stop_sequence);
                stopTimesTracker.addBatch();

                prevPatternStop = patternStop;
            }
        }
        return stopTimesTracker.executeRemaining();
    }

    /**
     * Updates the non-timepoint stop times between two timepoints using the speed implied  by
     * the travel time between them. Ignores any existing default_travel_time or default_dwell_time
     * entered for the non-timepoint stops.
     */
    private int interpolateTimesFromTimepoints(
        PatternStop patternStop,
        List<PatternStop> timepoints,
        Integer timepointNumber,
        PatternStop prevPatternStop
    ) {
        if (timepointNumber == 0 || timepoints.size() == 1 || timepointNumber >= timepoints.size()) {
            throw new IllegalStateException("Issue in pattern stops which prevents interpolation (e.g. less than 2 stoptimes to interpolate between)");
        }
        PatternStop nextTimepoint = timepoints.get(timepointNumber);
        PatternStop prevTimepoint = timepoints.get(timepointNumber - 1);

        if (
            nextTimepoint == null ||
                nextTimepoint.default_travel_time == Entity.INT_MISSING ||
                nextTimepoint.shape_dist_traveled == Entity.DOUBLE_MISSING ||
                prevTimepoint.shape_dist_traveled == Entity.DOUBLE_MISSING ||
                (prevPatternStop != null && prevPatternStop.shape_dist_traveled == Entity.DOUBLE_MISSING)
        ) {
            throw new IllegalStateException("Error with stop time interpolation: timepoint or shape_dist_traveled is null");
        }

        double prevSdt = prevPatternStop == null ? 0 : prevPatternStop.shape_dist_traveled;

        double timepointSpeed = (nextTimepoint.shape_dist_traveled - prevTimepoint.shape_dist_traveled) / nextTimepoint.default_travel_time;
        return (int) Math.round((patternStop.shape_dist_traveled - prevSdt) / timepointSpeed);
    }
}
