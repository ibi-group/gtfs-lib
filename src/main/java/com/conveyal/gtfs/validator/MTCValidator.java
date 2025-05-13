package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.error.SQLErrorStorage;
import com.conveyal.gtfs.loader.Feed;
import com.conveyal.gtfs.model.*;
import org.apache.commons.lang3.StringUtils;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import static com.conveyal.gtfs.error.NewGTFSErrorType.FIELD_VALUE_TOO_LONG;
import static com.conveyal.gtfs.error.NewGTFSErrorType.MISSING_STOP_CODE_PREFIX;

/**
 * MTCValidator runs a set of custom validation checks for GTFS feeds managed by MTC in Data Tools.
 * The checks consist of validating field lengths at this time per the 511 MTC guidelines at
 * https://github.com/ibi-group/datatools-ui/files/4438625/511.Transit_Data.Guidelines_V2.0_3-27-2020.pdf.
 * For specific field lengths, search the guidelines for the word 'character'.
 *
 * Note that other validations, e.g. on GTFS+ files, are discussed in
 * https://github.com/ibi-group/datatools-ui/issues/544.
 */
public class MTCValidator extends FeedValidator {
    private String primaryStopCodePrefix;
    private List<String> secondaryStopCodePrefixes = new ArrayList<>();
    private boolean hasStopPrefixes = false;

    public MTCValidator(Feed feed, SQLErrorStorage errorStorage) {
        super(feed, errorStorage);
    }

    public MTCValidator(
        Feed feed,
        SQLErrorStorage errorStorage,
        String primaryStopPrefix,
        List<String> secondaryStopPrefixes
    ) {
        super(feed, errorStorage);
        hasStopPrefixes = true;
        this.primaryStopCodePrefix = primaryStopPrefix;
        this.secondaryStopCodePrefixes = secondaryStopPrefixes;
    }

    @Override
    public void validate() {
        for (Agency agency : feed.agencies) {
            validateFieldLength(agency, agency.agency_id, 50);
            validateFieldLength(agency, agency.agency_name, 50);
            validateFieldLength(agency, agency.agency_url, 500);
        }

        for (Stop stop : feed.stops) {
            validateFieldLength(stop, stop.stop_name, 100);
        }

        for (Trip trip : feed.trips) {
            validateFieldLength(trip, trip.trip_headsign, 120);
            validateFieldLength(trip, trip.trip_short_name, 50);
        }

        if (hasStopPrefixes) {
            for (Stop stop : feed.stops) {
                validateStopCodePrefix(stop, stop.stop_code);
            }
        }
    }

    /**
     * Checks that the length of a string does not exceed a certain length.
     * Reports an error if the length is exceeded.
     * @param entity The containing GTFS entity (for error reporting purposes).
     * @param value The String value to check.
     * @param maxLength The length to check, should be positive or zero.
     * @return true if value.length() is maxLength or less, or if value is null; false otherwise.
     */
    public boolean validateFieldLength(Entity entity, String value, int maxLength) {
        if (value != null && value.length() > maxLength) {
            if (errorStorage != null) registerError(entity, FIELD_VALUE_TOO_LONG, "[over " + maxLength + " characters] " + value);
            return false;
        }
        return true;
    }

    public boolean validateFieldLength(Entity entity, URL url, int maxLength) {
        return validateFieldLength(entity, url != null ? url.toString() : "", maxLength);
    }

    /**
     * Checks that the stop code contains primary and secondary prefixes.
     * @param entity The containing GTFS entity (for error reporting purposes).
     * @param stopCode The stop code to check for prefix.
     * @return true if the stop code contains expected prefix or validation is skipped; false otherwise.
     */
    public boolean validateStopCodePrefix(Entity entity, String stopCode) {
        if (stopCode == null || (primaryStopCodePrefix == null && secondaryStopCodePrefixes == null)) {
            return true;
        }

        boolean hasPrefix = false;
        if (!StringUtils.isBlank(primaryStopCodePrefix)) {
            hasPrefix = stopCode.startsWith(primaryStopCodePrefix);
        }
        if (!hasPrefix && secondaryStopCodePrefixes != null && !secondaryStopCodePrefixes.isEmpty()) {
            hasPrefix = secondaryStopCodePrefixes
                .stream()
                .anyMatch(stopCode::startsWith);
        }
        if (!hasPrefix && errorStorage != null) {
            registerError(entity, MISSING_STOP_CODE_PREFIX, stopCode);
        }

        return hasPrefix;
    }
}