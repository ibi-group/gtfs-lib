package com.conveyal.gtfs.util;

import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Utility class for dealing with CSV content
 */
public class CsvUtil {

    private CsvUtil() {
        // This utility class should not be instantiated
    }

    public static String createCSVRow(String... columnData) {
        return Arrays.stream(columnData)
            .map(col -> col.contains(",") ? StringUtils.wrap(col, "\"") : col)
            .collect(Collectors.joining(",")) + System.lineSeparator();
    }
}
