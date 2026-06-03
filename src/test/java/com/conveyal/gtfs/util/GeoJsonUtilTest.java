package com.conveyal.gtfs.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class GeoJsonUtilTest {
    @Test
    void createCsvRowWithCommasInValues() {
        String[] fields = new String[] {"column1", "column2, with commas, and more", "column 3"};
        assertEquals(
            String.format("column1,\"column2, with commas, and more\",column 3%n"),
            GeoJsonUtil.createCSVRow(fields)
        );
    }
}
