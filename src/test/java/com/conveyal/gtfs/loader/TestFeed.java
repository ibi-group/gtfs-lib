package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.TestUtils;

import javax.sql.DataSource;

public class TestFeed {
    public final String fileName;
    public final String dbName;
    public final DataSource dataSource;

    public TestFeed(String fileName) {
        this.fileName = fileName;
        dbName = TestUtils.generateNewDB();
        dataSource = TestUtils.createTestDataSource(String.format("jdbc:postgresql://localhost/%s", dbName));
    }

    public void dropDB() {
        TestUtils.dropDB(dbName);
    }
}
