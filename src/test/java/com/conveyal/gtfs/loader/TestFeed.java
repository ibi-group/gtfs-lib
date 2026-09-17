package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.TestUtils;

import javax.sql.DataSource;
import java.io.Closeable;

public class TestFeed implements Closeable, AutoCloseable {
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

    @Override
    public void close() {
        dropDB();
    }
}
