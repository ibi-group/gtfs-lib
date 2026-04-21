package com.conveyal.gtfs;

import com.conveyal.gtfs.error.NewGTFSErrorType;
import com.csvreader.CsvReader;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.BOMInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

import static com.conveyal.gtfs.util.Util.randomIdString;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestUtils {

    private static final Logger LOG = LoggerFactory.getLogger(TestUtils.class);
    private static final String PG_URL = "jdbc:postgresql://localhost/postgres";
    public static final String PG_TEST_USER = "postgres";
    public static final String PG_TEST_PASSWORD = "postgres";

    public static final String JDBC_URL = "jdbc:postgresql://localhost";

    static final String TEST_RESOURCE_PATH = "src/test/resources/";

    /**
     * Forcefully drops a database even if other users are connected to it.
     *
     * @param dbName
     */
    public static void dropDB(String dbName) {
        // first, terminate all other user sessions
        executeAndClose(String.format("SELECT pg_terminate_backend(pg_stat_activity.pid) " +
            "FROM pg_stat_activity " +
            "WHERE pg_stat_activity.datname = '%s' " +
            "AND pid <> pg_backend_pid()", dbName
        ));
        // drop the db
        if(executeAndClose(String.format("DROP DATABASE %s", dbName))) {
            LOG.info(String.format("Successfully dropped database: %s", dbName));
        } else {
            LOG.error(String.format("Failed to drop database: %s", dbName));
        }
    }

    /**
     * Boilerplate for opening a connection, executing a statement and closing connection.
     *
     * @param statement
     * @return true if everything worked.
     */
    private static boolean executeAndClose(String statement) {
        Connection connection;
        try {
            // This connection must be established without GTFS#createDataSource because the "create database" command
            // cannot run inside a transaction block.
            connection = DriverManager.getConnection(PG_URL, PG_TEST_USER, PG_TEST_PASSWORD);
        } catch (SQLException e) {
            e.printStackTrace();
            LOG.error("Error connecting to database!");
            return false;
        }

        try {
            LOG.info(statement);
            connection.prepareStatement(statement).execute();
        } catch (SQLException e) {
            e.printStackTrace();
            LOG.error(String.format("Error executing statement: %s", statement));
            return false;
        }

        try {
            connection.close();
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
            LOG.error("Error closing connection!");
            return false;
        }
    }

    public static DataSource createTestDataSource (String dbUrl) {
        return GTFS.createDataSource(dbUrl, PG_TEST_USER, PG_TEST_PASSWORD);
    }

    /**
     * Generate a new database for isolating a test.
     *
     * @return The name of the name database, or null if creation unsuccessful
     */
    public static String generateNewDB() {
        String newDBName = String.format("test_db_%s", randomIdString());
        if (executeAndClose(String.format("CREATE DATABASE %s", newDBName))) {
            return newDBName;
        } else {
            return null;
        }
    }

    /**
     * Helper to return the relative path to a test resource file.
     *
     * @param fileName
     * @return
     */
    public static String getResourceFileName(String fileName) {
        return String.format("./src/test/resources/%s", fileName);
    }

    public static String getTestResourceAsString(String resourcePathName) throws IOException {
        return getFileContents(TEST_RESOURCE_PATH + resourcePathName);
    }

    /**
     * Extract the file contents from the provided path and file name.
     */
    public static String getFileContents(String pathAndFileName) throws IOException {
        FileInputStream fileInputStream = new FileInputStream(pathAndFileName);
        return IOUtils.toString(fileInputStream, StandardCharsets.UTF_8);
    }


    /**
     * Zip files in a folder into a temporary zip file
     */
    public static String zipFolderFiles(String folderName, boolean isRelativeToResourcesPath) throws IOException {
        // create temporary zip file
        File tempFile = File.createTempFile("temp-gtfs-zip-", ".zip");
        tempFile.deleteOnExit();
        String tempFilePath = tempFile.getAbsolutePath();
        // If folder name is relative to resources path, get full path. Otherwise, it is assumed to be an absolute path.
        String folderPath = isRelativeToResourcesPath ? getResourceFileName(folderName) : folderName;
        // Do not nest files under a subdirectory if directly zipping a folder in src/main/resources
        compressZipfile(folderPath, tempFilePath, !isRelativeToResourcesPath);
        return tempFilePath;
    }

    private static void compressZipfile(String sourceDir, String outputFile, boolean nestDirectory) throws IOException {
        try (ZipOutputStream zipFile = new ZipOutputStream(new FileOutputStream(outputFile))) {
            compressDirectoryToZipfile(sourceDir, sourceDir, zipFile, nestDirectory);
        }
    }

    /**
     * Convenience method for zipping a directory.
     * @param nestDirectory whether nested folders should be preserved as subdirectories
     */
    private static void compressDirectoryToZipfile(String rootDir, String sourceDir, ZipOutputStream out, boolean nestDirectory) throws IOException {
        for (File file : new File(sourceDir).listFiles()) {
            if (file.isDirectory()) {
                compressDirectoryToZipfile(rootDir, fileNameWithDir(sourceDir, file.getName()), out, nestDirectory);
            } else {
                String folderName = sourceDir.replace(rootDir, "");
                String zipEntryName = nestDirectory
                    ? fileNameWithDir(folderName, file.getName())
                    : String.join("", folderName, file.getName());
                ZipEntry entry = new ZipEntry(zipEntryName);
                out.putNextEntry(entry);

                FileInputStream in = new FileInputStream(file.getAbsolutePath());
                IOUtils.copy(in, out);
                IOUtils.closeQuietly(in);
            }
        }
    }

    public static String fileNameWithDir(String directory, String filename) {
        return String.join(File.separator, directory, filename);
    }

    /**
     * Asserts that the result of a SQL count statement is equal to an expected value
     *
     * @param sql A SQL statement in the form of `SELECT count(*) FROM ...`
     * @param expectedCount The expected count that is returned from the result of the SQL statement.
     */
    public static void assertThatSqlCountQueryYieldsExpectedCount(DataSource dataSource, String sql, int expectedCount) {
        int count = -1;
        LOG.info(sql);
        // Encapsulate connection in try-with-resources to ensure it is closed and does not interfere with other tests.
        try (Connection connection = dataSource.getConnection()) {
            ResultSet resultSet = connection.prepareStatement(sql).executeQuery();
            if (resultSet.next()) {
                count = resultSet.getInt(1);
            }
        } catch (SQLException e) {
            LOG.error("SQL error encountered", e);
        }
        assertThat(
            "Records matching query should equal expected count.",
            count,
            equalTo(expectedCount)
        );
    }

    /**
     * Check that the test feed has expected number of errors for the provided values.
     */
    public static void checkFeedHasExpectedNumberOfErrors(
        String testNamespace,
        DataSource testDataSource,
        NewGTFSErrorType errorType,
        String entityType,
        String lineNumber,
        String entityId,
        String badValue,
        int expectedNumberOfErrors
    ) {
        String sql = String.format("select count(*) from %s.errors where error_type = '%s' and entity_type = '%s' and line_number = '%s'",
            testNamespace,
            errorType,
            entityType,
            lineNumber);

        if (entityId != null) sql += String.format(" and entity_id = '%s'", entityId);
        if (badValue != null) sql += String.format(" and bad_value = '%s'", badValue);

        assertThatSqlCountQueryYieldsExpectedCount(testDataSource, sql, expectedNumberOfErrors);
    }

    public static class FileTestCase {
        public String filename;
        public TestUtils.DataExpectation[] expectedColumnData;

        public FileTestCase(String filename, TestUtils.DataExpectation[] expectedColumnData) {
            this.filename = filename;
            this.expectedColumnData = expectedColumnData;
        }

        @Override
        public String toString() {
            return "FileTestCase{" +
                "filename='" + filename + '\'' +
                '}';
        }
    }

    public static class DataExpectation {
        public String columnName;
        public String expectedValue;

        public DataExpectation(String columnName, String expectedValue) {
            this.columnName = columnName;
            this.expectedValue = expectedValue;
        }
    }

    /**
     * Look through all written files and confirm that the record matching the expected row exists in appropriate file.
     */
    public static void checkFileTestCases(ZipFile zip, FileTestCase[] fileTestCases) throws IOException {
        // Look through all written files in the zip file.
        for (TestUtils.FileTestCase fileTestCase : fileTestCases) {
            checkFileTestCase(zip, fileTestCase);
        }
    }

    public static void checkFileTestCase(ZipFile zip, FileTestCase fileTestCase) throws IOException {
        ZipEntry entry = zip.getEntry(fileTestCase.filename);

        // make sure the file exists within the zip file.
        assertThat(entry, notNullValue());

        // create csv reader for file
        InputStream zis = zip.getInputStream(entry);
        InputStream bis = new BOMInputStream(zis);
        CsvReader reader = new CsvReader(bis, ',', StandardCharsets.UTF_8);

        // make sure the file has headers
        boolean hasHeaders = reader.readHeaders();
        assertTrue(hasHeaders);

        // make sure that the record matching the expected row exists in this table.
        boolean recordFound = false;
        while (reader.readRecord() && !recordFound) {
            boolean allExpectationsMetForThisRecord = true;
            for (TestUtils.DataExpectation dataExpectation : fileTestCase.expectedColumnData) {
                if (!reader.get(dataExpectation.columnName).equals(dataExpectation.expectedValue)) {
                    allExpectationsMetForThisRecord = false;
                    break;
                }
            }
            if (allExpectationsMetForThisRecord) {
                recordFound = true;
            }
        }
        assertTrue(
            recordFound,
            String.format("Data Expectation record not found in %s", fileTestCase.filename)
        );
    }

    public static void assertThatSqlQueryYieldsRowCount(DataSource dataSource, String sql, int expectedRowCount) throws SQLException {
        LOG.info(sql);
        int recordCount = 0;
        ResultSet rs = dataSource.getConnection().prepareStatement(sql).executeQuery();
        while (rs.next()) recordCount++;
        assertThat("Records matching query should equal expected count.", recordCount, equalTo(expectedRowCount));
    }

    public static void assertThatSqlQueryYieldsZeroRows(DataSource dataSource, String sql) throws SQLException {
        assertThatSqlQueryYieldsRowCount(dataSource, sql, 0);
    }

    /**
     * Helper function to export a GTFS from the database to a temporary zip file.
     */
    public static File exportGtfs(
        String namespace,
        DataSource dataSource,
        boolean fromEditor,
        boolean publishProprietaryFiles
    ) throws IOException {
        File tempFile = File.createTempFile("snapshot", ".zip");
        GTFS.export(namespace, tempFile.getAbsolutePath(), dataSource, fromEditor, publishProprietaryFiles);
        return tempFile;
    }

}
