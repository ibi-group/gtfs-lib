package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.GTFSFeed;
import com.conveyal.gtfs.TestUtils;
import com.conveyal.gtfs.loader.EntityPopulator;
import com.conveyal.gtfs.loader.FeedLoadResult;
import com.conveyal.gtfs.loader.JDBCTableReader;
import com.conveyal.gtfs.loader.Table;
import com.conveyal.gtfs.model.Pattern;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static com.conveyal.gtfs.GTFS.load;
import static com.conveyal.gtfs.GTFS.validate;
import static com.conveyal.gtfs.TestUtils.getResourceFileName;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;


class PatternFinderValidatorTest {

    private static String testDBName;

    private static DataSource testDataSource;

    @BeforeAll
    public static void setUpClass() {
        // create a new database
        testDBName = TestUtils.generateNewDB();
        String dbConnectionUrl = String.format("jdbc:postgresql://localhost/%s", testDBName);
        testDataSource = TestUtils.createTestDataSource(dbConnectionUrl);
    }

    @AfterAll
    public static void tearDownClass() {
        TestUtils.dropDB(testDBName);
    }

    @Test
    void canUseFeedPatterns() throws SQLException {
        String fileName = getResourceFileName("real-world-gtfs-feeds/RABA.zip");
        FeedLoadResult feedLoadResult = load(fileName, testDataSource);
        String testNamespace = feedLoadResult.uniqueIdentifier;
        validate(testNamespace, testDataSource);
        checkPatternStopsAgainstFeedPatterns(fileName, testNamespace);
    }

    /**
     * Remove one pattern from the feed so that there is a mismatch between the patterns loaded and the patterns
     * generated. This will result in the generated patterns taking precedence over the loaded patterns.
     */
    @Test
    void canRevertToGeneratedPatterns() throws SQLException {
        String fileName = getResourceFileName("real-world-gtfs-feeds/RABA.zip");
        FeedLoadResult feedLoadResult = load(fileName, testDataSource);
        String testNamespace = feedLoadResult.uniqueIdentifier;
        String patternIdToExclude = "2k3j";
        executeSqlStatement(String.format(
            "delete from %s where pattern_id = '%s'",
            String.format("%s.%s", testNamespace, Table.PATTERNS.name),
            patternIdToExclude
        ));
        validate(testNamespace, testDataSource);
        JDBCTableReader<Pattern> patterns = new JDBCTableReader(Table.PATTERNS,
            testDataSource,
            testNamespace + ".",
            EntityPopulator.PATTERN
        );
        for (Pattern pattern : patterns.getAllOrdered()) {
            assertThatSqlQueryYieldsRowCountGreaterThanZero(generateSql(testNamespace, pattern.pattern_id));
        }
    }

    @Test
    void canUseGeneratedPatterns() throws SQLException, IOException {
        String zipFileName = TestUtils.zipFolderFiles("fake-agency", true);
        FeedLoadResult feedLoadResult = load(zipFileName, testDataSource);
        String testNamespace = feedLoadResult.uniqueIdentifier;
        validate(testNamespace, testDataSource);
        checkPatternStopsAgainstFeedPatterns(zipFileName, testNamespace);
    }

    private void checkPatternStopsAgainstFeedPatterns(String zipFileName, String testNamespace) throws SQLException {
        GTFSFeed feed = GTFSFeed.fromFile(zipFileName);
        for (String key : feed.patterns.keySet()) {
            Pattern pattern = feed.patterns.get(key);
            assertThatSqlQueryYieldsRowCountGreaterThanZero(generateSql(testNamespace, pattern.pattern_id));
        }
    }

    private String generateSql(String testNamespace, String patternId) {
        return String.format(
            "select * from %s where pattern_id = '%s'",
            String.format("%s.%s", testNamespace, Table.PATTERN_STOP.name),
            patternId
        );
    }

    private void assertThatSqlQueryYieldsRowCountGreaterThanZero(String sql) throws SQLException {
        int recordCount = 0;
        ResultSet rs = testDataSource.getConnection().prepareStatement(sql).executeQuery();
        while (rs.next()) recordCount++;
        assertThat(recordCount, greaterThan(0));
    }

    private void executeSqlStatement(String sql) throws SQLException {
        Connection connection = testDataSource.getConnection();
        Statement statement = connection.createStatement();
        statement.execute(sql);
        statement.close();
        connection.commit();
    }
}