package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.TestUtils;
import com.conveyal.gtfs.util.InvalidNamespaceException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.stream.Stream;

import static com.conveyal.gtfs.GTFS.makeSnapshot;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * This class contains CRUD tests for {@link JdbcTableWriter} (i.e., editing GTFS entities in the RDBMS). Set up
 * consists of creating a scratch database and an empty feed snapshot, which is the necessary starting condition
 * for building a GTFS feed from scratch. It then runs the various CRUD tests and finishes by dropping the database
 * (even if tests fail).
 */
public class JDBCTableWriterFaresV2Test {

    private static final Logger LOG = LoggerFactory.getLogger(JDBCTableWriterFaresV2Test.class);

    private static String testDBName;
    private static DataSource testDataSource;
    private static String testNamespace;

    private static JdbcTableWriter createTestTableWriter(Table table) throws InvalidNamespaceException {
        return new JdbcTableWriter(table, testDataSource, testNamespace);
    }

    @BeforeAll
    public static void setUpClass() throws SQLException {
        testDBName = TestUtils.generateNewDB();
        String dbConnectionUrl = String.format("jdbc:postgresql://localhost/%s", testDBName);
        testDataSource = TestUtils.createTestDataSource(dbConnectionUrl);
        LOG.info("creating feeds table because it isn't automatically generated unless you import a feed");
        Connection connection = testDataSource.getConnection();
        connection.createStatement().execute(JdbcGtfsLoader.getCreateFeedRegistrySQL());
        connection.commit();
        LOG.info("feeds table created");
        // Create an empty snapshot to create a new namespace and all the tables.
        FeedLoadResult result = makeSnapshot(null, testDataSource, false);
        testNamespace = result.uniqueIdentifier;
    }

    @AfterAll
    public static void tearDownClass() {
        TestUtils.dropDB(testDBName);
    }

    @ParameterizedTest
    @MethodSource("createEntityInput")
    void canCreateUpdateAndDeleteEntity(
        Table table,
        String entity,
        String entityUpdated
    ) throws IOException, SQLException, InvalidNamespaceException {
        assertEquals(entity, createTestTableWriter(table).create(entity, true));

        JdbcTableWriter updateTableWriter = createTestTableWriter(table);
        assertEquals(entityUpdated, updateTableWriter.update(1, entityUpdated, true));

        deleteEntity(table);
    }

    /**
     * Define JSON payload. ID must be set as 1.
     */
    private static Stream<Arguments> createEntityInput() throws IOException {
        return Stream.of(
            Arguments.of(
                Table.FARE_PRODUCTS,
                getEntityFromFile("fare_product.json"),
                getEntityFromFile("fare_product_updated.json")
            ),
            Arguments.of(
                Table.FARE_MEDIAS,
                getEntityFromFile("fare_media.json"),
                getEntityFromFile("fare_media_updated.json")
            ),
            Arguments.of(
                Table.FARE_LEG_RULES,
                getEntityFromFile("fare_leg_rules.json"),
                getEntityFromFile("fare_leg_rules_updated.json")
            ),
            Arguments.of(
                Table.FARE_TRANSFER_RULES,
                getEntityFromFile("fare_transfer_rules.json"),
                getEntityFromFile("fare_transfer_rules_updated.json")
            ),
            Arguments.of(
                Table.ROUTE_NETWORKS,
                getEntityFromFile("route_networks.json"),
                getEntityFromFile("route_networks_updated.json")
            ),
            Arguments.of(
                Table.NETWORKS,
                getEntityFromFile("networks.json"),
                getEntityFromFile("networks_updated.json")
            ),
            Arguments.of(
                Table.AREAS,
                getEntityFromFile("areas.json"),
                getEntityFromFile("areas_updated.json")
            ),
            Arguments.of(
                Table.STOP_AREAS,
                getEntityFromFile("stop_areas.json"),
                getEntityFromFile("stop_areas_updated.json")
            ),
            Arguments.of(
                Table.TIME_FRAMES,
                getEntityFromFile("time_frames.json"),
                getEntityFromFile("time_frames_updated.json")
            )
        );
    }

    /**
     * Get an entity from file which is expected to be in JSON and remove any formatting.
     */
    private static String getEntityFromFile(String fileName) throws IOException {
        return TestUtils
            .getTestResourceAsString("fares-v2-json-entities/" + fileName)
            .replace(System.lineSeparator(), "")
            .replace(" ", "")
            .replace("\"\"", "null");
    }

    private static void deleteEntity(Table table) throws InvalidNamespaceException, SQLException {
        JdbcTableWriter deleteTableWriter = createTestTableWriter(table);
        deleteTableWriter.delete(1, true);
        assertThatSqlQueryYieldsRowCount(getColumnsForId(1, table));
    }

    /**
     * Constructs SQL query for the specified ID and columns and returns the resulting result set.
     */
    private static String getColumnsForId(int id, Table table, String... columns) {
        return String.format(
            "select %s from %s.%s where id=%d",
            columns.length > 0 ? String.join(", ", columns) : "*",
            testNamespace,
            table.name,
            id
        );
    }

    private static void assertThatSqlQueryYieldsRowCount(String sql) throws SQLException {
        int recordCount = 0;
        ResultSet rs = testDataSource.getConnection().prepareStatement(sql).executeQuery();
        while (rs.next()) recordCount++;
        assertEquals(0, recordCount, "Records matching query should equal expected count.");
    }
}
