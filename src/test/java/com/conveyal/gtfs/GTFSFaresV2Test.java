package com.conveyal.gtfs;

import com.conveyal.gtfs.dto.AreaDTO;
import com.conveyal.gtfs.dto.FareLegRuleDTO;
import com.conveyal.gtfs.dto.FareMediaDTO;
import com.conveyal.gtfs.dto.FareProductDTO;
import com.conveyal.gtfs.dto.FareTransferRuleDTO;
import com.conveyal.gtfs.dto.NetworkDTO;
import com.conveyal.gtfs.dto.RouteNetworkDTO;
import com.conveyal.gtfs.dto.StopAreaDTO;
import com.conveyal.gtfs.dto.TimeFrameDTO;
import com.conveyal.gtfs.graphql.GTFSGraphQL;
import com.conveyal.gtfs.loader.FeedLoadResult;
import com.conveyal.gtfs.loader.JdbcTableWriter;
import com.conveyal.gtfs.loader.Table;
import com.conveyal.gtfs.model.Area;
import com.conveyal.gtfs.model.FareLegRule;
import com.conveyal.gtfs.model.FareMedia;
import com.conveyal.gtfs.model.FareProduct;
import com.conveyal.gtfs.model.FareTransferRule;
import com.conveyal.gtfs.model.Network;
import com.conveyal.gtfs.model.RouteNetwork;
import com.conveyal.gtfs.model.StopArea;
import com.conveyal.gtfs.model.TimeFrame;
import com.conveyal.gtfs.util.InvalidNamespaceException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.zip.ZipFile;

import static com.conveyal.gtfs.GTFS.load;
import static com.conveyal.gtfs.GTFS.makeSnapshot;
import static com.conveyal.gtfs.GTFS.validate;
import static com.conveyal.gtfs.TestUtils.assertResultValue;
import static com.conveyal.gtfs.TestUtils.assertThatSqlQueryYieldsZeroRows;
import static com.conveyal.gtfs.TestUtils.checkFileTestCases;
import static com.conveyal.gtfs.TestUtils.getColumnsForId;
import static com.conveyal.gtfs.TestUtils.getResultSetForId;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeout;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class GTFSFaresV2Test {

    private static final Logger LOG = LoggerFactory.getLogger(GTFSFaresV2Test.class);
    private static String faresZipFileName;
    public static String faresDBName;
    private static DataSource faresDataSource;
    private static String faresNamespace;
    private static final int TEST_TIMEOUT = 5000;

    private static final ObjectMapper mapper = new ObjectMapper();

    private static JdbcTableWriter createTestTableWriter (Table table) throws InvalidNamespaceException {
        return new JdbcTableWriter(table, faresDataSource, faresNamespace);
    }

    @BeforeAll
    public static void setUpClass() throws IOException {
        String folderName = "fake-agency-with-fares-v2";
        faresZipFileName = TestUtils.zipFolderFiles(folderName, true);
        // create a new database
        faresDBName = TestUtils.generateNewDB();
        String dbConnectionUrl = String.format("jdbc:postgresql://localhost/%s", faresDBName);
        faresDataSource = TestUtils.createTestDataSource(dbConnectionUrl);
        // load feed into db
        FeedLoadResult feedLoadResult = load(faresZipFileName, faresDataSource);
        faresNamespace = feedLoadResult.uniqueIdentifier;
        // validate feed to create additional tables
        validate(faresNamespace, faresDataSource);
        // Create an empty snapshot to create a new namespace and all the tables
        FeedLoadResult result = makeSnapshot(null, faresDataSource, false);
        faresNamespace = result.uniqueIdentifier;
    }

    @AfterAll
    public static void tearDownClass() {
        TestUtils.dropDB(faresDBName);
    }

    /** Tests that the graphQL schema can initialize. */
    @Test
    void canInitialize() {
        assertTimeout(Duration.ofMillis(TEST_TIMEOUT), () -> {
            GTFSGraphQL.initialize(faresDataSource);
            GTFSGraphQL.getGraphQl();
        });
    }

    /**
     * Make sure a round-trip of loading fares v2 data and then writing this to another zip file can be performed.
     */
    @Test
    void canDoRoundTripLoadAndWriteToZipFile() throws IOException {
        // create a temp file for this test
        File outZip = File.createTempFile("fares-v2-output", ".zip");

        // delete file to make sure we can assert that this program created the file
        outZip.delete();

        GTFSFeed feed = GTFSFeed.fromFile(faresZipFileName);
        feed.toFile(outZip.getAbsolutePath());
        feed.close();
        assertTrue(outZip.exists());

        // assert that rows of data were written to files within the zipfile
        ZipFile zip = new ZipFile(outZip);

        TestUtils.FileTestCase[] fileTestCases = {
            new TestUtils.FileTestCase(
                "areas.txt",
                new TestUtils.DataExpectation[] {
                    new TestUtils.DataExpectation("area_id", "area_bl"),
                    new TestUtils.DataExpectation("area_name", "Blue Line")
                }
            ),
            new TestUtils.FileTestCase(
                "fare_leg_rules.txt",
                new TestUtils.DataExpectation[] {
                    new TestUtils.DataExpectation("leg_group_id", "leg_airport_rapid_transit_quick_subway"),
                    new TestUtils.DataExpectation("network_id", "rapid_transit"),
                    new TestUtils.DataExpectation("from_area_id", "area_bl_airport")
                }
            ),
            new TestUtils.FileTestCase(
                "fare_media.txt",
                new TestUtils.DataExpectation[] {
                    new TestUtils.DataExpectation("fare_media_id", "cash"),
                    new TestUtils.DataExpectation("fare_media_name", "Cash"),
                    new TestUtils.DataExpectation("fare_media_type", "0")
                }
            ),
            new TestUtils.FileTestCase(
                "fare_products.txt",
                new TestUtils.DataExpectation[] {
                    new TestUtils.DataExpectation("fare_product_id", "prod_boat_zone_1"),
                    new TestUtils.DataExpectation("fare_product_name", "Ferry Zone 1 one-way fare"),
                    new TestUtils.DataExpectation("fare_media_id", "cash"),
                    new TestUtils.DataExpectation("amount", "6.5000000"),
                    new TestUtils.DataExpectation("currency", "USD")
                }
            ),
            new TestUtils.FileTestCase(
                "fare_transfer_rules.txt",
                new TestUtils.DataExpectation[] {
                    new TestUtils.DataExpectation("from_leg_group_id", "leg_airport_rapid_transit_quick_subway"),
                    new TestUtils.DataExpectation("to_leg_group_id", "leg_local_bus_quick_subway"),
                    new TestUtils.DataExpectation("transfer_count", ""),
                    new TestUtils.DataExpectation("duration_limit", "7200"),
                    new TestUtils.DataExpectation("duration_limit_type", "1"),
                    new TestUtils.DataExpectation("fare_transfer_type", "0"),
                    new TestUtils.DataExpectation("fare_product_id", "prod_rapid_transit_quick_subway")
                }
            ),
            new TestUtils.FileTestCase(
                "networks.txt",
                new TestUtils.DataExpectation[] {
                    new TestUtils.DataExpectation("network_id", "1"),
                    new TestUtils.DataExpectation("network_name", "Forbidden because network id is defined in routes")
                }
            ),
            new TestUtils.FileTestCase(
                "route_networks.txt",
                new TestUtils.DataExpectation[] {
                    new TestUtils.DataExpectation("network_id", "1"),
                    new TestUtils.DataExpectation("route_id", "1")
                }
            ),
            new TestUtils.FileTestCase(
                "stop_areas.txt",
                new TestUtils.DataExpectation[] {
                    new TestUtils.DataExpectation("stop_id", "4u6g"),
                    new TestUtils.DataExpectation("area_id", "area_route_426_downtown")
                }
            ),
            new TestUtils.FileTestCase(
                "timeframes.txt",
                new TestUtils.DataExpectation[] {
                    new TestUtils.DataExpectation("timeframe_group_id", "timeframe_sumner_tunnel_closure"),
                    new TestUtils.DataExpectation("start_time", "00:00:00"),
                    new TestUtils.DataExpectation("end_time", "02:30:00"),
                    new TestUtils.DataExpectation("service_id", "04100312-8fe1-46a5-a9f2-556f39478f57")
                }
            )
        };
        checkFileTestCases(zip, fileTestCases);
    }

    @Test
    void canCreateUpdateAndDeleteAreas() throws IOException, SQLException, InvalidNamespaceException {
        // Create.
        String areaId = "area-id-1";
        AreaDTO createdArea = createArea(areaId);
        assertEquals(createdArea.area_id, areaId);

        // Update.
        String updatedAreaId = "area-id-2";
        createdArea.area_id = updatedAreaId;
        JdbcTableWriter updateTableWriter = createTestTableWriter(Table.AREAS);
        String updateOutput = updateTableWriter.update(
            createdArea.id,
            mapper.writeValueAsString(createdArea),
            true
        );
        AreaDTO updatedAreaDTO = mapper.readValue(updateOutput, AreaDTO.class);
        assertEquals(updatedAreaDTO.area_id, updatedAreaId);

        ResultSet resultSet = getResultSetForId(faresDataSource, faresNamespace,  updatedAreaDTO.id, Table.AREAS);
        while (resultSet.next()) {
            assertResultValue(resultSet, Area.AREA_ID_COLUMN_NAME, equalTo(createdArea.area_id));
            assertResultValue(resultSet, Area.AREA_NAME_COLUMN_NAME,equalTo(createdArea.area_name));
         }

        // Delete.
        deleteRecord(Table.AREAS, createdArea.id);
    }

    @Test
    void canCreateUpdateAndDeleteStopAreas() throws IOException, SQLException, InvalidNamespaceException {
        // Create.
        String areaId = "area-id-1";
        StopAreaDTO createdStopArea = createStopArea(areaId);
        assertEquals(createdStopArea.area_id, areaId);

        // Update.
        String updatedAreaId = "area-id-2";
        createdStopArea.area_id = updatedAreaId;
        JdbcTableWriter updateTableWriter = createTestTableWriter(Table.STOP_AREAS);
        String updateOutput = updateTableWriter.update(
            createdStopArea.id,
            mapper.writeValueAsString(createdStopArea),
            true
        );
        StopAreaDTO updatedStopAreaDTO = mapper.readValue(updateOutput, StopAreaDTO.class);
        assertEquals(updatedStopAreaDTO.area_id, updatedAreaId);

        ResultSet resultSet = getResultSetForId(faresDataSource, faresNamespace, updatedStopAreaDTO.id, Table.STOP_AREAS);
        while (resultSet.next()) {
            assertResultValue(resultSet, StopArea.AREA_ID_COLUMN_NAME, equalTo(createdStopArea.area_id));
            assertResultValue(resultSet, StopArea.STOP_ID_COLUMN_NAME,equalTo(createdStopArea.stop_id));
         }

        // Delete.
        deleteRecord(Table.STOP_AREAS, createdStopArea.id);
    }

    @Test
    void canCreateUpdateAndDeleteTimeFrames() throws IOException, SQLException, InvalidNamespaceException {
        // Create.
        String timeFrameGroupId = "time-frame-group-id-1";
        TimeFrameDTO createdTimeFrame = createTimeFrame(timeFrameGroupId);
        assertEquals(createdTimeFrame.timeframe_group_id, timeFrameGroupId);

        // Update.
        String updatedTimeFrameGroupId = "time-frame-group-id-2";
        createdTimeFrame.timeframe_group_id = updatedTimeFrameGroupId;
        JdbcTableWriter updateTableWriter = createTestTableWriter(Table.TIME_FRAMES);
        String updateOutput = updateTableWriter.update(
            createdTimeFrame.id,
            mapper.writeValueAsString(createdTimeFrame),
            true
        );
        TimeFrameDTO updatedTimeFrameDTO = mapper.readValue(updateOutput, TimeFrameDTO.class);
        assertEquals(updatedTimeFrameDTO.timeframe_group_id, updatedTimeFrameGroupId);

        ResultSet resultSet = getResultSetForId(faresDataSource, faresNamespace, updatedTimeFrameDTO.id, Table.TIME_FRAMES);
        while (resultSet.next()) {
            assertResultValue(resultSet, TimeFrame.TIME_FRAME_GROUP_ID_COLUMN_NAME, equalTo(createdTimeFrame.timeframe_group_id));
            assertResultValue(resultSet, TimeFrame.START_TIME_COLUMN_NAME, equalTo(createdTimeFrame.start_time));
            assertResultValue(resultSet, TimeFrame.END_TIME_COLUMN_NAME, equalTo(createdTimeFrame.end_time));
            assertResultValue(resultSet, TimeFrame.SERVICE_ID_COLUMN_NAME, equalTo(createdTimeFrame.service_id));
         }

        // Delete.
        deleteRecord(Table.TIME_FRAMES, createdTimeFrame.id);
    }

    @Test
    void canCreateUpdateAndDeleteNetworks() throws IOException, SQLException, InvalidNamespaceException {
        // Create.
        String networkId = "network-id-1";
        NetworkDTO createdNetwork = createNetwork(networkId);
        assertEquals(createdNetwork.network_id, networkId);

        // Update.
        String updatedNetworkId = "network-id-2";
        createdNetwork.network_id = updatedNetworkId;
        JdbcTableWriter updateTableWriter = createTestTableWriter(Table.NETWORKS);
        String updateOutput = updateTableWriter.update(
            createdNetwork.id,
            mapper.writeValueAsString(createdNetwork),
            true
        );
        NetworkDTO updatedNetworkDTO = mapper.readValue(updateOutput, NetworkDTO.class);
        assertEquals(updatedNetworkDTO.network_id, updatedNetworkId);

        ResultSet resultSet = getResultSetForId(faresDataSource, faresNamespace,  updatedNetworkDTO.id, Table.NETWORKS);
        while (resultSet.next()) {
            assertResultValue(resultSet, Network.NETWORK_ID_COLUMN_NAME, equalTo(createdNetwork.network_id));
            assertResultValue(resultSet, Network.NETWORK_NAME_COLUMN_NAME, equalTo(createdNetwork.network_name));
         }

        // Delete.
        deleteRecord(Table.NETWORKS, createdNetwork.id);
    }

    @Test
    void canCreateUpdateAndDeleteRouteNetworks() throws IOException, SQLException, InvalidNamespaceException {
        // Create.
        String networkId = "network-id-1";
        RouteNetworkDTO createdRouteNetwork = createRouteNetwork(networkId);
        assertEquals(createdRouteNetwork.network_id, networkId);

        // Update.
        String updatedRouteNetworkId = "network-id-2";
        createdRouteNetwork.network_id = updatedRouteNetworkId;
        JdbcTableWriter updateTableWriter = createTestTableWriter(Table.ROUTE_NETWORKS);
        String updateOutput = updateTableWriter.update(
            createdRouteNetwork.id,
            mapper.writeValueAsString(createdRouteNetwork),
            true
        );
        RouteNetworkDTO updatedNetworkDTO = mapper.readValue(updateOutput, RouteNetworkDTO.class);
        assertEquals(updatedNetworkDTO.network_id, updatedRouteNetworkId);

        ResultSet resultSet = getResultSetForId(faresDataSource, faresNamespace,  updatedNetworkDTO.id, Table.ROUTE_NETWORKS);
        while (resultSet.next()) {
            assertResultValue(resultSet, RouteNetwork.NETWORK_ID_COLUMN_NAME, equalTo(createdRouteNetwork.network_id));
            assertResultValue(resultSet, RouteNetwork.ROUTE_ID_COLUMN_NAME, equalTo(createdRouteNetwork.route_id));
         }

        // Delete.
        deleteRecord(Table.ROUTE_NETWORKS, createdRouteNetwork.id);
    }

    @Test
    void canCreateUpdateAndDeleteFareLegRules() throws IOException, SQLException, InvalidNamespaceException {
        // Create.
           String legGroupId = "leg-group-id-1";
        FareLegRuleDTO createdFareLegRule = createFareLegRule(legGroupId);
        assertEquals(createdFareLegRule.leg_group_id, legGroupId);

        // Update.
        String updatedLegGroupId = "leg-group-id-2";
        createdFareLegRule.leg_group_id = updatedLegGroupId;
        JdbcTableWriter updateTableWriter = createTestTableWriter(Table.FARE_LEG_RULES);
        String updateOutput = updateTableWriter.update(
            createdFareLegRule.id,
            mapper.writeValueAsString(createdFareLegRule),
            true
        );
        FareLegRuleDTO updatedFareLegRuleDTO = mapper.readValue(updateOutput, FareLegRuleDTO.class);
        assertEquals(updatedFareLegRuleDTO.leg_group_id, updatedLegGroupId);

        ResultSet resultSet = getResultSetForId(faresDataSource, faresNamespace,  updatedFareLegRuleDTO.id, Table.FARE_LEG_RULES);
        while (resultSet.next()) {
            assertResultValue(resultSet, FareLegRule.LEG_GROUP_ID_COLUMN_NAME, equalTo(createdFareLegRule.leg_group_id));
            assertResultValue(resultSet, FareLegRule.NETWORK_ID_COLUMN_NAME, equalTo(createdFareLegRule.network_id));
            assertResultValue(resultSet, FareLegRule.FROM_AREA_ID_COLUMN_NAME, equalTo(createdFareLegRule.from_area_id));
            assertResultValue(resultSet, FareLegRule.TO_AREA_ID_COLUMN_NAME, equalTo(createdFareLegRule.to_area_id));
            assertResultValue(resultSet, FareLegRule.FROM_TIMEFRAME_GROUP_ID_COLUMN_NAME, equalTo(createdFareLegRule.from_timeframe_group_id));
            assertResultValue(resultSet, FareLegRule.FARE_PRODUCT_ID_COLUMN_NAME, equalTo(createdFareLegRule.fare_product_id));
            assertResultValue(resultSet, FareLegRule.RULE_PRIORITY_COLUMN_NAME, equalTo(createdFareLegRule.rule_priority));
         }

        // Delete.
        deleteRecord(Table.FARE_LEG_RULES, createdFareLegRule.id);
    }

    @Test
    void canCreateUpdateAndDeleteFareMedia() throws IOException, SQLException, InvalidNamespaceException {
        // Create.
        String fareMediaId = "fare-media-id-1";
        FareMediaDTO createdFareMedia = createFareMedia(fareMediaId);
        assertEquals(createdFareMedia.fare_media_id, fareMediaId);

        // Update.
        String updatedFareMediaId = "fare-media-id-2";
        createdFareMedia.fare_media_id = updatedFareMediaId;
        JdbcTableWriter updateTableWriter = createTestTableWriter(Table.FARE_MEDIAS);
        String updateOutput = updateTableWriter.update(
            createdFareMedia.id,
            mapper.writeValueAsString(createdFareMedia),
            true
        );
        FareMediaDTO updatedFareMediaDTO = mapper.readValue(updateOutput, FareMediaDTO.class);
        assertEquals(updatedFareMediaDTO.fare_media_id, updatedFareMediaId);

        ResultSet resultSet = getResultSetForId(faresDataSource, faresNamespace,  updatedFareMediaDTO.id, Table.FARE_MEDIAS);
        while (resultSet.next()) {
            assertResultValue(resultSet, FareMedia.FARE_MEDIA_ID_COLUMN_NAME, equalTo(createdFareMedia.fare_media_id));
            assertResultValue(resultSet, FareMedia.FARE_MEDIA_NAME_COLUMN_NAME, equalTo(createdFareMedia.fare_media_name));
            assertResultValue(resultSet, FareMedia.FARE_MEDIA_TYPE_COLUMN_NAME, equalTo(createdFareMedia.fare_media_type));
         }

        // Delete.
        deleteRecord(Table.FARE_MEDIAS, createdFareMedia.id);
    }

    @Test
    void canCreateUpdateAndDeleteFareProduct() throws IOException, SQLException, InvalidNamespaceException {
        // Create.
        String fareProductId = "fare-product-id-1";
        FareProductDTO createdFareProduct = createFareProduct(fareProductId);
        assertEquals(createdFareProduct.fare_product_id, fareProductId);

        // Update.
        String updatedFareProductId = "fare-product-id-2";
        createdFareProduct.fare_product_id = updatedFareProductId;
        JdbcTableWriter updateTableWriter = createTestTableWriter(Table.FARE_PRODUCTS);
        String updateOutput = updateTableWriter.update(
            createdFareProduct.id,
            mapper.writeValueAsString(createdFareProduct),
            true
        );
        FareProductDTO fareProductDTO = mapper.readValue(updateOutput, FareProductDTO.class);
        assertEquals(fareProductDTO.fare_product_id, updatedFareProductId);

        ResultSet resultSet = getResultSetForId(faresDataSource, faresNamespace,  fareProductDTO.id, Table.FARE_PRODUCTS);
        while (resultSet.next()) {
            assertResultValue(resultSet, FareProduct.FARE_PRODUCT_ID_COLUMN_NAME, equalTo(createdFareProduct.fare_product_id));
            assertResultValue(resultSet, FareProduct.FARE_PRODUCT_NAME_COLUMN_NAME, equalTo(createdFareProduct.fare_product_name));
            assertResultValue(resultSet, FareProduct.FARE_MEDIA_ID_COLUMN_NAME, equalTo(createdFareProduct.fare_media_id));
         }

        // Delete.
        deleteRecord(Table.FARE_PRODUCTS, createdFareProduct.id);
    }

    @Test
    void canCreateUpdateAndDeleteFareTransferRule() throws IOException, SQLException, InvalidNamespaceException {
        // Create.
        String fromLegGroupId = "from-leg-group-id-1";
        String toLegGroupId = "to-leg-group-id-1";
        FareTransferRuleDTO fareTransferRules = createFareTransferRules(fromLegGroupId, toLegGroupId);
        assertEquals(fareTransferRules.from_leg_group_id, fromLegGroupId);
        assertEquals(fareTransferRules.to_leg_group_id, toLegGroupId);

        // Update.
        String updatedFromLegGroupId = "from-leg-group-id-2";
        String updatedToLegGroupId = "to-leg-group-id-2";
        fareTransferRules.from_leg_group_id = updatedFromLegGroupId;
        fareTransferRules.to_leg_group_id = updatedToLegGroupId;
        JdbcTableWriter updateTableWriter = createTestTableWriter(Table.FARE_TRANSFER_RULES);
        String updateOutput = updateTableWriter.update(
            fareTransferRules.id,
            mapper.writeValueAsString(fareTransferRules),
            true
        );
        FareTransferRuleDTO fareTransferRuleDTO = mapper.readValue(updateOutput, FareTransferRuleDTO.class);
        assertEquals(fareTransferRuleDTO.from_leg_group_id, updatedFromLegGroupId);
        assertEquals(fareTransferRuleDTO.to_leg_group_id, updatedToLegGroupId);

        ResultSet resultSet = getResultSetForId(faresDataSource, faresNamespace,  fareTransferRuleDTO.id, Table.FARE_TRANSFER_RULES);
        while (resultSet.next()) {
            assertResultValue(resultSet, FareTransferRule.FROM_LEG_GROUP_ID_COLUMN_NAME, equalTo(fareTransferRules.from_leg_group_id));
            assertResultValue(resultSet, FareTransferRule.TO_LEG_GROUP_ID_COLUMN_NAME, equalTo(fareTransferRules.to_leg_group_id));
            assertResultValue(resultSet, FareTransferRule.TRANSFER_COUNT_COLUMN_NAME, equalTo(fareTransferRules.transfer_count));
            assertResultValue(resultSet, FareTransferRule.DURATION_LIMIT_COLUMN_NAME, equalTo(fareTransferRules.duration_limit));
            assertResultValue(resultSet, FareTransferRule.FARE_TRANSFER_TYPE_COLUMN_NAME, equalTo(fareTransferRules.fare_transfer_type));
            assertResultValue(resultSet, FareTransferRule.FARE_PRODUCT_ID_COLUMN_NAME, equalTo(fareTransferRules.fare_product_id));
         }

        // Delete.
        deleteRecord(Table.FARE_TRANSFER_RULES, fareTransferRules.id);
    }

    private void deleteRecord(Table table, Integer id) throws InvalidNamespaceException, SQLException {
        JdbcTableWriter deleteTableWriter = createTestTableWriter(table);
        int deleteOutput = deleteTableWriter.delete(id, true);
        LOG.info("deleted {} records from {}", deleteOutput, table.name);
        assertThatSqlQueryYieldsZeroRows(faresDataSource, getColumnsForId(faresNamespace, id, table));
    }

    /**
     * Create and store an area for testing.
     */
    private static AreaDTO createArea(String areaId) throws InvalidNamespaceException, IOException, SQLException {
        AreaDTO input = new AreaDTO();
        input.area_id = areaId;
        input.area_name = "area-name";
        JdbcTableWriter createTableWriter = createTestTableWriter(Table.AREAS);
        String output = createTableWriter.create(mapper.writeValueAsString(input), true);
        return mapper.readValue(output, AreaDTO.class);
    }

    /**
     * Create and store a stop area for testing.
     */
    private static StopAreaDTO createStopArea(String areaId) throws InvalidNamespaceException, IOException, SQLException {
        StopAreaDTO input = new StopAreaDTO();
        input.area_id = areaId;
        input.stop_id = "stop-id-1";
        JdbcTableWriter createTableWriter = createTestTableWriter(Table.STOP_AREAS);
        String output = createTableWriter.create(mapper.writeValueAsString(input), true);
        return mapper.readValue(output, StopAreaDTO.class);
    }

    /**
     * Create and store a time frame for testing.
     */
    private static TimeFrameDTO createTimeFrame(String timeframeGroupId) throws InvalidNamespaceException, IOException, SQLException {
        TimeFrameDTO input = new TimeFrameDTO();
        input.timeframe_group_id = timeframeGroupId;
        input.start_time = 0;
        input.end_time = 2600;
        input.service_id = "service-id-1";
        JdbcTableWriter createTableWriter = createTestTableWriter(Table.TIME_FRAMES);
        String output = createTableWriter.create(mapper.writeValueAsString(input), true);
        return mapper.readValue(output, TimeFrameDTO.class);
    }

    /**
     * Create and store a network for testing.
     */
    private static NetworkDTO createNetwork(String networkId) throws InvalidNamespaceException, IOException, SQLException {
        NetworkDTO input = new NetworkDTO();
        input.network_id = networkId;
        input.network_name = "network-name-1";
        JdbcTableWriter createTableWriter = createTestTableWriter(Table.NETWORKS);
        String output = createTableWriter.create(mapper.writeValueAsString(input), true);
        return mapper.readValue(output, NetworkDTO.class);
    }

    /**
     * Create and store a route network for testing.
     */
    private static RouteNetworkDTO createRouteNetwork(String networkId) throws InvalidNamespaceException, IOException, SQLException {
        RouteNetworkDTO input = new RouteNetworkDTO();
        input.network_id = networkId;
        input.route_id = "route-id-1";
        JdbcTableWriter createTableWriter = createTestTableWriter(Table.ROUTE_NETWORKS);
        String output = createTableWriter.create(mapper.writeValueAsString(input), true);
        return mapper.readValue(output, RouteNetworkDTO.class);
    }

    /**
     * Create and store a fare leg rule for testing.
     */
    private static FareLegRuleDTO createFareLegRule(String legGroupId) throws InvalidNamespaceException, IOException, SQLException {
        FareLegRuleDTO input = new FareLegRuleDTO();
        input.leg_group_id = legGroupId;
        input.network_id = "network-id-1";
        input.from_area_id = "from-area-id-1";
        input.to_area_id = "to-area-id-1";
        input.from_timeframe_group_id = "from-timeframe-group-id-1";
        input.to_timeframe_group_id = "to-timeframe-group-id-1";
        input.fare_product_id = "fare-product-id-1";
        input.rule_priority = 1;
        JdbcTableWriter createTableWriter = createTestTableWriter(Table.FARE_LEG_RULES);
        String output = createTableWriter.create(mapper.writeValueAsString(input), true);
        return mapper.readValue(output, FareLegRuleDTO.class);
    }

    /**
     * Create and store a fare media for testing.
     */
    private static FareMediaDTO createFareMedia(String fareMediaId) throws InvalidNamespaceException, IOException, SQLException {
        FareMediaDTO input = new FareMediaDTO();
        input.fare_media_id = fareMediaId;
        input.fare_media_name = "fare-media-name";
        input.fare_media_type = 1;
        JdbcTableWriter createTableWriter = createTestTableWriter(Table.FARE_MEDIAS);
        String output = createTableWriter.create(mapper.writeValueAsString(input), true);
        return mapper.readValue(output, FareMediaDTO.class);
    }

    /**
     * Create and store a fare product for testing.
     */
    private static FareProductDTO createFareProduct(String fareProductId) throws InvalidNamespaceException, IOException, SQLException {
        FareProductDTO input = new FareProductDTO();
        input.fare_product_id = fareProductId;
        input.fare_product_name = "fare-product-name";
        input.fare_media_id = "fare-media-id";
        input.amount = 6.25;
        input.currency = "USD";
        JdbcTableWriter createTableWriter = createTestTableWriter(Table.FARE_PRODUCTS);
        String output = createTableWriter.create(mapper.writeValueAsString(input), true);
        return mapper.readValue(output, FareProductDTO.class);
    }

    /**
     * Create and store a fare transfer rules for testing.
     */
    private static FareTransferRuleDTO createFareTransferRules(String fromLegGroupId, String toLegGroupId) throws InvalidNamespaceException, IOException, SQLException {
        FareTransferRuleDTO input = new FareTransferRuleDTO();
        input.from_leg_group_id = fromLegGroupId;
        input.to_leg_group_id = toLegGroupId;
        input.transfer_count = -1;
        input.duration_limit = 1;
        input.duration_limit_type = 1;
        input.fare_transfer_type = 2;
        input.fare_product_id = "fare-product-id-1";
        JdbcTableWriter createTableWriter = createTestTableWriter(Table.FARE_TRANSFER_RULES);
        String output = createTableWriter.create(mapper.writeValueAsString(input), true);
        return mapper.readValue(output, FareTransferRuleDTO.class);
    }
}
