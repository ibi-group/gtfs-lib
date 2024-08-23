package com.conveyal.gtfs.graphql;

import com.conveyal.gtfs.graphql.fetchers.MapFetcher;
import com.conveyal.gtfs.model.Area;
import com.conveyal.gtfs.model.FareLegRule;
import com.conveyal.gtfs.model.FareMedia;
import com.conveyal.gtfs.model.FareProduct;
import com.conveyal.gtfs.model.FareTransferRule;
import com.conveyal.gtfs.model.Network;
import com.conveyal.gtfs.model.Route;
import com.conveyal.gtfs.model.RouteNetwork;
import com.conveyal.gtfs.model.StopArea;
import com.conveyal.gtfs.model.TimeFrame;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLObjectType;

import java.util.Arrays;
import java.util.List;

import static com.conveyal.gtfs.graphql.GraphQLUtil.createFieldDefinition;
import static graphql.Scalars.GraphQLInt;
import static graphql.schema.GraphQLObjectType.newObject;

public class GraphQLGtfsFaresV2Schema {

    private GraphQLGtfsFaresV2Schema() {}

    public static final GraphQLObjectType stopAreaType = newObject().name("stopArea")
        .description("A GTFS stop area object")
        .field(MapFetcher.field("id", GraphQLInt))
        .field(MapFetcher.field(StopArea.AREA_ID_NAME))
        .field(MapFetcher.field(StopArea.STOP_ID_NAME))
        .field(createFieldDefinition("stops", GraphQLGtfsSchema.stopType, "stops", StopArea.STOP_ID_NAME))
        .build();

    public static final GraphQLObjectType areaType = newObject().name("area")
        .description("A GTFS area object")
        .field(MapFetcher.field("id", GraphQLInt))
        .field(MapFetcher.field(Area.AREA_ID_NAME))
        .field(MapFetcher.field(Area.AREA_NAME_NAME))
        .field(createFieldDefinition("stopAreas", stopAreaType, StopArea.TABLE_NAME, Area.AREA_ID_NAME))
        .build();

    public static final GraphQLObjectType timeFrameType = newObject().name("timeFrame")
        .description("A GTFS time frame object")
        .field(MapFetcher.field("id", GraphQLInt))
        .field(MapFetcher.field(TimeFrame.TIME_FRAME_GROUP_ID_NAME))
        .field(MapFetcher.field(TimeFrame.START_TIME_NAME))
        .field(MapFetcher.field(TimeFrame.END_TIME_NAME))
        .field(MapFetcher.field(TimeFrame.SERVICE_ID_NAME))
        .build();

    public static final GraphQLObjectType networkType = newObject().name("network")
        .description("A GTFS network object")
        .field(MapFetcher.field("id", GraphQLInt))
        .field(MapFetcher.field(Network.NETWORK_ID_NAME))
        .field(MapFetcher.field(Network.NETWORK_NAME_NAME))
        .build();

    public static final GraphQLObjectType routeNetworkType = newObject().name("routeNetwork")
        .description("A GTFS route network object")
        .field(MapFetcher.field("id", GraphQLInt))
        .field(MapFetcher.field(RouteNetwork.NETWORK_ID_NAME))
        .field(MapFetcher.field(RouteNetwork.ROUTE_ID_NAME))
        .field(createFieldDefinition("networks", networkType, Network.TABLE_NAME, RouteNetwork.NETWORK_ID_NAME))
        .build();

    public static final GraphQLObjectType fareMediaType = newObject().name("fareMedia")
        .description("A GTFS fare media object")
        .field(MapFetcher.field("id", GraphQLInt))
        .field(MapFetcher.field(FareMedia.FARE_MEDIA_ID_NAME))
        .field(MapFetcher.field(FareMedia.FARE_MEDIA_NAME_NAME))
        .field(MapFetcher.field(FareMedia.FARE_MEDIA_TYPE_NAME))
        .build();

    public static final GraphQLObjectType fareProductType = newObject().name("fareProduct")
        .description("A GTFS fare product object")
        .field(MapFetcher.field("id", GraphQLInt))
        .field(MapFetcher.field(FareProduct.FARE_PRODUCT_ID_NAME))
        .field(MapFetcher.field(FareProduct.FARE_PRODUCT_NAME_NAME))
        .field(MapFetcher.field(FareProduct.FARE_MEDIA_ID_NAME))
        .field(MapFetcher.field(FareProduct.AMOUNT_NAME))
        .field(MapFetcher.field(FareProduct.CURRENCY_NAME))
        .field(createFieldDefinition("fareMedia", fareMediaType, FareMedia.TABLE_NAME, FareProduct.FARE_MEDIA_ID_NAME))
        .build();

    public static final GraphQLObjectType fareLegRuleType = newObject().name("fareLegRule")
        .description("A GTFS fare leg rule object")
        .field(MapFetcher.field("id", GraphQLInt))
        .field(MapFetcher.field(FareLegRule.LEG_GROUP_ID_NAME))
        .field(MapFetcher.field(FareLegRule.NETWORK_ID_NAME))
        .field(MapFetcher.field(FareLegRule.FROM_AREA_ID_NAME))
        .field(MapFetcher.field(FareLegRule.TO_AREA_ID_NAME))
        .field(MapFetcher.field(FareLegRule.FROM_TIMEFRAME_GROUP_ID_NAME))
        .field(MapFetcher.field(FareLegRule.TO_TIMEFRAME_GROUP_ID_NAME))
        .field(MapFetcher.field(FareLegRule.FARE_PRODUCT_ID_NAME))
        .field(MapFetcher.field(FareLegRule.RULE_PRIORITY_NAME))
        // Will return either routes or networks, not both.
        .field(createFieldDefinition("routes", GraphQLGtfsSchema.routeType, Route.TABLE_NAME, FareLegRule.NETWORK_ID_NAME))
        .field(createFieldDefinition("networks", networkType, Network.TABLE_NAME, Network.NETWORK_ID_NAME))
        .field(createFieldDefinition("fareProducts", fareProductType, FareProduct.TABLE_NAME, FareLegRule.FARE_PRODUCT_ID_NAME))
        // fromTimeFrame and toTimeFrame may return multiple time frames.
        .field(createFieldDefinition(
            "fromTimeFrame",
            timeFrameType,
            TimeFrame.TABLE_NAME,
            FareLegRule.FROM_TIMEFRAME_GROUP_ID_NAME,
            TimeFrame.TIME_FRAME_GROUP_ID_NAME
        ))
        .field(createFieldDefinition(
            "toTimeFrame",
            timeFrameType,
            TimeFrame.TABLE_NAME,
            FareLegRule.TO_TIMEFRAME_GROUP_ID_NAME,
            TimeFrame.TIME_FRAME_GROUP_ID_NAME
        ))
        .field(createFieldDefinition("toArea", areaType, Area.TABLE_NAME, FareLegRule.TO_AREA_ID_NAME, Area.AREA_ID_NAME))
        .field(createFieldDefinition("fromArea", areaType, Area.TABLE_NAME, FareLegRule.FROM_AREA_ID_NAME, Area.AREA_ID_NAME))
        .build();

    public static final GraphQLObjectType fareTransferRuleType = newObject().name("fareTransferRule")
        .description("A GTFS fare transfer rule object")
        .field(MapFetcher.field("id", GraphQLInt))
        .field(MapFetcher.field(FareTransferRule.FROM_LEG_GROUP_ID_NAME))
        .field(MapFetcher.field(FareTransferRule.TO_LEG_GROUP_ID_NAME))
        .field(MapFetcher.field(FareTransferRule.TRANSFER_COUNT_NAME))
        .field(MapFetcher.field(FareTransferRule.DURATION_LIMIT_NAME))
        .field(MapFetcher.field(FareTransferRule.DURATION_LIMIT_TYPE_NAME))
        .field(MapFetcher.field(FareTransferRule.FARE_PRODUCT_ID_NAME))
        .field(createFieldDefinition("toArea", areaType, Area.TABLE_NAME, FareLegRule.TO_AREA_ID_NAME, Area.AREA_ID_NAME))
        .field(createFieldDefinition("fareProducts", fareProductType, FareProduct.TABLE_NAME, FareProduct.FARE_PRODUCT_ID_NAME))
        .field(createFieldDefinition(
            "fromFareLegRule",
            fareLegRuleType,
            FareLegRule.TABLE_NAME,
            FareTransferRule.FROM_LEG_GROUP_ID_NAME,
            FareLegRule.LEG_GROUP_ID_NAME
        ))
        .field(createFieldDefinition(
            "toFareLegRule",
            fareLegRuleType,
            FareLegRule.TABLE_NAME,
            FareTransferRule.TO_LEG_GROUP_ID_NAME,
            FareLegRule.LEG_GROUP_ID_NAME
        ))
        .build();

    public static List<GraphQLFieldDefinition> getFaresV2FieldDefinitions() {
        return Arrays.asList(
            createFieldDefinition("area", areaType, Area.TABLE_NAME),
            createFieldDefinition("stopArea", stopAreaType, StopArea.TABLE_NAME),
            createFieldDefinition("fareTransferRule", fareTransferRuleType, FareTransferRule.TABLE_NAME),
            createFieldDefinition("fareProduct", fareProductType, FareProduct.TABLE_NAME),
            createFieldDefinition("fareMedia", fareMediaType, FareMedia.TABLE_NAME),
            createFieldDefinition("fareLegRule", fareLegRuleType, FareLegRule.TABLE_NAME),
            createFieldDefinition("timeFrame", timeFrameType, TimeFrame.TABLE_NAME),
            createFieldDefinition("network", networkType, Network.TABLE_NAME),
            createFieldDefinition("routeNetwork", routeNetworkType, RouteNetwork.TABLE_NAME)
        );
    }
}
