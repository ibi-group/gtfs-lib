package com.conveyal.gtfs.graphql;

import com.conveyal.gtfs.graphql.fetchers.MapFetcher;
import com.conveyal.gtfs.model.Area;
import com.conveyal.gtfs.model.FareLegJoinRule;
import com.conveyal.gtfs.model.FareLegRule;
import com.conveyal.gtfs.model.FareMedia;
import com.conveyal.gtfs.model.FareProduct;
import com.conveyal.gtfs.model.FareTransferRule;
import com.conveyal.gtfs.model.Network;
import com.conveyal.gtfs.model.RiderCategory;
import com.conveyal.gtfs.model.TimeFrame;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLObjectType;

import java.util.Arrays;
import java.util.List;

import static graphql.Scalars.GraphQLInt;
import static graphql.schema.GraphQLObjectType.newObject;

public class GraphQLGtfsFaresV2Schema {

    private static final String AREA_TYPE_NAME = "area";
    private static final String TIME_FRAME_TYPE_NAME = "time_frame";
    private static final String NETWORK_TYPE_NAME = "network";
    private static final String FARE_MEDIA_TYPE_NAME = "fare_media";
    private static final String FARE_PRODUCT_TYPE_NAME = "fare_product";
    private static final String FARE_LEG_RULE_TYPE_NAME = "fare_leg_rule";
    private static final String FARE_LEG_JOIN_RULE_TYPE_NAME = "fare_leg_join_rule";
    private static final String FARE_TRANSFER_RULE_TYPE_NAME = "fare_transfer_rule";
    private static final String RIDER_CATEGORY_TYPE_NAME = "rider_category";

    private GraphQLGtfsFaresV2Schema() {}

    private static final GraphQLObjectType areaType = newObject()
        .name(AREA_TYPE_NAME)
        .description("A GTFS area object")
        .field(MapFetcher.field("id", GraphQLInt))
        .field(MapFetcher.field(Area.AREA_ID_NAME))
        .field(MapFetcher.field(Area.AREA_NAME_NAME))
        .build();

    private static final GraphQLObjectType timeFrameType = newObject()
        .name(TIME_FRAME_TYPE_NAME)
        .description("A GTFS time frame object")
        .field(MapFetcher.field("id", GraphQLInt))
        .field(MapFetcher.field(TimeFrame.TIME_FRAME_GROUP_ID_NAME))
        .field(MapFetcher.field(TimeFrame.START_TIME_NAME))
        .field(MapFetcher.field(TimeFrame.END_TIME_NAME))
        .field(MapFetcher.field(TimeFrame.SERVICE_ID_NAME))
        .build();

    private static final GraphQLObjectType networkType = newObject()
        .name(NETWORK_TYPE_NAME)
        .description("A GTFS network object")
        .field(MapFetcher.field("id", GraphQLInt))
        .field(MapFetcher.field(Network.NETWORK_ID_NAME))
        .field(MapFetcher.field(Network.NETWORK_NAME_NAME))
        .build();

    private static final GraphQLObjectType fareMediaType = newObject()
        .name(FARE_MEDIA_TYPE_NAME)
        .description("A GTFS fare media object")
        .field(MapFetcher.field("id", GraphQLInt))
        .field(MapFetcher.field(FareMedia.FARE_MEDIA_ID_NAME))
        .field(MapFetcher.field(FareMedia.FARE_MEDIA_NAME_NAME))
        .field(MapFetcher.field(FareMedia.FARE_MEDIA_TYPE_NAME))
        .build();

    private static final GraphQLObjectType fareProductType = newObject()
        .name(FARE_PRODUCT_TYPE_NAME)
        .description("A GTFS fare product object")
        .field(MapFetcher.field("id", GraphQLInt))
        .field(MapFetcher.field(FareProduct.FARE_PRODUCT_ID_NAME))
        .field(MapFetcher.field(FareProduct.FARE_PRODUCT_NAME_NAME))
        .field(MapFetcher.field(FareProduct.RIDER_CATEGORY_ID_NAME))
        .field(MapFetcher.field(FareProduct.FARE_MEDIA_ID_NAME))
        .field(MapFetcher.field(FareProduct.AMOUNT_NAME))
        .field(MapFetcher.field(FareProduct.CURRENCY_NAME))
        .build();

    private static final GraphQLObjectType fareLegRuleType = newObject()
        .name(FARE_LEG_RULE_TYPE_NAME)
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
        .build();

    private static final GraphQLObjectType fareLegJoinRuleType = newObject()
        .name(FARE_LEG_JOIN_RULE_TYPE_NAME)
        .description("A GTFS fare leg join rule object")
        .field(MapFetcher.field("id", GraphQLInt))
        .field(MapFetcher.field(FareLegJoinRule.FROM_NETWORK_ID_NAME))
        .field(MapFetcher.field(FareLegJoinRule.TO_NETWORK_ID_NAME))
        .field(MapFetcher.field(FareLegJoinRule.FROM_STOP_ID_NAME))
        .field(MapFetcher.field(FareLegJoinRule.TO_STOP_ID_NAME))
        .build();

    private static final GraphQLObjectType fareTransferRuleType = newObject()
        .name(FARE_TRANSFER_RULE_TYPE_NAME)
        .description("A GTFS fare transfer rule object")
        .field(MapFetcher.field("id", GraphQLInt))
        .field(MapFetcher.field(FareTransferRule.FROM_LEG_GROUP_ID_NAME))
        .field(MapFetcher.field(FareTransferRule.TO_LEG_GROUP_ID_NAME))
        .field(MapFetcher.field(FareTransferRule.TRANSFER_COUNT_NAME))
        .field(MapFetcher.field(FareTransferRule.DURATION_LIMIT_NAME))
        .field(MapFetcher.field(FareTransferRule.DURATION_LIMIT_TYPE_NAME))
        .field(MapFetcher.field(FareTransferRule.FARE_TRANSFER_TYPE_NAME))
        .field(MapFetcher.field(FareTransferRule.FARE_PRODUCT_ID_NAME))
        .build();

    private static final GraphQLObjectType riderCategoryType = newObject()
        .name(RIDER_CATEGORY_TYPE_NAME)
        .description("A GTFS rider category object")
        .field(MapFetcher.field("id", GraphQLInt))
        .field(MapFetcher.field(RiderCategory.RIDER_CATEGORY_ID_NAME))
        .field(MapFetcher.field(RiderCategory.RIDER_CATEGORY_NAME_NAME))
        .field(MapFetcher.field(RiderCategory.RIDER_CATEGORY_IS_DEFAULT_FARE_CATEGORY_NAME))
        .field(MapFetcher.field(RiderCategory.RIDER_CATEGORY_ELIGIBILITY_URL_NAME))
        .build();

    public static List<GraphQLFieldDefinition> getFaresV2FieldDefinitions() {
        return Arrays.asList(
            GraphQLUtil.field(AREA_TYPE_NAME, areaType, Area.TABLE_NAME),
            GraphQLUtil.field(FARE_LEG_JOIN_RULE_TYPE_NAME, fareLegJoinRuleType, FareLegJoinRule.TABLE_NAME),
            GraphQLUtil.field(FARE_LEG_RULE_TYPE_NAME, fareLegRuleType, FareLegRule.TABLE_NAME),
            GraphQLUtil.field(FARE_MEDIA_TYPE_NAME, fareMediaType, FareMedia.TABLE_NAME),
            GraphQLUtil.field(FARE_PRODUCT_TYPE_NAME, fareProductType, FareProduct.TABLE_NAME),
            GraphQLUtil.field(FARE_TRANSFER_RULE_TYPE_NAME, fareTransferRuleType, FareTransferRule.TABLE_NAME),
            GraphQLUtil.field(NETWORK_TYPE_NAME, networkType, Network.TABLE_NAME),
            GraphQLUtil.field(RIDER_CATEGORY_TYPE_NAME, riderCategoryType, RiderCategory.TABLE_NAME),
            GraphQLUtil.field(TIME_FRAME_TYPE_NAME, timeFrameType, TimeFrame.TABLE_NAME)
        );
    }
}
