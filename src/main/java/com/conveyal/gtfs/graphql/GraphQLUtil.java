package com.conveyal.gtfs.graphql;

import com.conveyal.gtfs.graphql.fetchers.JDBCFetcher;
import graphql.schema.GraphQLArgument;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLType;
import graphql.schema.PropertyDataFetcher;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.conveyal.gtfs.graphql.fetchers.JDBCFetcher.ID_ARG;
import static com.conveyal.gtfs.graphql.fetchers.JDBCFetcher.LIMIT_ARG;
import static com.conveyal.gtfs.graphql.fetchers.JDBCFetcher.OFFSET_ARG;
import static graphql.Scalars.GraphQLFloat;
import static graphql.Scalars.GraphQLInt;
import static graphql.Scalars.GraphQLString;
import static graphql.schema.GraphQLArgument.newArgument;
import static graphql.schema.GraphQLFieldDefinition.newFieldDefinition;

public class GraphQLUtil {

    public static GraphQLFieldDefinition string (String name) {
        return newFieldDefinition()
                .name(name)
                .type(GraphQLString)
                .dataFetcher(new PropertyDataFetcher(name))
                .build();
    }

    public static GraphQLFieldDefinition intt (String name) {
        return newFieldDefinition()
                .name(name)
                .type(GraphQLInt)
                .dataFetcher(new PropertyDataFetcher(name))
                .build();
    }

    public static GraphQLArgument stringArg (String name) {
        return newArgument()
                .name(name)
                .type(GraphQLString)
                .build();
    }

    public static GraphQLArgument multiStringArg (String name) {
        return newArgument()
                .name(name)
                .type(new GraphQLList(GraphQLString))
                .build();
    }

    public static GraphQLArgument intArg (String name) {
        return newArgument()
                .name(name)
                .type(GraphQLInt)
                .build();
    }

    public static GraphQLArgument floatArg (String name) {
        return newArgument()
                .name(name)
                .type(GraphQLFloat)
                .build();
    }

    /**
     * Standard base arguments.
     */
    public static List<GraphQLArgument> buildArgs() {
        return new ArrayList<>(Arrays.asList(intArg(ID_ARG), intArg(LIMIT_ARG), intArg(OFFSET_ARG)));
    }

    /**
     * Standard base arguments with additions.
     */
    public static List<GraphQLArgument> buildArgs(GraphQLArgument... addOns) {
        List<GraphQLArgument> args = buildArgs();
        Collections.addAll(args, addOns);
        return args;
    }

    /**
     * Standard field definition with base arguments.
     */
    public static GraphQLFieldDefinition createFieldDefinition(String name, GraphQLType graphQLType, String tableName) {
        return createFieldDefinition(name, graphQLType, tableName, buildArgs());
    }

    /**
     * Field definition with bespoke arguments. Name and table name are the same.
     */
    public static GraphQLFieldDefinition createFieldDefinition(
        String name,
        GraphQLType graphQLType,
        List<GraphQLArgument> arguments
    ) {
        return createFieldDefinition(name, graphQLType, name, arguments);
    }

    /**
     * Field definition with bespoke arguments.
     */
    public static GraphQLFieldDefinition createFieldDefinition(
        String name,
        GraphQLType graphQLType,
        String tableName,
        List<GraphQLArgument> arguments
    ) {
        return newFieldDefinition()
            .name(name)
            .type(new GraphQLList(graphQLType))
            .argument(stringArg("namespace"))
            .arguments(arguments)
            .dataFetcher(new JDBCFetcher(tableName))
            .build();
    }

    /**
     * Field definition for standard table join.
     */
    public static GraphQLFieldDefinition createFieldDefinition(
        String name,
        GraphQLType graphQLType,
        String tableName,
        String parentJoinField
    ) {
        return newFieldDefinition()
            .name(name)
            .type(new GraphQLList(graphQLType))
            .dataFetcher(new JDBCFetcher(tableName, parentJoinField))
            .build();
    }

    /**
     * Field definition for join with child table.
     */
    public static GraphQLFieldDefinition createFieldDefinition(
        String name,
        GraphQLType graphQLType,
        String tableName,
        String parentJoinField,
        String childJoinField
    ) {
        return newFieldDefinition()
            .name(name)
            .type(new GraphQLList(graphQLType))
            .dataFetcher(new JDBCFetcher(tableName, parentJoinField, null, false, childJoinField))
            .build();
    }
}
