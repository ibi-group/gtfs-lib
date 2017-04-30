package com.conveyal.gtfs.error;

import com.conveyal.gtfs.loader.Table;
import com.conveyal.gtfs.storage.StorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

/**
 * This is an abstraction for something that stores GTFS loading and validation errors one by one.
 * Currently there's only one implementation, which uses SQL tables.
 * We used to store the errors in plain old Lists, and could make an alternative implementation to do so.
 */
public class SQLErrorStorage {

    private static final Logger LOG = LoggerFactory.getLogger(SQLErrorStorage.class);

    private Connection connection;

    private int errorCount; // This serves as a unique ID, so it must persist across multiple validator runs.

    private PreparedStatement insertError;
    private PreparedStatement insertRef;
    private PreparedStatement insertInfo;

    private static final long INSERT_BATCH_SIZE = 500;

    public SQLErrorStorage (Connection connection, boolean createTables) {
        this.connection = connection;
        errorCount = 0;
        if (createTables) createErrorTables();
        else reconnectErrorTables();
        createPreparedStatements();
    }

    public void storeError (NewGTFSError error) {
        try {
            // Insert one row for the error itself
            insertError.setInt(1, errorCount);
            insertError.setString(2, error.type.name());
            insertError.setString(3, error.badValues);
            insertError.addBatch();
            // Insert rows for informational key-value pairs for this error
            // [NOT IMPLEMENTED, USING STRINGS]
            // Insert rows for entities referenced by this error
            for (NewGTFSError.EntityReference ref : error.referencedEntities) {
                insertRef.setInt(1, errorCount);
                insertRef.setString(2, ref.type.getSimpleName());
                // TODO handle missing (-1?) We generate these so we can safely use negative to mean missing.
                insertRef.setInt(3, ref.lineNumber);
                insertRef.setString(4, ref.id);
                // TODO are seq numbers constrained to be positive? If so we don't need to use objects.
                if (ref.sequenceNumber == null) insertRef.setNull(5, Types.INTEGER);
                else insertRef.setInt(5, ref.sequenceNumber);
                insertRef.addBatch();
            }
            if (errorCount % INSERT_BATCH_SIZE == 0) {
                insertError.executeBatch();
                insertRef.executeBatch();
                insertInfo.executeBatch();
            }
            errorCount += 1;
        } catch (SQLException ex) {
            throw new StorageException(ex);
        }
    }

    public int getErrorCount () {
        return errorCount;
    }

    public void finish() {
        try {
            // Execute any remaining batch inserts and commit the transaction.
            insertError.executeBatch();
            insertRef.executeBatch();
            insertInfo.executeBatch();
            connection.commit();
        } catch (SQLException ex) {
            throw new StorageException(ex);
        }
    }

    private void createErrorTables() {
        try {
            Statement statement = connection.createStatement();
            // Order in which tables are dropped matters because of foreign keys.
            statement.execute("drop table if exists error_info");
            statement.execute("drop table if exists error_refs");
            statement.execute("drop table if exists errors");
            statement.execute("create table errors (error_id integer primary key, type varchar, problems varchar)");
            statement.execute("create table error_refs (error_id integer, entity_type varchar, line_number integer, entity_id varchar, sequence_number integer)");
            statement.execute("create table error_info (error_id integer, key varchar, value varchar)");
            connection.commit();
        } catch (SQLException ex) {
            throw new StorageException(ex);
        }
    }

    private void createPreparedStatements () {
        try {
            insertError = connection.prepareStatement("insert into errors values (?, ?, ?)");
            insertRef =   connection.prepareStatement("insert into error_refs values (?, ?, ?, ?, ?)");
            insertInfo =  connection.prepareStatement("insert into error_info values (?, ?, ?)");
        } catch (SQLException ex) {
            throw new StorageException(ex);
        }
    }

    private void reconnectErrorTables () {
        try {
            Statement statement = connection.createStatement();
            statement.execute("select max(error_id) from errors");
            ResultSet resultSet = statement.getResultSet();
            resultSet.next();
            errorCount = resultSet.getInt(1);
            LOG.info("Reconnected to errors table, max error ID is {}.", errorCount);
        } catch (SQLException ex) {
            throw new StorageException("Could not connect to errors table.");
        }
    }

}
