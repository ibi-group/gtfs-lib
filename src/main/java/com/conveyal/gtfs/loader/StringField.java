package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.error.NewGTFSError;
import com.conveyal.gtfs.storage.StorageException;

import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLType;
import java.util.Set;

/**
 * Created by abyrd on 2017-03-31
 */
public class StringField extends Field {

    public StringField (String name, Requirement requirement) {
        super(name, requirement);
    }

    /** Check that a string can be properly parsed and is in range. */
    public ValidateFieldResult validateAndConvert (String string) {
        return cleanString(string);
    }

    public Set<NewGTFSError> setParameter(PreparedStatement preparedStatement, int oneBasedIndex, String string) {
        try {
            ValidateFieldResult<String> result = validateAndConvert(string);
            preparedStatement.setString(oneBasedIndex, result.clean);
            return result.errors;
        } catch (Exception ex) {
            throw new StorageException(ex);
        }
    }

    @Override
    public SQLType getSqlType() {
        return JDBCType.VARCHAR;
    }

}
