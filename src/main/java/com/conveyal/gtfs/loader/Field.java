package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.error.NewGTFSError;
import com.conveyal.gtfs.error.NewGTFSErrorType;
import com.google.common.collect.ImmutableMap;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLType;
import java.util.Map;
import java.util.Set;

/**
 * Field subclasses process an incoming String that represents a single GTFS CSV field value.
 * The value is validated and converted to its final format.
 * We need to propagate any validation errors up to the caller (where file, line, and column number context are known),
 * Unfortunately Java does not allow multiple return values. There are multiple options.
 * We could emulate multiple return by wrapping the resulting String in another object that combines it with an error type.
 * We could pass a list into every function and the functions could add errors to that list.
 * We could make the Field instances have state, which will also make them single-use and single-thread. They could
 * then accumulate errors as they do their work.
 * We could return an error or list of errors from functions that store the validated value into an array passed in as a parameter.
 * In all cases, to avoid enormous amounts of useless object creation we could re-use error lists and just clear them
 * before each validation operation.
 * However, within the Field implementations, we may need to call private/internal functions that also return multiple
 * values (an error and a modified value).
 */
public abstract class Field {

    public final String name;
    /**
     * Keep any illegal character sequences and their respective replacements here.
     *
     * TODO: Add other illegal character sequences (e.g., HTML tags, comments or escape sequences).
     */
    private static final Map<String, String> ILLEGAL_CHAR_REPLACEMENTS = ImmutableMap.of(
        // Backslashes, newlines, and tabs have special meaning to Postgres. Also, new lines, tabs, and carriage returns are
        // prohibited by GTFS.
        "\\", "\\\\",
        "\t", " ",
        "\n", " ",
        "\r", " "
    );

    final Requirement requirement;
    /**
     * Indicates that this field acts as a foreign key to this referenced table. This is used when checking referential
     * integrity when loading a feed.
     * */
    Table referenceTable = null;
    private boolean shouldBeIndexed;
    private boolean emptyValuePermitted;

    public Field(String name, Requirement requirement) {
        this.name = name;
        this.requirement = requirement;
    }

    /**
     * Check the supplied string to see if it can be parsed as the proper data type.
     * Perform any conversion (I think this is only done for times, to integer numbers of seconds).
     * TODO should we really be converting times and dates to numbers or storing them as strings to simplify things?
     * @param original a non-null String
     * @return a string that is parseable as this field's type, or null if it is not parseable
     */
    public abstract ValidateFieldResult<String> validateAndConvert(String original);

    public abstract Set<NewGTFSError> setParameter(PreparedStatement preparedStatement, int oneBasedIndex, String string);

    public void setNull(PreparedStatement preparedStatement, int oneBasedIndex) throws SQLException {
        preparedStatement.setNull(oneBasedIndex, getSqlType().getVendorTypeNumber());
    }

    public abstract SQLType getSqlType ();

    // Overridden to create exception for "double precision", since its enum value is just called DOUBLE, and ARRAY types,
    // which require "string[]" syntax.
    public String getSqlTypeName () {
        return getSqlType().getName().toLowerCase();
    }

    public String getSqlDeclaration() {
        return String.join(" ", name, getSqlTypeName());
    }

    // TODO test for input with tabs, newlines, carriage returns, and slashes in it.
    protected static ValidateFieldResult<String> cleanString (String string) {
        // Initially set string result.
        ValidateFieldResult<String> result = new ValidateFieldResult<>(string);
        // Check for illegal character sequences and replace them as needed.
        for (String illegalChar: ILLEGAL_CHAR_REPLACEMENTS.keySet()) {
            // String.contains is significantly faster than using a regex or replace, and has barely any speed impact.
            if (string.contains(illegalChar)) {
                result.clean = string.replace(illegalChar, ILLEGAL_CHAR_REPLACEMENTS.get(illegalChar));
                // We don't know the Table or line number here, but when the errors bubble up, these values should be
                // assigned to the errors.
                result.errors.add(NewGTFSError.forFeed(NewGTFSErrorType.ILLEGAL_FIELD_VALUE, illegalChar));
            }
        }
        return result;
    }

    protected static ValidateFieldResult<String> cleanString (ValidateFieldResult<String> previousResult) {
        ValidateFieldResult<String> result = ValidateFieldResult.from(previousResult);
        for (String illegalChar: ILLEGAL_CHAR_REPLACEMENTS.keySet()) {
            // String.contains is significantly faster than using a regex or replace, and has barely any speed impact.
            if (previousResult.clean.contains(illegalChar)) {
                result.clean = previousResult.clean.replace(illegalChar, ILLEGAL_CHAR_REPLACEMENTS.get(illegalChar));
                // We don't know the Table or line number here, but when the errors bubble up, these values should be
                // assigned to the errors.
                result.errors.add(NewGTFSError.forFeed(NewGTFSErrorType.ILLEGAL_FIELD_VALUE, illegalChar));
            }
        }
        return result;
    }

    /**
     * Generally any required field should be present on every row.
     * TODO override this method for exceptions, e.g. arrival and departure can be missing though the field must be present
     */
    public boolean missingRequired (String string) {
        return  (string == null || string.isEmpty()) && this.isRequired();
    }

    public boolean isRequired () {
        return this.requirement == Requirement.REQUIRED;
    }

    /**
     * More than one foreign reference should not be created on the same table to the same foreign table. This is what
     * allows us to embed updates to a sub-table in nested JSON because this creates a many-to-one reference instead of
     * a many-to-many reference.
     */
    public boolean isForeignReference () {
        return this.referenceTable != null;
    }

    /**
     * Fluent method that indicates that a newly constructed field should be indexed after the table is loaded.
     * FIXME: should shouldBeIndexed be determined based on presence of referenceTable?
     * @return this same Field instance, which allows constructing and assigning the instance in the same statement.
     */
    public Field indexThisColumn () {
        this.shouldBeIndexed = true;
        return this;
    }

    public boolean shouldBeIndexed() {
        return shouldBeIndexed;
    }

    /**
     * Fluent method indicates that this field is a reference to an entry in the table provided as an argument.
     * @param table
     * @return this same Field instance
     */
    public Field isReferenceTo(Table table) {
        this.referenceTable = table;
        return this;
    }

    /**
     * Fluent method to permit empty values for this field. Used for cases like fare_attributes#transfers, where empty
     * values are OK on a required field.
     * @return this same Field instance, which allows constructing and assigning the instance in the same statement.
     */
    public Field permitEmptyValue () {
        this.emptyValuePermitted = true;
        return this;
    }

    /**
     * Check if empty values are permitted for this field.
     */
    public boolean isEmptyValuePermitted() {
        return this.emptyValuePermitted;
    }

    /**
     * Get the expression used to select this column from the database based on the prefix.  The csvOutput parameter is
     * needed in overriden method implementations that have special ways of outputting certain fields.  The prefix
     * parameter is assumed to be either null or a string in the format: `schema.`
     */
    public String getColumnExpression(String prefix, boolean csvOutput) {
        return prefix != null ? String.format("%s%s", prefix, name) : name;
    }
}
