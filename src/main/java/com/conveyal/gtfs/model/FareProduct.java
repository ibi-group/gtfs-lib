package com.conveyal.gtfs.model;

import com.conveyal.gtfs.GTFSFeed;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.stream.Collectors;

public class FareProduct extends Entity {

    private static final long serialVersionUID = -5678890165823575940L;

    public String fare_product_id;
    public String fare_product_name;
    public String fare_media_id;
    public double amount;
    public String currency;
    public String feed_id;

    public static final String TABLE_NAME = "fare_products";
    public static final String FARE_PRODUCT_ID_COLUMN_NAME = "fare_product_id";
    public static final String FARE_PRODUCT_NAME_COLUMN_NAME = "fare_product_name";
    public static final String FARE_MEDIA_ID_COLUMN_NAME = "fare_media_id";
    public static final String AMOUNT_COLUMN_NAME = "amount";
    public static final String CURRENCY_COLUMN_NAME = "currency";


    @Override
    public String getId () {
        return createPrimaryKey(fare_product_id, fare_media_id);
    }

    /**
     * Sets the parameters for a prepared statement following the parameter order defined in
     * {@link com.conveyal.gtfs.loader.Table#FARE_PRODUCTS}. JDBC prepared statement parameters use a one-based index.
     */
    @Override
    public void setStatementParameters(PreparedStatement statement, boolean setDefaultId) throws SQLException {
        int oneBasedIndex = 1;
        if (!setDefaultId) statement.setInt(oneBasedIndex++, id);
        statement.setString(oneBasedIndex++, fare_product_id);
        statement.setString(oneBasedIndex++, fare_product_name);
        statement.setString(oneBasedIndex++, fare_media_id);
        setDoubleParameter(statement, oneBasedIndex++, amount);
        statement.setString(oneBasedIndex, currency);
    }

    public static class Loader extends Entity.Loader<FareProduct> {

        public Loader(GTFSFeed feed) {
            super(feed, TABLE_NAME);
        }

        @Override
        protected boolean isRequired() {
            return false;
        }

        @Override
        public void loadOneRow() throws IOException {
            FareProduct fareProduct = new FareProduct();
            fareProduct.id = row + 1; // offset line number by 1 to account for 0-based row index
            fareProduct.fare_product_id = getStringField(FARE_PRODUCT_ID_COLUMN_NAME, true);
            fareProduct.fare_product_name = getStringField(FARE_PRODUCT_NAME_COLUMN_NAME, false);
            fareProduct.fare_media_id = getStringField(FARE_MEDIA_ID_COLUMN_NAME, false);
            fareProduct.amount = getDoubleField(AMOUNT_COLUMN_NAME, true, 0.0, Double.MAX_VALUE);
            fareProduct.currency = getStringField(CURRENCY_COLUMN_NAME, true);
            fareProduct.feed = feed;
            fareProduct.feed_id = feed.feedId;
            feed.fare_products.put(fareProduct.getId(), fareProduct);

            /*
              Check referential integrity without storing references.
             */
            getRefField(FARE_MEDIA_ID_COLUMN_NAME, false, feed.fare_medias);
        }

    }

    public static class Writer extends Entity.Writer<FareProduct> {
        public Writer(GTFSFeed feed) {
            super(feed, TABLE_NAME);
        }

        @Override
        public void writeHeaders() throws IOException {
            writer.writeRecord(new String[] {
                FARE_PRODUCT_ID_COLUMN_NAME,
                FARE_PRODUCT_NAME_COLUMN_NAME,
                FARE_MEDIA_ID_COLUMN_NAME,
                AMOUNT_COLUMN_NAME,CURRENCY_COLUMN_NAME
            });
        }

        @Override
        public void writeOneRow(FareProduct fareProduct) throws IOException {
            writeStringField(fareProduct.fare_product_id);
            writeStringField(fareProduct.fare_product_name);
            writeStringField(fareProduct.fare_media_id);
            writeDoubleField(fareProduct.amount);
            writeStringField(fareProduct.currency);
            endRecord();
        }

        @Override
        public Iterator<FareProduct> iterator() {
            return this.feed.fare_products.values().iterator();
        }
    }
}


