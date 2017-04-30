package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.error.GTFSError;
import com.conveyal.gtfs.error.SQLErrorStorage;
import com.conveyal.gtfs.model.*;
import com.conveyal.gtfs.validator.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

/**
 * This connects to an SQL RDBMS containing GTFS data and lets you fetch things out of it.
 *
 * Created by abyrd on 2017-04-04
 */
public class Feed {

    private static final Logger LOG = LoggerFactory.getLogger(Feed.class);

    private final Connection connection;

    public final TableReader<Route> routes;
    public final TableReader<Stop>  stops;
    public final TableReader<Trip>  trips;
    public final TableReader<ShapePoint> shapePoints;
    public final TableReader<StopTime>   stopTimes;

    /* A place to accumulate errors while the feed is loaded. Tolerate as many errors as possible and keep on loading. */
    // TODO remove this and use only NewGTFSErrors in Validators, loaded into a JDBC table
    public final List<GTFSError> errors = new ArrayList<>();

    /**
     * Create a feed that reads tables over a JDBC connection. The connection should already be set to the right
     * schema within the database.
     */
    public Feed (Connection connection) {
        this.connection = connection; // Should probably be a connection source.
        routes      = new JDBCTableReader(Table.ROUTES,     connection, EntityPopulator.ROUTE);
        stops       = new JDBCTableReader(Table.STOPS,      connection, EntityPopulator.STOP);
        trips       = new JDBCTableReader(Table.TRIPS,      connection, EntityPopulator.TRIP);
        shapePoints = new JDBCTableReader(Table.SHAPES,     connection, EntityPopulator.SHAPE_POINT);
        stopTimes   = new JDBCTableReader(Table.STOP_TIMES, connection, EntityPopulator.STOP_TIME);
    }

    /**
     * This will return a Feed object for the given GTFS feed file. It will load the data from the file into the Feed
     * object as needed, but will first look for a cached database file in the same directory and with the same name as
     * the GTFS feed file. This speeds up uses of the feed after the first time.
     */
    public Feed loadOrUseCached (String gtfsFilePath) {
        return null;
    }

    public static void main (String[] params) {
        ConnectionSource connectionSource = new ConnectionSource(ConnectionSource.POSTGRES_LOCAL_URL);
        Connection connection = connectionSource.getConnection(null);
        Feed feed = new Feed(connection);

        feed.validate();

        // TODO make this into a unit test
        if (params.length > 0 && params[0].equalsIgnoreCase("test")) {
            LOG.info("Start.");
            double x = 0;
            for (Route route : feed.routes) {
                x += route.route_type;
            }
            LOG.info("Done. {}", x);
            for (Stop stop : feed.stops) {
                x += stop.stop_lat;
            }
            LOG.info("Done. {}", x);
            for (Trip trip : feed.trips) {
                x += trip.direction_id;
            }
            LOG.info("Done. {}", x);
    //        for (ShapePoint shapePoint : feed.shapePoints) {
    //            x += shapePoint.shape_dist_traveled;
    //        }
    //        LOG.info("Done. {}", x);
            // It takes about 25 seconds to iterate over all stop times,
            // as opposed to 83 seconds to iterate over all stop times in order for each trip.
            for (StopTime stopTime : feed.stopTimes) {
                x += stopTime.shape_dist_traveled;
            }
            LOG.info("Done. {}", x);
            return;
        }
    }

    private void validate (SQLErrorStorage errorStorage, FeedValidator... feedValidators) {
        long validationStartTime = System.currentTimeMillis();
        for (FeedValidator feedValidator : feedValidators) {
            try {
                LOG.info("Running {}.", feedValidator.getClass().getSimpleName());
                int errorCountBefore = errorStorage.getErrorCount();
                feedValidator.validate();
                LOG.info("Found {} errors.", errorStorage.getErrorCount() - errorCountBefore);
            } catch (Exception e) {
                LOG.error("{} failed.", feedValidator.getClass().getSimpleName());
                LOG.error(e.toString());
                e.printStackTrace();
            }
        }
        errorStorage.finish();
        long validationEndTime = System.currentTimeMillis();
        long totalValidationTime = validationEndTime - validationStartTime;
        LOG.info("{} validators completed in {} milliseconds.", feedValidators.length, totalValidationTime);
    }

    public void validate () {
        // Error tables should already be present from the initial load.
        SQLErrorStorage errorStorage = new SQLErrorStorage(connection, false);
        validate (errorStorage,
            new MisplacedStopValidator(this, errorStorage),
            new DuplicateStopsValidator(this, errorStorage),
            new TimeZoneValidator(this, errorStorage),
            new NewTripTimesValidator(this, errorStorage),
            new NamesValidator(this, errorStorage)
        );
    }

}
