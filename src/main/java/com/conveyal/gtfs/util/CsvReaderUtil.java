package com.conveyal.gtfs.util;

import com.conveyal.gtfs.error.NewGTFSError;
import com.conveyal.gtfs.error.SQLErrorStorage;
import com.conveyal.gtfs.loader.Table;
import com.conveyal.gtfs.model.Route;
import com.conveyal.gtfs.model.RouteNetwork;
import com.conveyal.gtfs.model.Stop;
import com.csvreader.CsvReader;
import org.apache.commons.io.input.BOMInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static com.conveyal.gtfs.error.NewGTFSErrorType.TABLE_IN_SUBDIRECTORY;
import static com.conveyal.gtfs.loader.Table.getTableFileNameWithExtension;
import static com.conveyal.gtfs.model.Stop.STOPS_FILE_NAME;

public class CsvReaderUtil {

    private static final Logger LOG = LoggerFactory.getLogger(CsvReaderUtil.class);

    private CsvReaderUtil() {
        throw new IllegalStateException("Utility class");
    }

    /**
     * In GTFS feeds, all files are supposed to be in the root of the zip file, but feed producers often put them
     * in a subdirectory. This function will search subdirectories if the entry is not found in the root.
     * It records an error if the entry is in a subdirectory (as long as errorStorage is not null).
     * It then creates a {@link CsvReader} for that table if it's found.
     */
    public static CsvReader getCsvReaderAccordingToFileName(Table table, ZipFile zipFile, SQLErrorStorage sqlErrorStorage) {
        final String tableFileName = getTableFileNameWithExtension(table.name);
        ZipEntry entry = zipFile.getEntry(tableFileName);

        if (entry == null) {
            entry = getEntryFromZipFile(zipFile, tableFileName);

            if (entry != null && sqlErrorStorage != null) {
                sqlErrorStorage.storeError(NewGTFSError.forTable(table, TABLE_IN_SUBDIRECTORY));
            }
            if (entry == null) {
                return null;
            }
        }

        try {
            List<String> errors = new ArrayList<>();
            CsvReader csvReader = getCsvReaderAccordingToFileName(tableFileName, zipFile, entry, errors);
            if (csvReader == null) {
                return null;
            }
            if (!errors.isEmpty() && sqlErrorStorage != null) {
                errors.forEach(error -> sqlErrorStorage.storeError(NewGTFSError.forFeed(null, error)));
            }
            // Don't skip empty records. This is set to true by default on CsvReader. We want to check for empty records
            // during table load, so that they are logged as validation issues (WRONG_NUMBER_OF_FIELDS).
            csvReader.setSkipEmptyRecords(false);
            csvReader.readHeaders();
            return csvReader;
        } catch (IOException e) {
            LOG.error("Exception while opening zip entry: {}", entry, e);
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Create a {@link CsvReader} depending on the table to be loaded. If the table is location related unpack the data
     * first according to each individual case and load into a CSV reader, else, read the table contents directly into
     * the CSV reader.
     */
    public static CsvReader getCsvReaderAccordingToFileName(
        String tableFileName,
        ZipFile zipFile,
        ZipEntry entry,
        List<String> errors
    ) throws IOException {
        if (tableFileName.equals(STOPS_FILE_NAME)) {
            return getCsvReaderFromStopsFile(zipFile, entry, errors);
        } else if (tableFileName.equals(Route.ROUTE_FILE_NAME)) {
            return getCsvReaderFromRoutesFile(zipFile, entry, errors);
        } else {
            return getCsvReaderFromFile(zipFile, entry);
        }
    }

    /**
     * If the feed contains stop areas extract and merge with stops. If not, just return stops.
     */
    private static CsvReader getCsvReaderFromStopsFile(
        ZipFile zipFile,
        ZipEntry parentEntry,
        List<String> errors
    ) throws IOException {
        ZipEntry zipEntry = getEntryFromZipFile(zipFile, Stop.STOP_AREAS_FILE_NAME);

        CsvReader reader = getCsvReaderFromFile(zipFile, parentEntry);
        if (zipEntry != null) {
            reader.setSkipEmptyRecords(false);
            reader.readHeaders();
            CsvReader csvReader = getCsvReaderForFile(zipFile, zipEntry, errors, Stop.STOP_AREAS_NUMBER_OF_HEADERS);
            if (csvReader != null) {
                Map<String, Set<String>> zipData = Stop.groupStopAreaIds(csvReader, errors);
                if (!zipData.isEmpty()) {
                    return Stop.getCsvReaderForStopsWithStopAreas(reader, zipData);
                }
            }
        }
        // No stop areas, provide just the stops.
        return reader;
    }

    /**
     * If the feed contains route networks extract and merge with routes. If not, just return routes.
     */
    private static CsvReader getCsvReaderFromRoutesFile(
        ZipFile zipFile,
        ZipEntry parentEntry,
        List<String> errors
    ) throws IOException {
        ZipEntry zipEntry = getEntryFromZipFile(zipFile, RouteNetwork.ROUTE_NETWORK_FILE_NAME);

        CsvReader reader = getCsvReaderFromFile(zipFile, parentEntry);
        if (zipEntry != null) {
            reader.setSkipEmptyRecords(false);
            reader.readHeaders();
            CsvReader csvReader = getCsvReaderForFile(zipFile, zipEntry, errors, RouteNetwork.ROUTE_NETWORK_NUMBER_OF_HEADERS);
            if (csvReader != null) {
                Map<String, Set<String>> zipData = Route.groupRouteNetworkIds(csvReader, errors);
                if (!zipData.isEmpty()) {
                    return Route.getCsvReaderForRoutesWithRouteNetworks(reader, zipData);
                }
            }
        }
        // No route networks, provide just the routes.
        return reader;
    }

    /**
     * Create a {@link CsvReader} from file and check that the number of headers meets the expected number of headers.
     */
    private static CsvReader getCsvReaderForFile(
        ZipFile zipFile,
        ZipEntry entry,
        List<String> errors,
        int numOfHeaders
    ) throws IOException {
        CsvReader csvReader = getCsvReaderFromFile(zipFile, entry);
        csvReader.setSkipEmptyRecords(false);
        csvReader.readHeaders();
        String[] headers = csvReader.getHeaders();
        if (headers.length != numOfHeaders) {
            String message = String.format(
                "Wrong number of headers, expected=%d; found=%d in %s.",
                numOfHeaders,
                headers.length,
                entry.getName()
            );
            LOG.warn(message);
            if (errors != null) {
                errors.add(message);
            }
            return null;
        }
        return csvReader;
    }

    /**
     * Create a {@link CsvReader} from the provided file. Warning: Both input streams have to remain open so
     * downstream processing using the {@link CsvReader} will continue to function. In line with this, the downstream
     * processes will close the {@link CsvReader} and with it both input streams.
     */
    public static CsvReader getCsvReaderFromFile(ZipFile zipFile, ZipEntry entry) throws IOException {
        InputStream zipInputStream = zipFile.getInputStream(entry);
        // Skip any byte order mark that may be present. Files must be UTF-8,
        // but the GTFS spec says that "files that include the UTF byte order mark are acceptable".
        InputStream bomInputStream = new BOMInputStream(zipInputStream);
        return new CsvReader(bomInputStream, ',', StandardCharsets.UTF_8);
    }

    /**
     * Confirm if the current row being processed has the expected number of columns.
     */
    public static boolean hasExpectedNumberOfColumns(CsvReader csvReader, List<String> errors, int expectedNumberOfColumns) {
        int lineNumber = ((int) csvReader.getCurrentRecord()) + 2;
        if (csvReader.getColumnCount() != expectedNumberOfColumns) {
            String message = String.format("Wrong number of columns for line number=%d; expected=%d; found=%d.",
                lineNumber,
                expectedNumberOfColumns,
                csvReader.getColumnCount()
            );
            LOG.warn(message);
            if (errors != null) {
                errors.add(message);
            }
            return false;
        }
        return true;
    }

    /**
     * Get entry and allow for the file being in a subdirectory.
     */
    public static ZipEntry getEntryFromZipFile(ZipFile zipFile, String fileName) {
        ZipEntry entry = zipFile.getEntry(fileName);
        if (entry == null) {
            Enumeration<? extends ZipEntry> entries = zipFile.entries();
            // check if table is contained within subdirectory
            while (entries.hasMoreElements()) {
                ZipEntry e = entries.nextElement();
                if (Paths.get(e.getName()).getFileName().toString().equals(fileName)) {
                    entry = e;
                    break;
                }
            }
        }
        return entry;
    }
}