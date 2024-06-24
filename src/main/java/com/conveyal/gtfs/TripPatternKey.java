package com.conveyal.gtfs;

import com.conveyal.gtfs.model.StopTime;
import gnu.trove.list.TDoubleList;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.list.array.TIntArrayList;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.conveyal.gtfs.model.Entity.INT_MISSING;

/**
 * Used as a map key when grouping trips by stop pattern. Note that this includes the routeId, so the same sequence of
 * stops on two different routes makes two different patterns.
 * These objects are not intended for use outside the grouping process.
 */
public class TripPatternKey {

    public String routeId;
    public List<Boolean> isFlexStop = new ArrayList<>();
    public List<String> stops = new ArrayList<>();
    public TIntList pickupTypes = new TIntArrayList();
    public TIntList dropoffTypes = new TIntArrayList();
    // Flex additions included in equality check.
    public TIntList start_pickup_drop_off_window = new TIntArrayList();
    public TIntList end_pickup_drop_off_window = new TIntArrayList();

    // Note, the lists below are not used in the equality check.
    public TIntList arrivalTimes = new TIntArrayList();
    public TIntList departureTimes = new TIntArrayList();
    public TIntList timepoints = new TIntArrayList();
    public List<String> stopHeadsigns = new ArrayList<>();
    public TIntList continuous_pickup = new TIntArrayList();
    public TIntList continuous_drop_off = new TIntArrayList();
    public TDoubleList shapeDistances = new TDoubleArrayList();

    // Flex additions
    public List<String> locationGroupIds = new ArrayList<>();
    public List<String> locationIds = new ArrayList<>();
    public List<String> pickup_booking_rule_id = new ArrayList<>();
    public List<String> drop_off_booking_rule_id = new ArrayList<>();

    public TripPatternKey (String routeId) {
        this.routeId = routeId;
    }

    public void addStopTime (StopTime st) {
        isFlexStop.add((st.stop_id == null));
        stops.add(st.stop_id);
        locationGroupIds.add(st.location_group_id);
        locationIds.add(st.location_id);
        pickupTypes.add(resolvePickupOrDropOffType(st.pickup_type));
        dropoffTypes.add(resolvePickupOrDropOffType(st.drop_off_type));
        start_pickup_drop_off_window.add(st.start_pickup_drop_off_window);
        end_pickup_drop_off_window.add(st.end_pickup_drop_off_window);
        // Note, the items listed below are not used in the equality check.
        arrivalTimes.add(st.arrival_time);
        departureTimes.add(st.departure_time);
        timepoints.add(st.timepoint);
        stopHeadsigns.add(st.stop_headsign);
        shapeDistances.add(st.shape_dist_traveled);
        continuous_pickup.add(st.continuous_pickup);
        continuous_drop_off.add(st.continuous_drop_off);
        pickup_booking_rule_id.add(st.pickup_booking_rule_id);
        drop_off_booking_rule_id.add(st.drop_off_booking_rule_id);
    }

    /**
     * Resolves omitted (INT_MISSING) values for pickup and drop-off types to the default value (0 - regular)
     * for the purposes of determining whether entries in stop_times correspond to the same trip pattern(s).
     * @param pickupOrDropOffType the pickup or drop-off type to resolve.
     * @return 0 if pickupOrDropOffType is 0 or INT_MISSING, else pickupOrDropOffType.
     */
    private int resolvePickupOrDropOffType(int pickupOrDropOffType) {
        return pickupOrDropOffType == INT_MISSING ? 0 : pickupOrDropOffType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TripPatternKey that = (TripPatternKey) o;

        if (!Objects.equals(dropoffTypes, that.dropoffTypes)) return false;
        if (!Objects.equals(pickupTypes, that.pickupTypes)) return false;
        if (!Objects.equals(routeId, that.routeId)) return false;
        if (!Objects.equals(stops, that.stops)) return false;
        if (!Objects.equals(locationGroupIds, that.locationGroupIds)) return false;
        if (!Objects.equals(locationIds, that.locationIds)) return false;
        if (!Objects.equals(start_pickup_drop_off_window, that.start_pickup_drop_off_window)) return false;
        if (!Objects.equals(end_pickup_drop_off_window, that.end_pickup_drop_off_window)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            routeId, stops, locationGroupIds, locationIds, pickupTypes, dropoffTypes, start_pickup_drop_off_window, end_pickup_drop_off_window
        );
    }
}
