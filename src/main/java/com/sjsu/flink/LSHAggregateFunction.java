package com.sjsu.flink;

import org.apache.flink.table.functions.AggregateFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.HashMap; // Import if needed
import java.util.Map; // Import if needed
import java.util.HashSet; // Still needed for getFeatures() return type internally


/**
 * A Flink AggregateFunction to generate a simple pattern hash for train updates.
 * (Uses Map in Accumulator, includes TypeInformation overrides)
 */
public class LSHAggregateFunction extends AggregateFunction<Long, LSHAccumulator> {
    private static final long serialVersionUID = 3L; // Incremented version ID
    private static final Logger LOG = LoggerFactory.getLogger(LSHAggregateFunction.class);

    // --- Required methods for AggregateFunction ---

    @Override
    public LSHAccumulator createAccumulator() {
        return new LSHAccumulator();
    }

    @Override
    public Long getValue(LSHAccumulator acc) {
        if (acc == null || acc.isEmpty()) {
             LOG.debug("Accumulator is null or feature map is empty, returning 0 hash.");
             return 0L;
        }

        // Get features from the map's keyset
        List<String> sortedFeatures = new ArrayList<>(acc.getFeatures());
        Collections.sort(sortedFeatures);

        StringBuilder combinedFeatures = new StringBuilder();
        for (String feature : sortedFeatures) {
            combinedFeatures.append(feature).append("##");
        }

        long hashCode = Objects.hashCode(combinedFeatures.toString());

        LOG.debug("Generated hash {} from feature set size: {}", hashCode, sortedFeatures.size());
        return hashCode;
    }

    // --- Accumulate Method ---
    public void accumulate(LSHAccumulator acc, String sch_tpl, String ts_tpl, String event_type) {
        if (acc == null) { // Add null check for safety
             LOG.warn("Accumulator is null in accumulate, skipping.");
             return;
         }
        String safe_sch_tpl = sch_tpl == null ? "NULL" : sch_tpl;
        String safe_ts_tpl = ts_tpl == null ? "NULL" : ts_tpl;
        String safe_event_type = event_type == null ? "NULL" : event_type;

        String featureString = safe_sch_tpl + "|" + safe_ts_tpl + "|" + safe_event_type;

        // Use the Map-based addFeature method
        acc.addFeature(featureString);
        LOG.trace("Accumulated feature: {}", featureString);
    }

    // --- Merge Method ---
     public void merge(LSHAccumulator acc, Iterable<LSHAccumulator> it) {
         if (acc == null) { // Add null check
             // Cannot merge into a null accumulator
             LOG.error("Attempted to merge into a null accumulator!");
             // This case ideally shouldn't happen if createAccumulator works correctly
             return;
         }
        LOG.debug("Merging accumulators...");
        for (LSHAccumulator otherAcc : it) {
            if (otherAcc != null && !otherAcc.isEmpty()) {
                 // Use the Map-based mergeFeatures method, passing the other map
                acc.mergeFeatures(otherAcc.getFeatureMap());
                 LOG.trace("Merged features from another accumulator. Current map size: {}", acc.getFeatures().size());
            }
        }
         LOG.debug("Finished merging. Final map size: {}", (acc.getFeatures() != null ? acc.getFeatures().size() : 0));
    }

    // --- *** Type Information Overrides (Keep these as they are) *** ---

    @Override
    public TypeInformation<Long> getResultType() {
        return Types.LONG;
    }

    @Override
    public TypeInformation<LSHAccumulator> getAccumulatorType() {
        // We still tell Flink it's a POJO, hoping it can now infer the Map field correctly
        return Types.POJO(LSHAccumulator.class);
    }

    // --- Optional Reset Accumulator Method ---
     public void resetAccumulator(LSHAccumulator acc) {
         if (acc != null) {
             // Reset by setting to a new empty map
            acc.setFeatureMap(new HashMap<>());
         }
     }

    // Optional Retract Method... (as before)
}