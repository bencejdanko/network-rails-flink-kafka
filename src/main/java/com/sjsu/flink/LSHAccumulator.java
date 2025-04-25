package com.sjsu.flink;

import java.util.HashMap; // Use HashMap
import java.util.Map;
import java.util.Set; // Still needed for keySet() return type
import java.io.Serializable;
import java.util.HashSet;

/**
 * Accumulator for the LSHAggregateFunction.
 * Stores the set of unique feature strings observed within a window for a specific key (rid),
 * using a Map<String, Boolean> for better Flink type compatibility within POJOs.
 */
public class LSHAccumulator implements Serializable {
    private static final long serialVersionUID = 3L; // Incremented version ID

    // Use a Map<String, Boolean> instead of Set<String>
    private Map<String, Boolean> featureMap;

    // Public no-argument constructor (required for POJOs)
    public LSHAccumulator() {
        this.featureMap = new HashMap<>();
    }

    // Public getter for the map
    public Map<String, Boolean> getFeatureMap() {
        return featureMap;
    }

    // Public setter for the map
    public void setFeatureMap(Map<String, Boolean> featureMap) {
        this.featureMap = featureMap;
    }

    // --- Convenience methods updated for Map ---

    public void addFeature(String feature) {
        if (this.featureMap == null) {
            this.featureMap = new HashMap<>();
        }
        this.featureMap.put(feature, true); // Add feature as key
    }

    // Returns the Set of keys (features)
    public Set<String> getFeatures() {
         if (this.featureMap == null) {
             return new HashSet<>(); // Return empty set if map is null
         }
        return featureMap.keySet();
    }

     public boolean isEmpty() {
        return this.featureMap == null || this.featureMap.isEmpty();
    }

     // Merges features from another accumulator's map
     public void mergeFeatures(Map<String, Boolean> otherFeaturesMap) {
        if (this.featureMap == null) {
            this.featureMap = new HashMap<>();
        }
        if (otherFeaturesMap != null) {
            this.featureMap.putAll(otherFeaturesMap); // putAll merges the maps
        }
    }
}