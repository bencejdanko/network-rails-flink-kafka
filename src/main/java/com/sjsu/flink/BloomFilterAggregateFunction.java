package com.sjsu.flink;

import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.BitSet; // Keep this import if used internally, though maybe not needed now

/**
 * Flink Aggregate Function to create a Bloom Filter of visited locations (ts_tpl)
 * for each train (rid) within a window. (Accumulator stores state as Base64 String).
 */
public class BloomFilterAggregateFunction extends AggregateFunction<String, BloomFilterAccumulator> {
    private static final long serialVersionUID = 2L; // Incremented version
    private static final Logger LOG = LoggerFactory.getLogger(BloomFilterAggregateFunction.class);

    private final int filterSize; // m
    private final int numHashes;  // k
    private final long seed;      // seed for hash functions

    public BloomFilterAggregateFunction(int filterSize, int numHashes, long seed) {
        this.filterSize = filterSize;
        this.numHashes = numHashes;
        this.seed = seed;
        LOG.info("Initialized BloomFilterAggregateFunction: size={}, hashes={}, seed={}", filterSize, numHashes, seed);
    }

    @Override
    public BloomFilterAccumulator createAccumulator() {
        BloomFilterAccumulator acc = new BloomFilterAccumulator();
        acc.initialize(this.filterSize, this.numHashes, this.seed);
        return acc;
    }

    @Override
    public String getValue(BloomFilterAccumulator acc) {
        if (acc == null) {
            LOG.warn("Accumulator is null in getValue. Returning null.");
            return null;
        }
        // Return the Base64 string stored in the accumulator
         String base64 = acc.bitSetBase64;
         LOG.trace("Returning Base64 Bloom Filter state");
         return base64; // Return the stored state directly
    }

    // Accumulate method remains the same - operates via acc.add()
    public void accumulate(BloomFilterAccumulator acc, String locationTpl) {
        if (acc == null) {
            LOG.warn("Accumulator is null in accumulate. Skipping.");
            return;
        }
        if (locationTpl == null) {
            LOG.trace("Skipping null locationTpl.");
            return;
        }
        int hashCode = Objects.hashCode(locationTpl);
        acc.add(hashCode); // Accumulator internal logic handles transient BitSet and updates Base64
        LOG.trace("Accumulated location '{}' (hash {})", locationTpl, hashCode);
    }

    // Merge method now operates on the Base64 state from other accumulators
    public void merge(BloomFilterAccumulator acc, Iterable<BloomFilterAccumulator> it) {
         if (acc == null) {
             LOG.error("Cannot merge into a null accumulator!");
             return;
         }

        LOG.debug("Merging Bloom Filter accumulators...");
        for (BloomFilterAccumulator otherAcc : it) {
            if (otherAcc != null && otherAcc.bitSetBase64 != null) {
                // Call the accumulator's merge method, passing the Base64 string
                acc.merge(otherAcc.bitSetBase64);
                LOG.trace("Merged filter state.");
            }
        }
         LOG.debug("Finished merging Bloom Filters.");
    }

    // --- Type Information (No change needed) ---

    @Override
    public TypeInformation<String> getResultType() {
        return Types.STRING;
    }

    @Override
    public TypeInformation<BloomFilterAccumulator> getAccumulatorType() {
        return Types.POJO(BloomFilterAccumulator.class);
    }

     // Reset Accumulator (No change needed)
     public void resetAccumulator(BloomFilterAccumulator acc) {
         if (acc != null) {
             acc.initialize(this.filterSize, this.numHashes, this.seed);
         }
     }
}