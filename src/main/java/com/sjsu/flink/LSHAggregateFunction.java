package com.sjsu.flink; // Use your package name

import org.apache.flink.table.functions.AggregateFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes; // For ARRAY type hint

import java.util.ArrayList;
import java.util.Arrays; // For Arrays.fill
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.HashMap;
import java.util.Map;
import java.util.Random; // For generating hash function parameters

/**
 * A Flink AggregateFunction to generate a MinHash signature for train update patterns.
 */
public class LSHAggregateFunction extends AggregateFunction<List<Long>, LSHAccumulator> { // Return type changed to List<Long>
    private static final long serialVersionUID = 4L; // Incremented version ID
    private static final Logger LOG = LoggerFactory.getLogger(LSHAggregateFunction.class);

    // --- MinHash Configuration ---
    private final int numHashFunctions;
    private final long[] hashA; // 'a' coefficients for hash functions
    private final long[] hashB; // 'b' coefficients for hash functions
    private static final long LARGE_PRIME = (1L << 31) - 1; // A large prime (Mersenne prime 2^31 - 1)

    /**
     * Constructor for MinHash Aggregate Function.
     *
     * @param numHashFunctions The desired number of hash functions (signature length).
     * @param seed             A seed for generating reproducible hash functions.
     */
    public LSHAggregateFunction(int numHashFunctions, long seed) {
        if (numHashFunctions <= 0) {
            throw new IllegalArgumentException("Number of hash functions must be positive.");
        }
        this.numHashFunctions = numHashFunctions;
        this.hashA = new long[numHashFunctions];
        this.hashB = new long[numHashFunctions];

        Random rand = new Random(seed); // Use a seed for reproducibility
        for (int i = 0; i < numHashFunctions; i++) {
            // Ensure 'a' is non-zero, positive
            this.hashA[i] = rand.nextInt(Integer.MAX_VALUE - 1) + 1;
            this.hashB[i] = rand.nextInt(Integer.MAX_VALUE);
        }
        LOG.info("Initialized LSHAggregateFunction with {} hash functions (Seed: {}).", numHashFunctions, seed);
    }

    /**
     * Default constructor (less ideal for reproducibility, uses random seed).
     * Consider requiring the seeded constructor.
     * @param numHashFunctions The desired number of hash functions.
     */
     public LSHAggregateFunction(int numHashFunctions) {
         this(numHashFunctions, System.currentTimeMillis()); // Use current time as seed
         LOG.warn("LSHAggregateFunction created without explicit seed. Hash functions may vary across runs/restarts.");
     }


    // --- Required methods for AggregateFunction ---

    @Override
    public LSHAccumulator createAccumulator() {
        return new LSHAccumulator(); // Accumulator structure remains the same
    }

    @Override
    public List<Long> getValue(LSHAccumulator acc) {
        if (acc == null || acc.isEmpty()) {
            LOG.debug("Accumulator is null or feature map is empty, returning empty signature.");
            // Return an empty list or null, depending on desired downstream handling
            return Collections.emptyList();
        }

        // Initialize MinHash signature array with maximum values
        long[] minHashes = new long[this.numHashFunctions];
        Arrays.fill(minHashes, Long.MAX_VALUE);

        // Iterate through all unique features gathered in the accumulator
        for (String feature : acc.getFeatures()) {
            // Get a base hash code for the feature string
            // Using Objects.hashCode which handles nulls, though our features shouldn't be null here
             int baseHashCode = Objects.hashCode(feature);

            // Apply each hash function and update the minimums
            for (int i = 0; i < this.numHashFunctions; i++) {
                // Calculate hash_i = (a_i * x + b_i) % P
                // Perform operations in long to avoid intermediate overflow
                long hashValue = (this.hashA[i] * (long)baseHashCode + this.hashB[i]) % LARGE_PRIME;
                 // Ensure non-negative result from modulo
                 if (hashValue < 0) {
                     hashValue += LARGE_PRIME;
                 }

                // Update the signature if this hash is smaller
                minHashes[i] = Math.min(minHashes[i], hashValue);
            }
        }

        // Convert the long[] array to List<Long> for the return type
        List<Long> signature = new ArrayList<>(this.numHashFunctions);
        for (long h : minHashes) {
            // If a hash function never found a feature (set was empty, though checked above)
            // or in theory if all intermediate hashes were Long.MAX_VALUE, keep it.
            // Usually, we expect values much smaller than MAX_VALUE after hashing.
            signature.add(h);
        }

        LOG.debug("Generated MinHash signature (size {}) from feature set size: {}", signature.size(), acc.getFeatures().size());
        return signature;
    }

    // --- Accumulate Method (No change needed) ---
    public void accumulate(LSHAccumulator acc, String sch_tpl, String ts_tpl, String event_type) {
        if (acc == null) {
             LOG.warn("Accumulator is null in accumulate, skipping.");
             return;
         }
        String safe_sch_tpl = sch_tpl == null ? "NULL" : sch_tpl;
        String safe_ts_tpl = ts_tpl == null ? "NULL" : ts_tpl;
        String safe_event_type = event_type == null ? "NULL" : event_type;

        String featureString = safe_sch_tpl + "|" + safe_ts_tpl + "|" + safe_event_type;
        acc.addFeature(featureString);
        LOG.trace("Accumulated feature: {}", featureString);
    }

    // --- Merge Method (No change needed) ---
     public void merge(LSHAccumulator acc, Iterable<LSHAccumulator> it) {
          if (acc == null) {
              LOG.error("Attempted to merge into a null accumulator!");
              return;
          }
         LOG.debug("Merging accumulators...");
         for (LSHAccumulator otherAcc : it) {
             if (otherAcc != null && !otherAcc.isEmpty()) {
                 acc.mergeFeatures(otherAcc.getFeatureMap());
                  LOG.trace("Merged features from another accumulator. Current map size: {}", acc.getFeatures().size());
             }
         }
          LOG.debug("Finished merging. Final map size: {}", (acc.getFeatures() != null ? acc.getFeatures().size() : 0));
     }

    // --- Type Information Overrides ---

    @Override
    public TypeInformation<List<Long>> getResultType() {
        // Explicitly define the return type as an ARRAY (List) of BIGINT (Long)
        // Note: Flink's Types system doesn't have a direct LIST_OF_LONGS.
        // ARRAY<BIGINT> is the SQL equivalent. We hint using DataTypes.
        // Flink might infer this, but being explicit can help.
        // However, standard Types often work:
         return Types.LIST(Types.LONG); // Let Flink map List<Long>
        // If the above causes issues, you might need more specific TypeFactory hinting
        // based on your exact Flink version and planner.
    }

    @Override
    public TypeInformation<LSHAccumulator> getAccumulatorType() {
        // Accumulator type remains the same POJO
        return Types.POJO(LSHAccumulator.class);
    }

    // --- Optional Reset Accumulator Method (No change needed) ---
     public void resetAccumulator(LSHAccumulator acc) {
         if (acc != null) {
            acc.setFeatureMap(new HashMap<>());
         }
     }
}