package com.sjsu.flink;

import java.io.Serializable;
import java.util.BitSet;
import java.util.Random;
import java.util.Objects;
import java.util.Base64;

/**
 * Accumulator for the BloomFilterAggregateFunction.
 * Holds the Bloom Filter state as a Base64 String to be Flink POJO compatible.
 */
public class BloomFilterAccumulator implements Serializable {
    private static final long serialVersionUID = 2L; // Incremented version

    // --- Fields Flink will serialize (must be POJO compatible) ---
    public String bitSetBase64;  // Store state as Base64 String
    public int size;             // Size of the bit array (m)
    public int numHashes;        // Number of hash functions (k)
    public long[] hashSeedsA;    // Seeds/coefficients 'a' for the hash functions
    public long[] hashSeedsB;    // Seeds/coefficients 'b' for the hash functions

    // --- Transient field for active processing ---
    private transient BitSet transientBitSet; // Holds the live BitSet instance

    // Public no-argument constructor required by Flink POJO rules
    public BloomFilterAccumulator() {}

    /**
     * Initializes the accumulator state. Called by createAccumulator.
     * @param size The size (m) of the BitSet.
     * @param numHashes The number (k) of hash functions.
     * @param seed A seed for generating reproducible hash functions.
     */
    public void initialize(int size, int numHashes, long seed) {
        this.size = size;
        this.numHashes = numHashes;
        // Initialize transient BitSet
        this.transientBitSet = new BitSet(size);
        // Store initial empty state as Base64
        this.bitSetBase64 = encodeBitSet(this.transientBitSet);

        // Initialize hash function parameters
        this.hashSeedsA = new long[numHashes];
        this.hashSeedsB = new long[numHashes];
        Random rand = new Random(seed);
        for (int i = 0; i < numHashes; i++) {
            this.hashSeedsA[i] = rand.nextInt(Integer.MAX_VALUE - 1) + 1;
            this.hashSeedsB[i] = rand.nextInt(Integer.MAX_VALUE);
        }
    }

    /** Ensures the transient BitSet is ready for use, decoding if necessary. */
    private void ensureTransientBitSet() {
        if (transientBitSet == null) {
            // Decode from Base64 state if this instance was deserialized
            this.transientBitSet = decodeBitSet(this.bitSetBase64, this.size);
        }
    }

    /**
     * Adds an element (its hash code) to the Bloom Filter by setting k bits.
     * Updates the Base64 stored state afterwards.
     * @param elementHashCode The hash code of the element to add.
     */
    public void add(int elementHashCode) {
        ensureTransientBitSet(); // Make sure the BitSet is loaded
        if (transientBitSet == null) return; // Should not happen if initialized

        boolean changed = false;
        for (int i = 0; i < numHashes; i++) {
            int bitIndex = getHashIndex(i, elementHashCode);
            if (!transientBitSet.get(bitIndex)) {
                 transientBitSet.set(bitIndex, true);
                 changed = true; // Mark that the filter state changed
            }
        }
        // Only re-encode if the bitset actually changed
        if (changed) {
             this.bitSetBase64 = encodeBitSet(this.transientBitSet);
        }
    }

    /**
     * Checks if an element (its hash code) might be in the set.
     * Uses the transient BitSet for checking.
     * @param elementHashCode The hash code of the element to check.
     * @return true if the element might be present, false otherwise.
     */
    public boolean mightContain(int elementHashCode) {
        ensureTransientBitSet(); // Load if needed
        if (transientBitSet == null) return false;

        for (int i = 0; i < numHashes; i++) {
            int bitIndex = getHashIndex(i, elementHashCode);
            if (!transientBitSet.get(bitIndex)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Merges another Bloom Filter's state (from Base64) into this one.
     * @param otherBase64 The Base64 string from the other accumulator.
     */
    public void merge(String otherBase64) {
        ensureTransientBitSet(); // Load current state
         if (this.transientBitSet == null) return; // Cannot merge into uninitialized state

        if (otherBase64 != null && !otherBase64.isEmpty()) {
            BitSet otherBitSet = decodeBitSet(otherBase64, this.size);
            this.transientBitSet.or(otherBitSet);
            // Update the stored state
            this.bitSetBase64 = encodeBitSet(this.transientBitSet);
        }
    }

    /** Calculates the i-th hash function index. (No change) */
    private int getHashIndex(int i, int elementHashCode) {
        long hash = (hashSeedsA[i] * (long)elementHashCode + hashSeedsB[i]) % size;
        return (int) (hash < 0 ? hash + size : hash);
    }

    // --- Helper methods for encoding/decoding ---
    private static String encodeBitSet(BitSet bitSet) {
        return (bitSet != null) ? Base64.getEncoder().encodeToString(bitSet.toByteArray()) : ""; // Return empty string for null/empty
    }

    private static BitSet decodeBitSet(String base64String, int expectedSize) {
        if (base64String != null && !base64String.isEmpty()) {
            byte[] bytes = Base64.getDecoder().decode(base64String);
            return BitSet.valueOf(bytes);
        } else {
            // Return an empty BitSet of the expected size if input is null/empty
            return new BitSet(expectedSize);
        }
    }

    // --- Getters are needed for Flink POJO field detection if fields are private ---
    // Since fields are public, getters/setters are not strictly required by Flink,
    // but adding them is good practice if you ever make fields private.
    // public String getBitSetBase64() { return bitSetBase64; }
    // public void setBitSetBase64(String b64) { this.bitSetBase64 = b64; this.transientBitSet = null; } // Invalidate transient on set
    // ... getters/setters for size, numHashes, hashSeedsA, hashSeedsB ...
}