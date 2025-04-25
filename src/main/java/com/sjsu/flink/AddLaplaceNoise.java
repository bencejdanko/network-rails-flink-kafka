package com.sjsu.flink; // Adjust package name
import org.apache.commons.math3.random.RandomGenerator; // Import the interface
import org.apache.commons.math3.random.Well19937c;     // Import a specific implementation
import org.apache.commons.math3.distribution.LaplaceDistribution; // Need Apache Commons Math
import org.apache.flink.table.functions.ScalarFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
// import java.util.Random;

/**
 * Flink Scalar Function to add Laplace noise for Differential Privacy.
 */
public class AddLaplaceNoise extends ScalarFunction {
    private static final Logger LOG = LoggerFactory.getLogger(AddLaplaceNoise.class);

    private final double sensitivity;
    private final double epsilon;
    private final double scale; // Laplace scale parameter (b = sensitivity / epsilon)
    private transient LaplaceDistribution laplace; // Use transient for the distribution object
    private transient RandomGenerator randomGenerator;     // Use transient for the Random generator

    /**
     * Constructor for the UDF.
     * @param sensitivity The sensitivity of the query (e.g., 1.0 for COUNT or COUNT DISTINCT).
     * @param epsilon The privacy budget (positive value). Smaller means more privacy/noise.
     */
    public AddLaplaceNoise(double sensitivity, double epsilon) {
        if (sensitivity <= 0) {
            throw new IllegalArgumentException("Sensitivity must be positive.");
        }
        if (epsilon <= 0) {
            throw new IllegalArgumentException("Epsilon must be positive.");
        }
        this.sensitivity = sensitivity;
        this.epsilon = epsilon;
        this.scale = sensitivity / epsilon;
        LOG.info("Initialized AddLaplaceNoise: Sensitivity={}, Epsilon={}, Scale(b)={}",
                 sensitivity, epsilon, this.scale);
        // Initialize transient fields here, they will be re-initialized if null in eval
        initializeDistribution();
    }

    /** Initializes or re-initializes the transient distribution and random generator. */
    private void initializeDistribution() {
        if (randomGenerator == null) {
            // Use an implementation from Apache Commons Math
            // You can optionally seed it for reproducibility: new Well19937c(seedValue);
            randomGenerator = new Well19937c();
        }
        if (laplace == null) {
            // This line should now work as randomGenerator is the correct type
            laplace = new LaplaceDistribution(randomGenerator, 0.0, this.scale);
        }
    }

    /**
     * The evaluation method called by Flink SQL.
     * @param trueValue The true result of the aggregate query (e.g., count). Can be Long or Integer.
     * @return The true value plus Laplace noise, rounded to the nearest long.
     */
    public Double eval(Long trueValue) { // Accept Long from COUNT DISTINCT
        if (trueValue == null) {
            return null; // Or handle as 0? DP usually applies to non-null counts.
        }

        // Ensure distribution is initialized (in case of deserialization)
        initializeDistribution();

        // Sample noise from the Laplace distribution
        double noise = laplace.sample();

        // Add noise to the true value
        double noisyValue = trueValue.doubleValue() + noise;

        LOG.trace("True value: {}, Scale(b): {}, Noise: {}, Noisy value: {}",
                  trueValue, this.scale, noise, noisyValue);

        // Return the noisy value as a Double.
        // You could round to Long if integer counts are strictly needed downstream,
        // but returning Double preserves more info about the noise application.
        // return Math.round(noisyValue); // Option to return Long
        return noisyValue;
    }

     // Overload for Integer input if needed (e.g., from COUNT(*))
     public Double eval(Integer trueValue) {
         if (trueValue == null) {
             return null;
         }
         return eval(trueValue.longValue()); // Delegate to the Long version
     }
}