import numpy as np
from typing import Union, Optional
import logging

# --- Differential Privacy Parameters (Example - tune later) ---
DEFAULT_DP_EPSILON = 1.0 # Privacy budget
DEFAULT_DP_SENSITIVITY_AVG = 10.0 # Assumed max contribution of one train to avg delay (e.g., max delay / min count) - NEEDS REFINEMENT
DEFAULT_DP_SENSITIVITY_VAR = 100.0 # Assumed max contribution to variance - NEEDS REFINEMENT

def add_laplace_noise(
    value: Union[float, int],
    sensitivity: float,
    epsilon: float,
    seed: Optional[int] = None # Optional seed for reproducibility during testing
) -> float:
    """
    Adds Laplace noise to a value for differential privacy.

    Args:
        value: The original numeric value (e.g., average delay, count).
        sensitivity: The sensitivity (Delta f) of the query/function producing the value.
                     This is the maximum possible change to the output if one individual's
                     data is added or removed from the dataset.
        epsilon: The privacy budget (epsilon > 0). Smaller epsilon means more privacy, more noise.
        seed: An optional random seed for deterministic noise generation (for testing).

    Returns:
        The value with Laplace noise added.
    """
    if epsilon <= 0:
        # raise ValueError("Epsilon must be positive for Laplace mechanism.")
        # In production, might return infinity or handle differently,
        # but for now, returning original value might be safer than crashing.
        logging.warning(f"Warning: Epsilon is non-positive ({epsilon}). Returning original value.")
        return float(value)
        
    if sensitivity < 0:
        raise ValueError("Sensitivity must be non-negative.")

    scale = sensitivity / epsilon

    # Use a seeded generator if seed is provided
    rng = np.random.default_rng(seed) if seed is not None else np.random.default_rng()

    noise = rng.laplace(loc=0.0, scale=scale)

    return float(value) + noise

# --- Usage Example (for standalone testing) ---
# if __name__ == "__main__":
#     avg_delay = 5.5 # Example calculated average delay for a bucket
#     variance_delay = 20.25 # Example calculated variance
#     count = 15 # Example count in bucket
#
#     # Placeholder sensitivities - these need careful calculation based on data bounds!
#     # Sensitivity for average = (max_delay - min_delay) / min_possible_count_in_aggregation
#     # Sensitivity for variance is more complex. Let's use placeholders.
#     # We might need to clamp counts/values before DP for bounded sensitivity.
#     sensitivity_avg = 60.0 # Assume max delay diff is 60 mins, min count 1? (Needs thought)
#     sensitivity_var = 3600.0 # (Max delay diff)^2 ? (Needs more thought)
#
#     epsilon = 1.0
#
#     dp_avg_delay = add_laplace_noise(avg_delay, sensitivity_avg, epsilon, seed=123)
#     dp_variance_delay = add_laplace_noise(variance_delay, sensitivity_var, epsilon, seed=456)
#     # Note: Adding noise to count might also be needed depending on what's reported.
#     # Sensitivity for count is typically 1.
#     dp_count = add_laplace_noise(count, sensitivity=1.0, epsilon=epsilon, seed=789)
#
#
#     print(f"Original Avg Delay: {avg_delay:.2f}, DP Avg Delay: {dp_avg_delay:.2f}")
#     print(f"Original Variance : {variance_delay:.2f}, DP Variance : {dp_variance_delay:.2f}")
#     print(f"Original Count    : {count}, DP Count    : {dp_count:.2f}") # Noisy count often rounded/truncated