import numpy as np
from typing import Tuple, Optional

# --- LSH Parameters (Example - tune later) ---
# These would likely be loaded from config or passed during initialization
LSH_NUM_HASH_FUNCTIONS = 10 # Number of hash functions (k)
LSH_NUM_HASH_TABLES = 5     # Number of hash tables (L) - not used in simple impl.
LSH_DIMENSIONALITY = 5      # Must match DELAY_VECTOR_SIZE from delay.py

class LSHRandomProjection:
    """
    Simple Locality Sensitive Hashing using random projections.
    """
    def __init__(self, input_dim: int, num_hashes: int, seed: int = 42):
        """
        Initializes the random projection vectors.

        Args:
            input_dim: Dimensionality of the input vectors (e.g., DELAY_VECTOR_SIZE).
            num_hashes: The number of hash functions (k) to use for the signature.
            seed: Random seed for reproducibility.
        """
        self.input_dim = input_dim
        self.num_hashes = num_hashes
        self.rng = np.random.RandomState(seed)
        # Create random projection vectors (one per hash function)
        self.random_vectors = self.rng.randn(self.num_hashes, self.input_dim) # k x d

    def hash_vector(self, vector: np.ndarray) -> Optional[Tuple[int, ...]]:
        """
        Computes the LSH signature (hash bucket) for a given vector.

        Args:
            vector: The input delay vector (NumPy array of size input_dim).
                    Should handle potential NaNs gracefully.

        Returns:
            A tuple representing the hash bucket signature (e.g., (0, 1, 1, 0,...)),
            or None if the vector is unsuitable for hashing (e.g., all NaNs).
        """
        if vector is None or vector.shape != (self.input_dim,):
             # print(f"Warning: Invalid vector shape for LSH: {vector.shape if vector is not None else 'None'}")
             return None

        # Handle NaNs: Option 1: Replace with 0 (simple)
        # Option 2: Ignore dimensions with NaN (more complex)
        # Option 3: Return None if too many NaNs
        clean_vector = np.nan_to_num(vector, nan=0.0) # Replace NaN with 0.0

        if np.all(np.isnan(vector)): # If original vector was all NaN
             # print("Warning: Vector contains only NaNs, cannot hash.")
             return None # Or return a specific 'invalid' bucket

        # Compute dot products with random vectors
        projections = self.random_vectors.dot(clean_vector) # Shape (num_hashes,)

        # Generate binary hash signature: 1 if projection > 0, else 0
        signature = tuple(1 if p > 0 else 0 for p in projections)
        return signature

# --- Usage Example (for standalone testing) ---
# if __name__ == "__main__":
#     hasher = LSHRandomProjection(input_dim=LSH_DIMENSIONALITY, num_hashes=LSH_NUM_HASH_FUNCTIONS)
#     vec1 = np.array([5.0, -2.0, 10.0, 0.0, 1.0])
#     vec2 = np.array([6.0, -1.5, 9.5, 0.5, 1.2]) # Similar vector
#     vec3 = np.array([-20.0, 50.0, 0.0, -5.0, -10.0]) # Different vector
#     vec_nan = np.array([5.0, np.nan, 10.0, np.nan, 1.0])
#
#     print(f"Vector 1: {vec1}, Hash: {hasher.hash_vector(vec1)}")
#     print(f"Vector 2: {vec2}, Hash: {hasher.hash_vector(vec2)}")
#     print(f"Vector 3: {vec3}, Hash: {hasher.hash_vector(vec3)}")
#     print(f"NaN Vec : {vec_nan}, Hash: {hasher.hash_vector(vec_nan)}")