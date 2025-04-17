# test_lsh.py (or wherever you keep LSH tests)

import pytest
import numpy as np
from src.algorithms.lsh import LSHRandomProjection # Assuming it's in src/algorithms/lsh.py

# --- Constants for testing ---
# Assuming DELAY_VECTOR_SIZE = 5 from the delay example
TEST_LSH_DIM = 5
TEST_LSH_HASHES = 10
TEST_SEED_1 = 42
TEST_SEED_2 = 99

# --- Helper Functions / Fixtures (Optional but useful) ---
@pytest.fixture
def hasher_seed1():
    """Provides a hasher instance initialized with seed 1."""
    return LSHRandomProjection(input_dim=TEST_LSH_DIM, num_hashes=TEST_LSH_HASHES, seed=TEST_SEED_1)

@pytest.fixture
def hasher_seed2():
    """Provides a hasher instance initialized with seed 2."""
    return LSHRandomProjection(input_dim=TEST_LSH_DIM, num_hashes=TEST_LSH_HASHES, seed=TEST_SEED_2)

# --- Test Cases ---

def test_lsh_initialization(hasher_seed1):
    """Test that the hasher initializes correctly."""
    assert hasher_seed1.input_dim == TEST_LSH_DIM
    assert hasher_seed1.num_hashes == TEST_LSH_HASHES
    assert hasher_seed1.random_vectors.shape == (TEST_LSH_HASHES, TEST_LSH_DIM)
    # Check that the random vectors are not all zeros (highly unlikely with randn)
    assert np.any(hasher_seed1.random_vectors != 0)

def test_lsh_hashing_basic_output_format(hasher_seed1):
    """Test hashing a valid vector returns the correct format."""
    vec = np.random.rand(TEST_LSH_DIM)
    signature = hasher_seed1.hash_vector(vec)
    assert isinstance(signature, tuple)
    assert len(signature) == TEST_LSH_HASHES
    assert all(bit in (0, 1) for bit in signature)

def test_lsh_hashing_deterministic_instance(hasher_seed1):
    """Test hashing the same vector multiple times with the same instance yields the same hash."""
    vec = np.array([1.0, -2.0, 0.0, 5.5, -10.1])
    sig1 = hasher_seed1.hash_vector(vec)
    sig2 = hasher_seed1.hash_vector(vec)
    assert sig1 is not None
    assert sig1 == sig2

def test_lsh_hashing_deterministic_seed():
    """Test that two instances created with the same seed produce the same hash for the same vector."""
    hasher1 = LSHRandomProjection(input_dim=TEST_LSH_DIM, num_hashes=TEST_LSH_HASHES, seed=TEST_SEED_1)
    hasher2 = LSHRandomProjection(input_dim=TEST_LSH_DIM, num_hashes=TEST_LSH_HASHES, seed=TEST_SEED_1)
    vec = np.array([0.5, 0.5, 0.5, 0.5, 0.5])
    sig1 = hasher1.hash_vector(vec)
    sig2 = hasher2.hash_vector(vec)
    assert sig1 is not None
    assert sig1 == sig2

def test_lsh_hashing_different_seeds(hasher_seed1, hasher_seed2):
    """Test that different seeds lead to different hashing functions (and likely different hashes)."""
    vec = np.array([-1.0, -2.0, -3.0, -4.0, -5.0])
    sig1 = hasher_seed1.hash_vector(vec)
    sig2 = hasher_seed2.hash_vector(vec)
    assert sig1 is not None
    assert sig2 is not None
    # Hashes might collide by chance, but highly unlikely for different random projections
    assert sig1 != sig2

def test_lsh_hashing_identical_vectors(hasher_seed1):
    """Test that identical input vectors produce the same hash."""
    vec1 = np.array([5.0, -2.0, 10.0, 0.0, 1.0])
    vec2 = np.array([5.0, -2.0, 10.0, 0.0, 1.0])
    sig1 = hasher_seed1.hash_vector(vec1)
    sig2 = hasher_seed1.hash_vector(vec2)
    assert sig1 is not None
    assert sig1 == sig2

# Note: Testing "similarity" is tricky. LSH guarantees higher *probability* of collision
# for similar vectors, not guaranteed collision. We won't test that explicitly here,
# but focus on correctness of the mechanism.

def test_lsh_hashing_different_vectors(hasher_seed1):
    """Test that substantially different vectors likely produce different hashes."""
    vec1 = np.array([100.0, 200.0, 300.0, 400.0, 500.0])
    vec2 = np.array([-100.0, -200.0, -300.0, -400.0, -500.0])
    sig1 = hasher_seed1.hash_vector(vec1)
    sig2 = hasher_seed1.hash_vector(vec2)
    assert sig1 is not None
    assert sig2 is not None
    # Collisions are possible but less likely for very different vectors
    assert sig1 != sig2

def test_lsh_hashing_vector_with_nans(hasher_seed1):
    """Test hashing a vector containing NaNs (should be treated as 0)."""
    vec_clean = np.array([5.0, 0.0, 10.0, 0.0, 1.0]) # NaNs replaced by 0
    vec_nan   = np.array([5.0, np.nan, 10.0, np.nan, 1.0])
    sig_clean = hasher_seed1.hash_vector(vec_clean)
    sig_nan = hasher_seed1.hash_vector(vec_nan)
    assert sig_nan is not None
    # Since NaN is replaced by 0, the hash should be the same as the vector with 0s
    assert sig_nan == sig_clean

def test_lsh_hashing_vector_all_nans(hasher_seed1):
    """Test that a vector of all NaNs returns None."""
    vec_all_nan = np.full(TEST_LSH_DIM, np.nan)
    signature = hasher_seed1.hash_vector(vec_all_nan)
    assert signature is None

def test_lsh_hashing_none_input(hasher_seed1):
    """Test that hashing None input returns None."""
    signature = hasher_seed1.hash_vector(None)
    assert signature is None

def test_lsh_hashing_wrong_shape(hasher_seed1):
    """Test that hashing a vector with the wrong dimension returns None."""
    vec_wrong_dim = np.random.rand(TEST_LSH_DIM + 1)
    signature = hasher_seed1.hash_vector(vec_wrong_dim)
    assert signature is None

    vec_wrong_dim_2 = np.random.rand(TEST_LSH_DIM - 1)
    signature_2 = hasher_seed1.hash_vector(vec_wrong_dim_2)
    assert signature_2 is None

def test_lsh_hashing_zero_vector(hasher_seed1):
    """Test hashing the zero vector."""
    vec_zero = np.zeros(TEST_LSH_DIM)
    signature = hasher_seed1.hash_vector(vec_zero)
    # The hash of the zero vector will be all zeros, since all dot products will be 0
    expected_signature = tuple([0] * TEST_LSH_HASHES)
    assert signature == expected_signature