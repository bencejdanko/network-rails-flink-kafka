# test_dp.py (or wherever you keep DP tests)

import pytest
import numpy as np
from src.algorithms.dp import add_laplace_noise # Assuming it's in src/algorithms/dp.py

# --- Constants for testing ---
TEST_VALUE = 10.0
TEST_SENSITIVITY = 5.0
TEST_EPSILON = 1.0
TEST_SEED_1 = 123
TEST_SEED_2 = 456

# --- Test Cases ---

def test_add_laplace_noise_basic():
    """Test basic functionality: noise is added, output is float."""
    noisy_value = add_laplace_noise(TEST_VALUE, TEST_SENSITIVITY, TEST_EPSILON, seed=TEST_SEED_1)
    assert isinstance(noisy_value, float)
    # With sensitivity > 0 and epsilon > 0, noise should almost certainly be non-zero
    # (unless by extreme chance the random sample is exactly 0)
    # A robust check is difficult, but checking it's not *exactly* the original is a start.
    assert noisy_value != TEST_VALUE

def test_add_laplace_noise_deterministic_with_seed():
    """Test that the same seed produces the same noise."""
    noisy_value_1 = add_laplace_noise(TEST_VALUE, TEST_SENSITIVITY, TEST_EPSILON, seed=TEST_SEED_1)
    noisy_value_2 = add_laplace_noise(TEST_VALUE, TEST_SENSITIVITY, TEST_EPSILON, seed=TEST_SEED_1)
    assert noisy_value_1 == noisy_value_2

def test_add_laplace_noise_different_seeds():
    """Test that different seeds produce different noise."""
    noisy_value_1 = add_laplace_noise(TEST_VALUE, TEST_SENSITIVITY, TEST_EPSILON, seed=TEST_SEED_1)
    noisy_value_2 = add_laplace_noise(TEST_VALUE, TEST_SENSITIVITY, TEST_EPSILON, seed=TEST_SEED_2)
    # It's theoretically possible they are the same, but highly unlikely
    assert noisy_value_1 != noisy_value_2

def test_add_laplace_noise_no_seed_non_deterministic():
    """Test that omitting the seed likely produces different results on subsequent calls."""
    # Note: This isn't perfectly guaranteed, but should hold in practice
    noisy_value_1 = add_laplace_noise(TEST_VALUE, TEST_SENSITIVITY, TEST_EPSILON)
    noisy_value_2 = add_laplace_noise(TEST_VALUE, TEST_SENSITIVITY, TEST_EPSILON)
    assert noisy_value_1 != noisy_value_2

def test_add_laplace_noise_zero_sensitivity():
    """Test that zero sensitivity results in no added noise."""
    noisy_value = add_laplace_noise(TEST_VALUE, sensitivity=0.0, epsilon=TEST_EPSILON, seed=TEST_SEED_1)
    assert noisy_value == pytest.approx(TEST_VALUE)

def test_add_laplace_noise_negative_sensitivity():
    """Test that negative sensitivity raises a ValueError."""
    with pytest.raises(ValueError, match="Sensitivity must be non-negative."):
        add_laplace_noise(TEST_VALUE, sensitivity=-1.0, epsilon=TEST_EPSILON)

def test_add_laplace_noise_zero_epsilon(caplog):
    """Test that zero epsilon returns original value and logs a warning."""
    # caplog is a pytest fixture to capture log output
    value = 50.0
    sensitivity = 10.0
    epsilon = 0.0
    result = add_laplace_noise(value, sensitivity, epsilon)
    assert result == value
    assert "Warning: Epsilon is non-positive (0.0)" in caplog.text

def test_add_laplace_noise_negative_epsilon(caplog):
    """Test that negative epsilon returns original value and logs a warning."""
    value = 50.0
    sensitivity = 10.0
    epsilon = -0.5
    result = add_laplace_noise(value, sensitivity, epsilon)
    assert result == value
    assert f"Warning: Epsilon is non-positive ({epsilon})" in caplog.text

def test_add_laplace_noise_integer_input():
    """Test that the function works correctly with an integer input value."""
    int_value = 20
    noisy_value = add_laplace_noise(int_value, TEST_SENSITIVITY, TEST_EPSILON, seed=TEST_SEED_1)
    assert isinstance(noisy_value, float)
    # Re-run with same seed to check determinism holds for int input too
    noisy_value_2 = add_laplace_noise(int_value, TEST_SENSITIVITY, TEST_EPSILON, seed=TEST_SEED_1)
    assert noisy_value == noisy_value_2
    assert noisy_value != int_value # Noise should have been added

def test_add_laplace_noise_scale_effect():
    """Qualitatively check that lower epsilon or higher sensitivity increases noise magnitude."""
    # Note: This is probabilistic, so we use seeds for reproducibility and check relative difference
    # Same sensitivity, different epsilon
    noise_high_eps = add_laplace_noise(0, TEST_SENSITIVITY, epsilon=10.0, seed=TEST_SEED_1) # Less noise
    noise_low_eps  = add_laplace_noise(0, TEST_SENSITIVITY, epsilon=0.1, seed=TEST_SEED_1) # More noise
    assert abs(noise_low_eps) > abs(noise_high_eps)

    # Same epsilon, different sensitivity
    noise_low_sens = add_laplace_noise(0, sensitivity=1.0, epsilon=TEST_EPSILON, seed=TEST_SEED_2) # Less noise
    noise_high_sens= add_laplace_noise(0, sensitivity=100.0, epsilon=TEST_EPSILON, seed=TEST_SEED_2) # More noise
    assert abs(noise_high_sens) > abs(noise_low_sens)