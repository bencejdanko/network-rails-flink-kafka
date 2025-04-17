import pytest
from datetime import datetime
from src.algorithms.delay import calculate_delay_minutes, DELAY_VECTOR_SIZE, create_empty_delay_vector, update_delay_vector
import numpy as np

def test_calculate_delay_on_time():
    sched = datetime(2023, 4, 16, 10, 30, 0)
    fc = datetime(2023, 4, 16, 10, 30, 0)
    assert calculate_delay_minutes(sched, fc) == 0.0

def test_calculate_delay_late():
    sched = datetime(2023, 4, 16, 10, 30, 0)
    fc = datetime(2023, 4, 16, 10, 35, 30) # 5.5 mins late
    assert calculate_delay_minutes(sched, fc) == 5.5

def test_calculate_delay_early():
    sched = datetime(2023, 4, 16, 10, 30, 0)
    fc = datetime(2023, 4, 16, 10, 28, 0) # 2 mins early
    assert calculate_delay_minutes(sched, fc) == -2.0

def test_calculate_delay_none_input():
    sched = datetime(2023, 4, 16, 10, 30, 0)
    assert calculate_delay_minutes(sched, None) is None
    assert calculate_delay_minutes(None, sched) is None

def test_delay_vector_creation():
     vec = create_empty_delay_vector()
     assert vec.shape == (DELAY_VECTOR_SIZE,)
     assert np.all(np.isnan(vec))

def test_delay_vector_update():
     vec = create_empty_delay_vector()
     vec = update_delay_vector(vec, 5.0)
     vec = update_delay_vector(vec, -2.0)
     expected = np.full(DELAY_VECTOR_SIZE, np.nan)
     expected[-2] = 5.0
     expected[-1] = -2.0
     np.testing.assert_array_equal(vec, expected) # Use numpy testing for arrays

def test_delay_vector_update_full_roll():
    vec = np.arange(DELAY_VECTOR_SIZE, dtype=float) # [0., 1., 2., 3., 4.]
    vec = update_delay_vector(vec, 99.0)
    expected = np.array([1., 2., 3., 4., 99.])
    np.testing.assert_array_equal(vec, expected)

def test_delay_vector_update_with_none():
    vec = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
    vec = update_delay_vector(vec, None)
    expected = np.array([2.0, 3.0, 4.0, 5.0, np.nan])
    np.testing.assert_array_equal(vec, expected) # Check NaN propagation