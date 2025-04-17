import numpy as np
from typing import List, Optional
from datetime import datetime, timedelta

# --- Delay Vector Definition ---
# Consistent structure agreed upon for representing recent delays.
# Example: Fixed-size NumPy array storing the last N delay values (in minutes).
# A positive value means late, negative means early. NaN or another placeholder
# could indicate missing data or a cancelled stop.

DELAY_VECTOR_SIZE = 5 # Example size, adjust as needed

def create_empty_delay_vector() -> np.ndarray:
    """Creates an initialized delay vector (e.g., with NaNs or zeros)."""
    return np.full(DELAY_VECTOR_SIZE, np.nan) # Use NaN to represent missing data

def update_delay_vector(current_vector: np.ndarray, new_delay: Optional[float]) -> np.ndarray:
    """
    Adds a new delay value to the vector, maintaining its fixed size
    (e.g., shifts old values, adds new one).
    Handles None input for new_delay (e.g., keeps NaN).
    """
    new_vector = np.roll(current_vector, -1) # Shift elements left
    new_vector[-1] = new_delay if new_delay is not None else np.nan
    return new_vector

# --- Delay Calculation Logic ---
# Note: This function assumes time strings have ALREADY been parsed into datetime objects.
# Time parsing logic should ideally live in src/utils/time_parser.py

def calculate_delay_minutes(
    scheduled_dt: Optional[datetime],
    forecast_dt: Optional[datetime]
) -> Optional[float]:
    """
    Calculates the delay in minutes between a scheduled time and a forecast time.

    Args:
        scheduled_dt: The scheduled datetime object.
        forecast_dt: The forecast or actual datetime object.

    Returns:
        Delay in minutes (positive if late, negative if early),
        or None if either input is None or calculation is not possible.
    """
    if scheduled_dt is None or forecast_dt is None:
        return None

    delay_timedelta: timedelta = forecast_dt - scheduled_dt
    delay_minutes: float = delay_timedelta.total_seconds() / 60.0
    return delay_minutes

# --- Placeholder for Delay Calculation Context ---
# In Flink, this logic will be more complex, needing access to schedule state
# and handling different event types (arrival, departure, pass) and flags
# (cancelled, suppressed). Today, focus on the core calculation above.