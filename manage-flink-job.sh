#!/bin/bash

# Exit on any error, treat unset variables as errors, and propagate pipeline failures
set -euo pipefail

# --- Default Configuration File ---
DEFAULT_CONFIG_FILE="job.conf"
CONFIG_FILE=""

# --- Script Variables ---
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"
JOB_ID="" # Will store the Flink Job ID after submission
JOB_NAME="" # Set via command-line argument
MAIN_CLASS="" # Set via command-line argument
PARALLELISM="" # Set via command-line or config
FLINK_ARGS="" # Additional args for 'flink run'
JOB_ARGS=""   # Arguments passed to the Flink job main method
SKIP_BUILD=false # Flag to skip the build step
DETACHED_MODE=false # <<<<< ADDED: Flag for detached submission

# --- Functions ---

# Function to display usage instructions and exit
usage() {
    cat << EOF
Usage: $0 -m <main_class> -n <job_name> [options]

Manages the build and submission of a Flink job.

Required Arguments:
  -m, --main-class <class>   Fully qualified name of the Flink job's main class.
  -n, --job-name <name>      A descriptive name for the Flink job submission.

Options:
  -c, --config <file>        Path to the configuration file (default: ${DEFAULT_CONFIG_FILE} in script dir).
  -p, --parallelism <num>    Flink job parallelism (overrides config default).
  -f, --flink-args "<args>"  Additional arguments to pass directly to 'flink run'
                             (e.g., "--savepointPath /sp"). Quote if contains spaces.
                             Overrides FLINK_RUN_EXTRA_ARGS in config.
  -j, --job-args "<args>"    Arguments to pass to the Flink job's main method. Quote if contains spaces.
  -s, --skip-build           Skip the build step (assumes JAR already exists).
  -d, --detached             Submit the job and exit immediately without monitoring or waiting for Ctrl+C. # <<<<< ADDED
  -h, --help                 Display this help message and exit.
EOF
    exit 1
}

# Function to log messages
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1"
}

# Function to log errors and exit
error_exit() {
    log "ERROR: $1" >&2
    # Optionally: Call cleanup here if needed, though set -e might make it tricky
    exit 1
}

# Function to clean up (stop the Flink job)
cleanup() {
    echo # Add a newline after ^C
    # Only attempt cleanup if NOT in detached mode, as the script exits quickly anyway
    if [ "$DETACHED_MODE" = false ] && [ -n "$JOB_ID" ]; then # <<<<< MODIFIED Check DETACHED_MODE
        log "Ctrl+C detected. Attempting to stop Flink job '$JOB_NAME' ($JOB_ID)..."
        # Check if container exists/is running before trying to cancel
        if docker ps -f name="^/${JOBMANAGER_CONTAINER}$" --format '{{.Names}}' | grep -q "^${JOBMANAGER_CONTAINER}$"; then
            if docker exec "$JOBMANAGER_CONTAINER" flink cancel "$JOB_ID" > /dev/null 2>&1; then
                log "Flink job $JOB_ID cancellation request sent successfully."
            else
                # Job might have already finished or failed, or container issues
                log "Warning: Failed to send cancellation request for job '$JOB_NAME' ($JOB_ID)." >&2
                log "Job might have finished, failed, or the container might be stopped." >&2
            fi
        else
             log "Warning: JobManager container '$JOBMANAGER_CONTAINER' not found or not running. Cannot cancel job $JOB_ID automatically." >&2
        fi
    elif [ "$DETACHED_MODE" = false ]; then # <<<<< MODIFIED Check DETACHED_MODE
        log "Ctrl+C detected, but no Flink job ID was captured to stop."
    # else: In detached mode, Ctrl+C likely won't happen while script is running long, so do nothing.
    fi
    exit 0 # Exit the script gracefully after cleanup attempt
}

# --- Argument Parsing ---
while [[ $# -gt 0 ]]; do
    case "$1" in
        -m | --main-class)
            MAIN_CLASS="$2"
            shift 2
            ;;
        -n | --job-name)
            JOB_NAME="$2"
            shift 2
            ;;
        -c | --config)
            CONFIG_FILE="$2"
            shift 2
            ;;
        -p | --parallelism)
            PARALLELISM="$2"
            shift 2
            ;;
        -f | --flink-args)
            FLINK_ARGS="$2"
            shift 2
            ;;
        -j | --job-args)
            JOB_ARGS="$2"
            shift 2
            ;;
        -s | --skip-build)
            SKIP_BUILD=true
            shift 1
            ;;
        -d | --detached) # <<<<< ADDED
            DETACHED_MODE=true
            shift 1
            ;;
        -h | --help)
            usage
            ;;
        *)
            echo "Error: Unknown option: $1" >&2
            usage
            ;;
    esac
done

# --- Validate Required Arguments ---
if [ -z "$MAIN_CLASS" ]; then
    echo "Error: Main class (-m) is required." >&2
    usage
fi
if [ -z "$JOB_NAME" ]; then
    echo "Error: Job name (-n) is required." >&2
    usage
fi

# --- Load Configuration ---
CONFIG_FILE="${CONFIG_FILE:-${SCRIPT_DIR}/${DEFAULT_CONFIG_FILE}}"
if [ ! -f "$CONFIG_FILE" ]; then
    error_exit "Configuration file not found: $CONFIG_FILE"
fi

log "Loading configuration from: $CONFIG_FILE"
# Source the config file, exposing its variables to this script
# Use bash-specific source `.` for better compatibility
# shellcheck source=/dev/null # Tell ShellCheck we know we're sourcing dynamically
. "$CONFIG_FILE" || error_exit "Failed to load configuration file: $CONFIG_FILE"

# --- Set Defaults and Overrides ---
PARALLELISM="${PARALLELISM:-${DEFAULT_PARALLELISM:-1}}" # Default to 1 if not set in config or args
# Command-line Flink args override config file args
FLINK_ARGS="${FLINK_ARGS:-${FLINK_RUN_EXTRA_ARGS:-}}"

log "--- Configuration Summary ---"
log "Project Name:           ${PROJECT_NAME:-N/A}"
log "Main Class:             $MAIN_CLASS"
log "Job Name:               $JOB_NAME"
log "JobManager Container:   $JOBMANAGER_CONTAINER"
log "Build Output Dir (Host):$BUILD_OUTPUT_DIR"
log "JAR Search Pattern:     $JAR_SEARCH_PATTERN"
log "JAR Dir (Container):    $FLINK_JAR_DIR_CONTAINER"
log "Parallelism:            $PARALLELISM"
log "Skip Build:             $SKIP_BUILD"
log "Detached Mode:          $DETACHED_MODE" # <<<<< ADDED Logging
[ -n "$FLINK_ARGS" ] && log "Flink Run Args:         $FLINK_ARGS"
[ -n "$JOB_ARGS" ] && log "Job Args:               $JOB_ARGS"
log "-----------------------------"


# --- Trap SIGINT (Ctrl+C) ---
# Set trap *after* variables like JOB_NAME and JOBMANAGER_CONTAINER are loaded
# The cleanup function now checks DETACHED_MODE
trap cleanup SIGINT

# --- Check JobManager Container (Optional) ---
if [ "${CHECK_CONTAINER_RUNNING:-false}" = true ]; then
    log "Checking if JobManager container '$JOBMANAGER_CONTAINER' is running..."
    if ! docker ps -f name="^/${JOBMANAGER_CONTAINER}$" --format '{{.Names}}' | grep -q "^${JOBMANAGER_CONTAINER}$"; then
         error_exit "JobManager container '$JOBMANAGER_CONTAINER' is not running or not found."
    fi
    log "JobManager container is running."
fi


# --- Build Project (if not skipped) ---
BUILD_LOG_PATH="${SCRIPT_DIR}/${BUILD_LOG_FILE:-build.log}" # Ensure log path is absolute or relative to script dir
if [ "$SKIP_BUILD" = false ]; then
    log "Building project using command: $BUILD_COMMAND"
    # Execute build command relative to script dir (or where config is expected)
    # Redirect stdout/stderr to log file
    if ! (cd "$SCRIPT_DIR" && eval "$BUILD_COMMAND") > "$BUILD_LOG_PATH" 2>&1; then
        error_exit "Build failed. Check build log for details: $BUILD_LOG_PATH"
    fi
    log "Build successful. Log: $BUILD_LOG_PATH"
    # Optional: remove build log on success
     rm "$BUILD_LOG_PATH"
else
    log "Skipping build step as requested."
fi

# --- Find the JAR File ---
log "Searching for JAR in '$SCRIPT_DIR/$BUILD_OUTPUT_DIR' matching pattern '$JAR_SEARCH_PATTERN'..."
# Use find for robustness, handle spaces in names. Count matches.
# Ensure BUILD_OUTPUT_DIR is treated relative to SCRIPT_DIR
JAR_CANDIDATES=()
while IFS= read -r -d $'\0'; do
    JAR_CANDIDATES+=("$REPLY")
done < <(find "$SCRIPT_DIR/$BUILD_OUTPUT_DIR" -maxdepth 1 -name "$JAR_SEARCH_PATTERN" -print0)

# Check number of JARs found
if [ ${#JAR_CANDIDATES[@]} -eq 0 ]; then
    error_exit "No JAR file found in '$SCRIPT_DIR/$BUILD_OUTPUT_DIR' matching pattern '$JAR_SEARCH_PATTERN'. Check build output and pattern."
elif [ ${#JAR_CANDIDATES[@]} -gt 1 ]; then
    error_exit "Multiple JAR files found matching pattern '$JAR_SEARCH_PATTERN' in '$SCRIPT_DIR/$BUILD_OUTPUT_DIR'. Please refine the pattern:\n${JAR_CANDIDATES[*]}"
fi

# Exactly one JAR found
LOCAL_JAR_PATH="${JAR_CANDIDATES[0]}"
JAR_NAME=$(basename "$LOCAL_JAR_PATH")
FLINK_JAR_PATH_CONTAINER="${FLINK_JAR_DIR_CONTAINER}/${JAR_NAME}" # Construct path inside container

log "Found JAR: $LOCAL_JAR_PATH"
log "JAR will be referenced inside container as: $FLINK_JAR_PATH_CONTAINER"
log "(Ensure '$SCRIPT_DIR/$BUILD_OUTPUT_DIR' is correctly volume-mapped to '$FLINK_JAR_DIR_CONTAINER' in Docker/Compose)"

# --- Submit Flink Job ---
log "Submitting Flink job '$JOB_NAME' via container '$JOBMANAGER_CONTAINER'..."

# Build the command arguments array for 'flink run'
# Using an array avoids issues with spaces in arguments
# NOTE: The existing script already used '-d' for flink run, which tells Flink CLI
#       not to wait for the job to finish. Our new script -d controls whether
#       the *script itself* waits and monitors.
FLINK_RUN_CMD_ARGS=(
    flink run -d # Run detached (Flink CLI client exits after submission)
    -c "$MAIN_CLASS"
    --parallelism "$PARALLELISM"
    # Add extra Flink args - handle potential spaces using eval or preferably split if needed
    # Simple approach assumes FLINK_ARGS doesn't need complex parsing:
    ${FLINK_ARGS:-} # Use ${VAR:-} to avoid error if empty
    "$FLINK_JAR_PATH_CONTAINER"
    # Add job args
    ${JOB_ARGS:-} # Use ${VAR:-} to avoid error if empty
)

log "Executing: docker exec '$JOBMANAGER_CONTAINER' ${FLINK_RUN_CMD_ARGS[*]}"

# Execute flink run inside the container, capturing combined output and checking status
# Use an 'if !' structure to prevent set -e from exiting immediately on failure
if ! JOB_OUTPUT=$(docker exec "$JOBMANAGER_CONTAINER" "${FLINK_RUN_CMD_ARGS[@]}" 2>&1); then
    # Command failed (docker exec returned non-zero)
    FLINK_RUN_EXIT_CODE=$? # Capture the actual exit code
    error_exit "Failed to submit Flink job '$JOB_NAME' (docker exec exit code: $FLINK_RUN_EXIT_CODE). Output:\n$JOB_OUTPUT"
fi

# --- If we reach here, docker exec command itself succeeded (exit code 0) ---
# Now we need to check the *output* for Flink's confirmation and extract the Job ID

# Extract the Job ID (32 hex characters) from the captured output
JOB_ID=$(echo "$JOB_OUTPUT" | grep -oE '[0-9a-f]{32}')

# Check if we successfully extracted a Job ID AND saw a success message in the output
# It's possible 'docker exec' returns 0 but Flink reported an issue in stdout/stderr
if [ -z "$JOB_ID" ] || ! echo "$JOB_OUTPUT" | grep -q -E "Job has been submitted with JobID|job has been submitted with Job ID"; then
    # Even if docker exec returned 0, Flink didn't confirm success in the output.
    error_exit "Job '$JOB_NAME' submission command executed (exit code 0), but failed to confirm successful submission or extract Job ID from output. Output:\n$JOB_OUTPUT\nCheck Flink JobManager logs."
fi

# --- Success ---
log "Flink job '$JOB_NAME' submitted successfully with Job ID: $JOB_ID"
log "Full Flink Run Output:\n$JOB_OUTPUT" # Log the output for reference

# --- Exit if Detached Mode --- # <<<<< ADDED Conditional Exit
if [ "$DETACHED_MODE" = true ]; then
    log "Detached mode enabled. Script will now exit."
    exit 0
fi

# --- Wait and Monitor Job Status (Only if NOT detached) ---
log "Monitoring job status. Press Ctrl+C to attempt cancellation and exit."

CHECK_INTERVAL=15 # Check status every 15 seconds
STATUS_CMD="flink list -a" # Use '-a' to see finished/failed jobs too

while true; do
    sleep "$CHECK_INTERVAL"

    log "Checking status of job '$JOB_NAME' ($JOB_ID)..."
    JOB_STATUS_OUTPUT=""
    JOB_LINE=""
    JOB_STATE=""

    # Execute the command to list jobs inside the container
    # Use 'if !' structure to handle docker exec failures gracefully with set -e
    if ! JOB_STATUS_OUTPUT=$(docker exec "$JOBMANAGER_CONTAINER" $STATUS_CMD 2>&1); then
        # This could be a temporary docker issue or the container stopped
        log "Warning: Failed to execute '$STATUS_CMD' in container '$JOBMANAGER_CONTAINER'. Retrying next cycle." >&2
        # Optional: Implement retry logic or exit after N failures
        # For now, just log warning and continue the loop hoping it's temporary
        continue
    fi

    # Try to find the line corresponding to our job ID
    JOB_LINE=$(echo "$JOB_STATUS_OUTPUT" | grep "$JOB_ID")

    if [ -z "$JOB_LINE" ]; then
        # Job ID not found in the list. This could mean it finished very quickly OR there's an issue.
        # Let's check recent finished jobs explicitly if possible, although '-a' should cover it.
        # For simplicity, we'll treat missing ID after successful submission as potentially finished/failed/cleaned up.
        log "Job '$JOB_NAME' ($JOB_ID) not found in Flink job list ('flink list -a'). Assuming it finished, failed, or was cleaned up."
        # Check the jobmanager logs for final status if needed.
        # We exit non-zero because we lost track of it unexpectedly.
        error_exit "Lost track of Job '$JOB_NAME' ($JOB_ID). Check Flink JobManager logs for final status. List output:\n$JOB_STATUS_OUTPUT"
    fi

    # Extract the state - typically the last field in parentheses, e.g., "(RUNNING)"
    # This regex tries to safely extract the state like (STATE) at the end of the line
    JOB_STATE=$(echo "$JOB_LINE" | grep -oE '\([A-Z]+\)$' | tr -d '()')

    if [ -z "$JOB_STATE" ]; then
        log "Warning: Could not determine state for job '$JOB_NAME' ($JOB_ID) from line: $JOB_LINE. Will check again." >&2
        continue # Continue loop and check again later
    fi

    log "Current state of job '$JOB_NAME' ($JOB_ID): $JOB_STATE"

    # Evaluate the state
    case "$JOB_STATE" in
        RUNNING|RESTARTING|INITIALIZING|CREATED|RECONCILING) # Added RECONCILING
            # Job is alive, continue monitoring
            ;;
        FINISHED)
            log "Job '$JOB_NAME' ($JOB_ID) finished successfully."
            exit 0 # Exit script gracefully
            ;;
        FAILED|CANCELED|SUSPENDED|FAILING|CANCELLING)
            # Job is in a terminal failure or stopped state
            error_exit "Job '$JOB_NAME' ($JOB_ID) is in state: $JOB_STATE. Exiting."
            ;;
        *)
            # Unknown state encountered
            log "Warning: Encountered unknown job state '$JOB_STATE' for job '$JOB_NAME' ($JOB_ID). Continuing monitoring." >&2
            ;;
    esac
done

# This part should ideally not be reached if the loop logic is correct
log "Monitoring loop exited unexpectedly."
exit 1