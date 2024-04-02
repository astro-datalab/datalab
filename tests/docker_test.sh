#!/bin/bash

# This script logs into the Datalab service, runs a specified SQL query asynchronously,
# waits for the query to complete, and then checks if the resulting CSV file contains
# the expected number of lines (excluding the header).

# Arguments:
#   $1 (USER_NAME): Username for Datalab login. Mandatory.
#   $2 (PASSWD): Password for Datalab login. If not provided, the script will asks you
#                for one.

# Usage:
#   ./script_name.sh <USER_NAME> [<PASSWD>]

# The script performs the following operations:
#   1. Logs into Datalab with the provided username and optional password.
#   2. Executes a predefined SQL query to fetch data from the GAIA DR3 source,
#      limiting the results to a specified number of lines.
#   3. Waits for the query to complete by periodically checking the job status.
#   4. Saves the query results to a CSV file named after the job ID.
#   5. Checks if the CSV file contains the expected number of lines (excluding the header)
#      and prints a message indicating whether the check was successful.

set -e

USER_NAME=$1
PASSWD=$2

if [ -z "${PASSWD}" ]
then
  datalab login --user="${USER_NAME}"
else
  datalab login --user="${USER_NAME}" --password="${PASSWD}"
fi

# The expected number of lines
expected_lines=100000
export SQL="SELECT ra, dec, parallax, pmra, pmdec, radial_velocity
FROM gaia_dr3.gaia_source
WHERE 't' = Q3C_RADIAL_QUERY(ra, dec, 16.95, -4.1, 21.48)
LIMIT ${expected_lines}
"

JOBID=$(datalab query --sql="$SQL" --async=True)
echo "Job ID for submitted async SQL: $JOBID"

STATUS=$(datalab qstatus --jobId="$JOBID")
while [[ "$STATUS" != "COMPLETED" && "$STATUS" != "ERROR" && "$STATUS" != "ABORTED" ]]
do
    echo "Current status of async query: $STATUS"
    sleep 5 # Avoid querying too frequently
    STATUS=$(datalab qstatus --jobId="$JOBID")
done
echo "Final status reached: $STATUS"

csv_file=${JOBID}.csv
echo "Saving results to filename:${csv_file}"
datalab qresults --jobId="$JOBID" --fname=${csv_file}


# Count the number of lines excluding the header
line_count=$(tail -n +2 "$csv_file" | wc -l)

# Compare the line count to the expected number
if [ "$line_count" -eq "$expected_lines" ]; then
  echo "The ${csv_file} has the correct number of lines: $expected_lines."
else
  echo "Error: The ${csv_file} does not have the expected number of lines. Expected $expected_lines, found $line_count."
  exit 1
fi