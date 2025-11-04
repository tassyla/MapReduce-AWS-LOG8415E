set -euo pipefail

# run from script dir
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# Configuration
mkdir -p ~/.ssh
cp -f "$SCRIPT_DIR/labsuser.pem" ~/.ssh/labsuser.pem
KEY_PATH="$HOME/.ssh/labsuser.pem"
chmod 600 "$KEY_PATH" # Ensure key permissions (inside WSL)
SSH_USER="ubuntu"
REGION="us-east-1"

# Directories
FR_DIR="$SCRIPT_DIR/friend-recommendation"
RESULTS_DIR="$FR_DIR/results"

# Local files
MAPPER1="$FR_DIR/mapper_direct_and_potencial_friends.py"
MAPPER2="$FR_DIR/mapper_collect_flags.py"
REDUCER="$FR_DIR/reducer_top10.py"
INPUT_TXT="$FR_DIR/input/soc-LiveJournal1Adj.txt"

# Remote locations
R_TMP="/tmp"
R_MAPPER1="$R_TMP/mapper1.py"
R_MAPPER2="$R_TMP/mapper2.py"
R_REDUCER="$R_TMP/reducer.py"
R_INPUT="$R_TMP/input.txt"
R_PART1="$R_TMP/part1.txt"
R_PART2="$R_TMP/part2.txt"
R_OUT="$R_TMP/recommendations.txt"

# Prepare results dir
rm -rf "$RESULTS_DIR" && mkdir -p "$RESULTS_DIR"

# Determine S3 bucket to use
if [ -z "${FRIEND_BUCKET:-}" ]; then
  FRIEND_BUCKET="friend-reco-$(date +%s)"
  echo "[Auto] Using generated S3 bucket: $FRIEND_BUCKET"
fi

# Launch friend recommendation instances
echo ">>> Launching 3 instances for friend recommendation..."
LAUNCH_OUT="$(python3 "$SCRIPT_DIR/launch_friend_recommendation_instances.py" --region "$REGION" --bucket "$FRIEND_BUCKET")"
echo "$LAUNCH_OUT"

# Export IPs from launcher output
echo ">>> Exporting instance IPs..."
eval "$(printf '%s\n' "$LAUNCH_OUT" | tr -d '\r' | grep -E '^export ')"
echo "M1=${MAPPER1_IP:-unset}  M2=${MAPPER2_IP:-unset}  R=${REDUCER_IP:-unset}  BUCKET=${FRIEND_BUCKET:-unset}"

# SSH options
SSH_OPTS="-o StrictHostKeyChecking=no -o ServerAliveInterval=30 -o ServerAliveCountMax=10"

# Helper: wait for SSH to be ready
wait_ssh() {
  local host="$1"
  for i in {1..60}; do
    if ssh -i "$KEY_PATH" $SSH_OPTS -o ConnectTimeout=5 "ubuntu@$host" "echo ok" >/dev/null 2>&1; then
      return 0
    fi
    sleep 3
  done
  echo "ERROR: SSH not ready on $host" >&2
  exit 1
}

sleep 10
wait_ssh "$MAPPER1_IP"
wait_ssh "$MAPPER2_IP"
wait_ssh "$REDUCER_IP"

# Helper: SSH/SCP wrappers
ssh_nk() {
  ssh -i "$KEY_PATH" $SSH_OPTS "${SSH_USER}@${1}" "${2}"
}
scp_nk() {
  scp -i "$KEY_PATH" -o StrictHostKeyChecking=no "$1" "$2"
}

# Ensure python3 is present
for HOST in "$MAPPER1_IP" "$MAPPER2_IP" "$REDUCER_IP"; do
  echo ">>> Ensuring Python on $HOST"
  ssh_nk "$HOST" "which python3 >/dev/null 2>&1 || { sudo apt-get update -y && sudo apt-get install -y python3; }"
done

# Upload scripts and input, run mappers/reducer
echo ">>> Uploading scripts and input to mappers/reducer..."
scp_nk "$MAPPER1" "${SSH_USER}@${MAPPER1_IP}:${R_MAPPER1}"
scp_nk "$INPUT_TXT" "${SSH_USER}@${MAPPER1_IP}:${R_INPUT}"
scp_nk "$MAPPER2" "${SSH_USER}@${MAPPER2_IP}:${R_MAPPER2}"
scp_nk "$REDUCER"  "${SSH_USER}@${REDUCER_IP}:${R_REDUCER}"

# Run mapper1 (input.txt -> part1.txt)
echo ">>> Running mapper1 on ${MAPPER1_IP}..."
ssh_nk "$MAPPER1_IP" "/usr/bin/time -p python3 -u ${R_MAPPER1} < ${R_INPUT} > ${R_PART1} 2> /tmp/mapper1.time && tail -n +1 /tmp/mapper1.time"

# Copy part1.txt from mapper1 to mapper2
echo '>>> Transferring part1 -> mapper2 (via local scp -3)...'
scp -3 -i "$KEY_PATH" -o StrictHostKeyChecking=no \
  "${SSH_USER}@${MAPPER1_IP}:${R_PART1}" \
  "${SSH_USER}@${MAPPER2_IP}:${R_TMP}/mapper2_unsorted_input.txt"

# Suffle/Sort part1.txt on mapper2
echo ">>> Sorting mapper2 input on ${MAPPER2_IP} (Shuffle step)..."
ssh_nk "$MAPPER2_IP" "sort -T ${R_TMP} -t '\t' -k1,1n -k2,2n ${R_TMP}/mapper2_unsorted_input.txt > ${R_TMP}/mapper2_input.txt"
ssh_nk "$MAPPER2_IP" "rm ${R_TMP}/mapper2_unsorted_input.txt" # Clean up original unsorted file

# Run mapper2 (part1.txt -> part2.txt)
echo ">>> Running mapper2 on ${MAPPER2_IP}..."
ssh_nk "$MAPPER2_IP" "/usr/bin/time -p python3 -u ${R_MAPPER2} < ${R_TMP}/mapper2_input.txt > ${R_PART2} 2> /tmp/mapper2.time && tail -n +1 /tmp/mapper2.time"

# Copy part2.txt from mapper2 to reducer
echo '>>> Transferring part2 -> reducer (via local scp -3)...'
scp -3 -i "$KEY_PATH" -o StrictHostKeyChecking=no \
  "${SSH_USER}@${MAPPER2_IP}:${R_PART2}" \
  "${SSH_USER}@${REDUCER_IP}:${R_TMP}/reducer_input.txt"

# Run reducer (reducer_input.txt -> recommendations.txt)
echo ">>> Running reducer on ${REDUCER_IP}..."
ssh_nk "$REDUCER_IP" "/usr/bin/time -p python3 -u ${R_REDUCER} < ${R_TMP}/reducer_input.txt > ${R_OUT} 2> /tmp/reducer.time && tail -n +1 /tmp/reducer.time"

# Pull results locally
scp_nk "${SSH_USER}@${REDUCER_IP}:${R_OUT}" "$RESULTS_DIR/recommendations.txt"
echo ">>> DONE. Results at $RESULTS_DIR/recommendations.txt"