#!/bin/bash
set -euo pipefail

# Configuration for AWS/SSH (Must match orchestrate_friend_recommendation.sh)
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
KEY_PATH="$HOME/.ssh/labsuser.pem" # Assuming key is copied here by orchestrate.sh
SSH_USER="ubuntu"

# Helper: wait for SSH to be ready
wait_ssh() {
    local host="$1"
    local key="$2"
    local user="$3"
    local opts="-o StrictHostKeyChecking=no -o ServerAliveInterval=30 -o ServerAliveCountMax=10 -o ConnectTimeout=5"

    echo "Waiting for SSH to be ready on $host..."
    for i in {1..60}; do
        if ssh -i "$key" $opts "${user}@$host" "echo ok" >/dev/null 2>&1; then
            echo "SSH is ready on $host."
            return 0
        fi
        sleep 3
    done
    echo "ERROR: SSH not ready on $host after 3 minutes." >&2
    exit 1
}

echo "Launching benchmark instance..."

# Launch instance and capture JSON output
python3 launch_benchmark_instance.py > launch_benchmark_output.json

# Wait for the instance to transition to the running state
sleep 5

# Extract Public IP from the launch output using jq
if command -v jq >/dev/null 2>&1; then
    BENCH_IP=$(jq -r '..|.PublicIpAddress?|select(.)' launch_benchmark_output.json | head -n1)
else
    # Fallback parsing if jq not installed
    BENCH_IP=$(grep -oE '[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+' launch_benchmark_output.json | head -n1)
fi

if [ -z "$BENCH_IP" ]; then
    echo "Could not extract public IP from instance launch."
    exit 1
fi

echo "Benchmark instance running at $BENCH_IP"

# Wait for SSH to be ready before running the controller
wait_ssh "$BENCH_IP" "$KEY_PATH" "$SSH_USER"

# Run benchmark controller
echo "Running benchmark controller..."
# Controller uses constants for user/key, only needs --host
python3 tools/benchmark_controller.py --host "$BENCH_IP" --bootstrap-timeout-sec 300 || {
    echo "Benchmark controller failed. Check output above for details."
    exit 1
}

echo "Benchmark orchestration complete. Results in ./benchmarks/results/"
