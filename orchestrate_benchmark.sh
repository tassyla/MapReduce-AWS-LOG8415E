echo "Launching benchmark instance..."

# Launch instance and capture JSON output
python3 launch_benchmark_instance.py > launch_benchmark_output.json

# Wait 15 s to allow instance to initialize
sleep 15

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

# Run benchmark controller (adjust args if needed)
echo "Running benchmark controller..."
python3 benchmark_controller.py --host "$BENCH_IP" --duration 60 --rate 200 || echo "Benchmark may require different flags."

# Create local results folder
mkdir -p benchmarks/results

# Try to retrieve any benchmark result files from instance
echo "Retrieving benchmark results..."
scp -o StrictHostKeyChecking=no -i labsuser.pem ubuntu@"$BENCH_IP":~/benchmark_results* benchmarks/results/ || echo "No results retrieved or remote path differs."

echo "Benchmark orchestration complete. Results in ./benchmarks/results/"
