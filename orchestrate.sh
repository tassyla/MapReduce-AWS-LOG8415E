# Make scripts executable
chmod +x orchestrate_benchmark.sh
chmod +x orchestrate_friend_recommendation.sh

# Run orchestration scripts
./orchestrate_benchmark.sh
./orchestrate_friend_recommendation.sh

echo "All orchestration complete!"
