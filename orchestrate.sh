# run from script dir
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"


# Friend recommendation solution
cd "$SCRIPT_DIR/friend-recommendation"

INPUT="input/soc-LiveJournal1Adj.txt"

RESULTS_FOLDER="results"
rm -rf "$RESULTS_FOLDER" && mkdir -p "$RESULTS_FOLDER"

OUT1="$RESULTS_FOLDER/mapper_direct_and_potencial_friends_output.txt"
OUT2="$RESULTS_FOLDER/mapper_collect_flags_output.txt"
OUT3="$RESULTS_FOLDER/reducer_top10_output.txt"

echo "1/3: mapper_direct_and_potencial_friends.py -> $OUT1"
python3 mapper_direct_and_potencial_friends.py < "$INPUT" > "$OUT1"

echo "2/3: mapper_collect_flags.py -> $OUT2"
python3 mapper_collect_flags.py < "$OUT1" > "$OUT2"

echo "3/3: reducer_top10.py -> $OUT3"
python3 reducer_top10.py < "$OUT2" > "$OUT3"

echo "FINAL OUTPUT: $OUT3"