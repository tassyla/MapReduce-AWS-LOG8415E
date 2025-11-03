import sys
from collections import defaultdict

def reducer_top10():
    # python3 reducer_top10.py < mapper_collect_flags_output.txt > reducer_top10_output.txt
    reducer_recommendations = defaultdict(lambda: defaultdict(int)) 

    for line in sys.stdin:
        user_data = line.strip()
        if not user_data:
            continue

        data = user_data.split('\t')
        if len(data) != 3:
            continue
        flags = [int(x) for x in data[2].split(',')]
        user1, user2 = int(data[0]), int(data[1])

        if (0 in flags):
            continue  # They are already direct friends

        mutual_friends_count = sum(1 for f in flags if f == 1)
        if mutual_friends_count <= 0:
            continue # No mutual friends => no recommendation value
        
        reducer_recommendations[user1][user2] += mutual_friends_count
        reducer_recommendations[user2][user1] += mutual_friends_count

    for user in sorted(reducer_recommendations):
        candidates = reducer_recommendations[user]
        ordered = sorted(candidates.items(), key=lambda kv: (-kv[1], kv[0]))
        top10_users = [str(user) for user, _ in ordered[:10]]
        print(f"{user}\t{','.join(top10_users)}")

if __name__ == "__main__":
    reducer_top10()