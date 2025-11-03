import sys
from collections import defaultdict

def reducer_top10():
    reducer_recommendations = defaultdict(lambda: defaultdict(int)) # Dict[int, Dict[int, int]]: user -> (potential_friend -> mutual_friends_count)

    for line in sys.stdin:
        
        user_data = line.strip()
        if not user_data:
            continue

        data = user_data.split('\t')
        if len(data) == 1:
            reducer_recommendations[int(data[0])] = {} # no friends
            continue

        flags = list(map(int, data[2].split(',')))
        user1, user2 = int(data[0]), int(data[1])

        if (0 in flags):
            mutual_friends_count = 0 # direct friend
        else:
            mutual_friends_count = sum(flags) # potential friend
        
        reducer_recommendations[user1][user2] += mutual_friends_count
        reducer_recommendations[user2][user1] += mutual_friends_count

    for user in sorted(reducer_recommendations):
        candidates = reducer_recommendations[user]
        if (candidates == {}):
            print(f"{user}\t")
        else:
            ordered = sorted(
                filter(lambda kv: kv[1] != 0, candidates.items()), 
                key=lambda kv: (-kv[1], kv[0])
            )[:10]
            top10_users = [str(user) for user, _ in ordered]
            print(f"{user}\t{','.join(top10_users)}")

if __name__ == "__main__":
    reducer_top10()