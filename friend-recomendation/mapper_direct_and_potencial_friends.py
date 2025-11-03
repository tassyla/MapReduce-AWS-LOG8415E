import sys

def mapper_direct_and_potencial_friends():
    # python3 mapper_direct_and_potencial_friends.py < soc-LiveJournal1Adj.txt > mapper_direct_and_potencial_friends_output.txt
    for line in sys.stdin:
        user_data = line.strip()
        if not user_data:
            continue
      
        data = user_data.split('\t', 1)
        if len(data) != 2:
            continue
        user = int(data[0])
        friends = data[1].split(',')
        if not friends:
            continue

        # Emit direct friendships
        for friend in friends:
            if int(user) < int(friend):
                print(f"{user}\t{friend}\t0")
            else:
                print(f"{friend}\t{user}\t0")

        # Emit potential friendships
        for i in range(len(friends)):
            for j in range(i + 1, len(friends)):
                friend1 = int(friends[i])
                friend2 = int(friends[j])
                if friend1 < friend2:
                    print(f"{friend1}\t{friend2}\t1")
                else:
                    print(f"{friend2}\t{friend1}\t1")

if __name__ == "__main__":
    mapper_direct_and_potencial_friends()
