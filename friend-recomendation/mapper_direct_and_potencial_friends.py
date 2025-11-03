import sys

def mapper_direct_and_potencial_friends():

    for line in sys.stdin: # python3 mapper_direct_and_potencial_friends.py soc-LiveJournal1Adj.txt > mapper_direct_and_potencial_friends_output.txt
        user_data = line.strip()
        if not user_data:
            continue
      
        user_str, friends_str = user_data.split('\t', 1)
        user = int(user_str)
        friends = friends_str.split(',')
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
