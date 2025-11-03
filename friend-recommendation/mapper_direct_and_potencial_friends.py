import sys

def mapper_direct_and_potencial_friends():
    for line in sys.stdin:
        
        user_data = line.strip()
        if not user_data:
            continue
      
        data = user_data.split('\t')
        user = int(data[0])

        if len(data) == 1:
            print(f"{user}\t")
            continue
        
        friends = list(map(int, data[1].split(',')))

        # Emit direct friendships
        for friend in friends:
            if user < friend:
                print(f"{user}\t{friend}\t0")
            else:
                print(f"{friend}\t{user}\t0")

        # Emit potential friendships
        for i in range(len(friends)):
            for j in range(i + 1, len(friends)):
                if friends[i] < friends[j]:
                    print(f"{friends[i]}\t{friends[j]}\t1")
                else:
                    print(f"{friends[j]}\t{friends[i]}\t1")

if __name__ == "__main__":
    mapper_direct_and_potencial_friends()
