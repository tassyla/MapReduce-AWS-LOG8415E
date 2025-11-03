import sys
from collections import defaultdict

def mapper_collect_flags():
    flags_by_pair = defaultdict(list) # Dict[tuple(int,int), List[int]]: collects all flags per pair

    for line in sys.stdin:
        
        user_data = line.strip()
        if not user_data:
            continue

        data = user_data.split('\t')
        
        if len(data) == 1:
            print(f"{data[0]}\t")
            continue

        user1_str, user2_str, flag_str = data
        flag = int(flag_str)

        flags_by_pair[(user1_str, user2_str)].append(flag)

    # Emit collected flags per user pair
    for (user1, user2), flags in sorted(flags_by_pair.items()):
        flag_csv = ",".join(str(x) for x in flags)
        print(f"{user1}\t{user2}\t{flag_csv}")

if __name__ == "__main__":
    mapper_collect_flags()
