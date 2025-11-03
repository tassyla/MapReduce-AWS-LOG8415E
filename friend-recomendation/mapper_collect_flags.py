import sys
from collections import defaultdict

def mapper_collect_flags():
    # python3 mapper_collect_flags.py < mapper_direct_and_potencial_friends_output.txt > mapper_collect_flags_output.txt
    flags_by_pair = defaultdict(list) # Dict[tuple(int,int), List[int]]: collects all flags per pair

    for line in sys.stdin:
        user_data = line.strip()
        if not user_data:
            continue

        user1_str, user2_str, flag_str = user_data.split('\t', 2)
        flag = int(flag_str)

        flags_by_pair[(user1_str, user2_str)].append(flag)

    # Emit collected flags per user pair
    for (user1, user2), flags in sorted(flags_by_pair.items()):
        flag_csv = ",".join(str(x) for x in flags)
        print(f"{user1}\t{user2}\t{flag_csv}")

if __name__ == "__main__":
    mapper_collect_flags()
