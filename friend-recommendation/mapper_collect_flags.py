import sys

def print_group(key, flags):
    flag_csv = ",".join(str(x) for x in flags)
    print(f"{key[0]}\t{key[1]}\t{flag_csv}")

def mapper_collect_flags():
    current_key = None
    collected_flags = []

    for line in sys.stdin:
        
        user_data = line.strip()
        if not user_data:
            continue

        data = user_data.split('\t')
        
        if len(data) == 1:
            if current_key:
                print_group(current_key, collected_flags)
                current_key = None
                collected_flags = []
            print(f"{data[0]}\t")
            continue

        user1_str, user2_str, flag_str = data
        current_line_key = (user1_str, user2_str)
        flag = int(flag_str)

        if current_key and current_key != current_line_key:
            print_group(current_key, collected_flags)
            collected_flags = []

        current_key = current_line_key
        collected_flags.append(flag)

    if current_key:
        print_group(current_key, collected_flags)

if __name__ == "__main__":
    mapper_collect_flags()
