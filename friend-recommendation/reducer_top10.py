import sys
import os
import tempfile
from collections import defaultdict

def calculate_scores_and_dump(input_stream, output_file_path):
    current_key = None
    current_pair_scores = defaultdict(int)

    with open(output_file_path, 'w') as tmp_file:
        for line in input_stream:
            user_data = line.strip()
            if not user_data:
                continue

            data = user_data.split('\t')
            
            if len(data) == 1:
                if current_key:
                    for friend, score in current_pair_scores.items():
                        # EMIT: user1 -> user2
                        tmp_file.write(f"{current_key}\t{friend}\t{score}\n")
                        # EMIT: user2 -> user1
                        tmp_file.write(f"{friend}\t{current_key}\t{score}\n")
                    current_pair_scores.clear()
                
                tmp_file.write(f"{data[0]}\t\n")
                current_key = None
                continue

            flags = list(map(int, data[2].split(',')))
            user1, user2 = int(data[0]), int(data[1])
            
            streaming_key = user1

            if current_key is None:
                current_key = streaming_key
            
            if streaming_key != current_key:
                for friend, score in current_pair_scores.items():
                    # EMIT: user1 -> user2
                    tmp_file.write(f"{current_key}\t{friend}\t{score}\n")
                    # EMIT: user2 -> user1
                    tmp_file.write(f"{friend}\t{current_key}\t{score}\n")
                
                current_key = streaming_key
                current_pair_scores.clear()

            # Calculate score for the current line
            mutual_friends_count = 0
            if 0 not in flags:
                mutual_friends_count = sum(flags)

            # Accumulate score for user1 (key) and user2 (friend)
            current_pair_scores[user2] += mutual_friends_count
            
        if current_key is not None:
            for friend, score in current_pair_scores.items():
                # EMIT: user1 -> user2
                tmp_file.write(f"{current_key}\t{friend}\t{score}\n")
                # EMIT: user2 -> user1d)
                tmp_file.write(f"{friend}\t{current_key}\t{score}\n")
        
    return output_file_path

def find_top10_streaming(input_file_path):
    current_user = None
    current_candidates = {}

    with open(input_file_path, 'r') as tmp_file:
        for line in tmp_file:
            user_data = line.strip()
            if not user_data:
                continue

            data = user_data.split('\t')
            
            if len(data) == 1:
                if current_user:
                    emit_top10(current_user, current_candidates)
                    current_candidates.clear()
                print(f"{data[0]}\t") # Print user with no friends
                current_user = None
                continue

            user, friend, score_str = data
            user, friend, score = int(user), int(friend), int(score_str)

            if current_user is None:
                current_user = user
            
            if user != current_user:
                emit_top10(current_user, current_candidates)
                
                current_user = user
                current_candidates.clear()
            
            current_candidates[friend] = score
        
        if current_user is not None:
            emit_top10(current_user, current_candidates)

def emit_top10(user, candidates):
    ordered = sorted(
        filter(lambda kv: kv[1] != 0, candidates.items()),
        key=lambda kv: (-kv[1], kv[0]) # Sort by score DESC, then user ID ASC
    )[:10]
    
    top10_users = [str(user) for user, _ in ordered]
    print(f"{user}\t{','.join(top10_users)}")


def reducer_top10_streaming_two_pass():
    # Use the /tmp directory for intermediate file storage
    temp_file = os.path.join(tempfile.gettempdir(), 'reducer_pass1_output.txt')
    
    intermediate_file = calculate_scores_and_dump(sys.stdin, temp_file)
    sorted_intermediate_file = os.path.join(tempfile.gettempdir(), 'reducer_pass1_output_sorted.txt')

    os.system(f"sort -T {tempfile.gettempdir()} -t '\x09' -k1,1n {intermediate_file} > {sorted_intermediate_file}")
    
    find_top10_streaming(sorted_intermediate_file)
    
    os.remove(temp_file)
    os.remove(sorted_intermediate_file)

if __name__ == "__main__":
    reducer_top10_streaming_two_pass()