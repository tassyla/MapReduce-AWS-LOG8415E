import sys

def mapper_collect_flags():
    # output: (0,1) 0,1,1,1,1,0,1
    for line in sys.stdin: # python3 mapper_collect_flags.py soc-LiveJournal1Adj.txt > mapper_collect_flags_output.txt
        user_data = line.strip()
        if not user_data:
            continue
      
        pair_user_str, count = user_data.split('\t', 1)

        # to continue...

if __name__ == "__main__":
    mapper_collect_flags()
