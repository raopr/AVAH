#!/usr/bin/env python3
#
import sys
import getopt
import subprocess
import time
import pandas as pd
import matplotlib.pyplot as plt

def main():

    if (len(sys.argv) < 3):
        usage(sys.argv[0])
        sys.exit(2)

    command = sys.argv[1]
    num_nodes = int(sys.argv[2])

    if (command == "grep"):
        pattern = sys.argv[3]
        log_dir = sys.argv[4]
        for i in range(1, num_nodes):
            print("================ vm{} ================".format(i))
            run_cmd = "ssh vm{} grep -R {} {}".format(i, pattern, log_dir)
            run_ret = subprocess.call(run_cmd, shell=True)
    elif (command == "copy"):
        file = sys.argv[3]
        target_dir = sys.argv[4]
        for i in range(1, num_nodes):
            print("================ vm{} ================".format(i))
            run_cmd = "scp {} vm{}:{}".format(file, i, target_dir)
            run_ret = subprocess.call(run_cmd, shell=True)

def usage(prog_name):
    print("python3 {} <command> <num_nodes> <arg1> <arg2>".format(prog_name))
    print("")
    print(" Commands:")
    print(" copy    - copy a file to all worker nodes")
    print("     <arg1>   - file to copy")
    print("     <arg2>   - target directory")
    print("")
    print(" grep    - grep log files of all worker nodes")
    print("     <arg1>   - pattern")
    print("     <arg2>   - log directory")
    print("")

if __name__ == "__main__":
    main()
    print("👉 Done!")