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
    elif (command == "set_mtu"):
        interface = sys.argv[3]
        mtu_value = sys.argv[4]
        for i in range(0, num_nodes):
            print("================ vm{} ================".format(i))
            run_cmd = "ssh vm{} sudo ip link set {} mtu {}".format(i, interface, mtu_value)
            run_ret = subprocess.call(run_cmd, shell=True)
    elif (command == "set_tcp_win"):
        max_win_size = sys.argv[3]
        default_size = sys.argv[4]
        for i in range(0, num_nodes):
            print("================ vm{} ================".format(i))
            run_cmd = "ssh vm{} sudo sysctl -w " \
                        "net.core.rmem_max={} net.core.wmem_max={} " \
                        "net.ipv4.tcp_rmem=\"4096 {} {}\" net.ipv4.tcp_wmem=\"4096 {} {}\" "\
                        "net.ipv4.route.flush=1" \
                        .format(i, max_win_size, max_win_size, default_size, max_win_size,
                                default_size, max_win_size)
            run_ret = subprocess.call(run_cmd, shell=True)
    else:
        print("Unsupported command")
        usage(sys.argv[0])
        sys.exit(2)

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
    print(" set_mtu - set MTU value on all nodes")
    print("     <arg1>   - interface name")
    print("     <arg2>   - MTU value (e.g., 9000)")
    print("")
    print(" set_tcp_win - set TCP window size on all nodes")
    print("     <arg1>   - max. window size")
    print("     <arg2>   - default size")
    print("")

if __name__ == "__main__":
    main()
    print("👉 Done!")