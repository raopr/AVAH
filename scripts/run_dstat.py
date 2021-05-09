#!/usr/bin/env python3
#
import sys
import getopt
import subprocess
import time

def main():
    if (len(sys.argv) < 2):
        usage(sys.argv[0])
        sys.exit(2)

    command = sys.argv[1]
    num_hosts = int(sys.argv[2])
    output_file = "/mydata/report.csv"
    time_interval = "30"

    print("Num hosts", num_hosts)
    if (command=="start"):
        for i in range(1, num_hosts):
            run_cmd = "ssh vm{} rm -rf {}".format(i, output_file)
            run_ret = subprocess.call(run_cmd, shell=True)
            print(run_cmd)
            run_cmd = "ssh vm{} screen -dmS MYDSTAT dstat -t -l --output {} {}".\
                format(i, output_file, time_interval)
            print(run_cmd)
            run_ret = subprocess.call(run_cmd, shell=True)
    elif (command=="stop"):
        for i in range(1, num_hosts):
            run_cmd = "ssh vm{} screen -S MYDSTAT -X quit".format(i)
            print(run_cmd)
            run_ret = subprocess.call(run_cmd, shell=True)
    elif (command=="collect"):
        for i in range(1, num_hosts):
            run_cmd = "scp vm{}{}{} vm{}-report.out".format(i, ":", output_file, i)
            print(run_cmd)
            run_ret = subprocess.call(run_cmd, shell=True)
    else:
        print("Unsupported command")
        usage(sys.argv[0])

def usage(prog_name):
    print("python3 {} <command> <num_nodes>".format(prog_name))
    print(" Commands:")
    print(" start    - start dstat on all nodes")
    print(" stop     - stop dstat on all nodes")
    print(" collect  - get the reports from all nodes")

if __name__ == "__main__":
    main()
    print("👉 Done!")