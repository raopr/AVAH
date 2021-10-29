#!/usr/bin/env python3
#
import sys
import getopt
import subprocess
import time
import pandas as pd
import matplotlib.pyplot as plt

def main():
    if (len(sys.argv) < 2):
        usage(sys.argv[0])
        sys.exit(2)

    command = sys.argv[1]
    num_hosts = int(sys.argv[2])
    output_file = "/mydata/tshark-report.pcap.gz"
    screen_name = "MYDSTAT"
    report_name = "-tshark-report.pcap.gz"

    print("Num hosts", num_hosts)
    if (command=="start"):
        for i in range(1, num_hosts):
            run_cmd = "ssh vm{} rm -rf {}".format(i, output_file)
            run_ret = subprocess.call(run_cmd, shell=True)
            print(run_cmd)
            run_cmd = "ssh vm{} screen -dmS {} sudo tshark -w - | gzip -9 -f {}". \
                format(i, screen_name, output_file)
            print(run_cmd)
            run_ret = subprocess.call(run_cmd, shell=True)
    elif (command=="stop"):
        for i in range(1, num_hosts):
            run_cmd = "ssh vm{} screen -S {} -X quit".format(i, screen_name)
            print(run_cmd)
            run_ret = subprocess.call(run_cmd, shell=True)
    elif (command=="collect"):
        for i in range(1, num_hosts):
            run_cmd = "scp vm{}{}{} vm{}{}".format(i, ":", output_file, i, report_name)
            print(run_cmd)
            run_ret = subprocess.call(run_cmd, shell=True)
    else:
        print("Unsupported command")
        usage(sys.argv[0])

def usage(prog_name):
    print("python3 {} <command> <num_nodes> [attr_name]".format(prog_name))
    print("")
    print(" Commands:")
    print(" start    - start tshark on all nodes")
    print(" stop     - stop tshark on all nodes")
    print(" collect  - get the reports from all nodes")
    print("")
    print(" Required:")
    print(" num_nodes - cluster size")
    print("")

if __name__ == "__main__":
    main()
    print("👉 Done!")
