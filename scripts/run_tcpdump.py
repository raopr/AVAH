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
    output_file = "/mydata/tcpdump-report"
    screen_name = "MYDSTAT"
    time_interval = 1800 # 30 mins
    target_dir = "/mydata/"

    print("Num hosts", num_hosts)
    if (command=="start"):
        for i in range(1, num_hosts):
            run_cmd = "ssh vm{} rm -rf {}*.pcap*".format(i, output_file)
            run_ret = subprocess.call(run_cmd, shell=True)
            print(run_cmd)

            ip_addr_cmd = ''' ssh vm{} bash -c "'grep 'vm{}-lan' /etc/hosts | cut -f 1'" '''.format(i, i)

            with open('temp.txt', 'w+') as fout:
                run_ret = subprocess.call(ip_addr_cmd, shell=True, stdout=fout)
                fout.seek(0)
                ip_addr = fout.read().strip()

            print("Host IP address: ", ip_addr)

            interface_cmd = ''' ssh vm{} bash -c "'ip r | grep {} | cut -f 3 -d \\" \\" '"  '''.format(i, ip_addr)

            with open('temp.txt', 'w+') as fout:
                run_ret = subprocess.call(interface_cmd, shell=True, stdout=fout)
                fout.seek(0)
                interface_name = fout.read().strip()

            print("Host interface name: ", interface_name)

            #run_cmd = """ssh vm{} "screen -dmS {} sudo tshark -w - | gzip -9 -f > {}" """. \
            #    format(i, screen_name, output_file)
            run_cmd = """ssh vm{} "screen -dmS {} sudo tcpdump -G {} -i {} -w '{}_%Y-%m-%d_%H:%M:%S_vm{}.pcap' -z gzip -s 94" """. \
               format(i, screen_name, time_interval, interface_name, output_file, i)
            print(run_cmd)
            run_ret = subprocess.call(run_cmd, shell=True)
    elif (command=="stop"):
        for i in range(1, num_hosts):
            run_cmd = "ssh vm{} screen -S {} -X quit".format(i, screen_name)
            print(run_cmd)
            run_ret = subprocess.call(run_cmd, shell=True)
    elif (command=="merge"):
        for i in range(1, num_hosts):
            run_cmd = ''' ssh vm{} bash -c "mergecap -w {}tcpdump-report-merged_vm{}.pcap.gz {}tcpdump-report*.pcap.gz" '''\
                .format(i, target_dir, i, target_dir)
            print(run_cmd)
            run_ret = subprocess.call(run_cmd, shell=True)
    elif (command=="clean"):
        for i in range(1, num_hosts):
            run_cmd = "ssh vm{} rm -rf {}*.pcap*".format(i, output_file)
            print(run_cmd)
            run_ret = subprocess.call(run_cmd, shell=True)
    elif (command=="collect"):
        for i in range(1, num_hosts):
            run_cmd = "scp vm{}{}{}*merged_vm{}.pcap.gz {}".format(i, ":", output_file, i, target_dir)
            print(run_cmd)
            run_ret = subprocess.call(run_cmd, shell=True)
    else:
        print("Unsupported command")
        usage(sys.argv[0])

def usage(prog_name):
    print("python3 {} <command> <num_nodes> [attr_name]".format(prog_name))
    print("")
    print(" Commands:")
    print(" start    - start tcpdump on all nodes")
    print(" stop     - stop tcpdump on all nodes")
    print(" merge    - merge the reports on each node")
    print(" collect  - get the reports from all nodes")
    print(" clean    - delete the reports from all nodes")
    print("")
    print(" Required:")
    print(" num_nodes - cluster size")
    print("")

if __name__ == "__main__":
    main()
    print("👉 Done!")
