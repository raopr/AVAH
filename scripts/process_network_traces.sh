#!/usr/bin/env bash

if [[ $# -lt 1 ]]; then
    echo "Usage: process_network_traces.sh <host_name>"
    exit
fi

DATA_DIR="/mydata/"
HOST_ID=${1}

echo "👉 Processing .pcap files for host "${HOST_ID}" in "${DATA_DIR}

# merge the pcap files
mergecap -w ${DATA_DIR}tcpdump-report-merged.pcap.gz ${DATA_DIR}tcpdump-report_*.pcap.gz
echo "👍 Completed merging pcap files"

# IP/TCP conversations
tshark -q -r ${DATA_DIR}tcpdump-report-merged.pcap.gz -z conv,ip |& grep -v -E 'WARNING|$^' >& ${DATA_DIR}${HOST_ID}-conv-stat.txt
tshark -q -r ${DATA_DIR}tcpdump-report-merged.pcap.gz -z conv,tcp |& grep -v -E 'WARNING|$^' >& ${DATA_DIR}${HOST_ID}-conv-tcp-stat.txt
#tshark -q -r tcpdump-report-merged_vm1.pcap.gz -z flow,tcp,network >& flow-tcp-stat.txt
echo "👍 Completed IP/TCP conversation statistics"

# IP endpoints
tshark -q -r ${DATA_DIR}tcpdump-report-merged.pcap.gz -z endpoints,ip |& grep -v -E 'WARNING|$^' >& ${DATA_DIR}${HOST_ID}-endpoints-ip-stat.txt
tshark -q -r ${DATA_DIR}tcpdump-report-merged.pcap.gz -z endpoints,tcp |& grep -v -E 'WARNING|$^' >& ${DATA_DIR}${HOST_ID}-endpoints-tcp-stat.txt
echo "👍 Completed IP/TCP endpoint statistics"

# I/O statistics, every 60 seconds
tshark -q -r ${DATA_DIR}tcpdump-report-merged.pcap.gz -z io,stat,60,ip,"MAX(tcp.analysis.ack_rtt)tcp.analysis.ack_rtt,MIN(tcp.analysis.ack_rtt)tcp.analysis.ack_rtt,AVG(tcp.analysis.ack_rtt)tcp.analysis.ack_rtt,MAX(tcp.analysis.bytes_in_flight)tcp.analysis.bytes_in_flight,MIN(tcp.analysis.bytes_in_flight)tcp.analysis.bytes_in_flight,AVG(tcp.analysis.bytes_in_flight)tcp.analysis.bytes_in_flight,MAX(tcp.window_size)tcp.window_size,MIN(tcp.window_size)tcp.window_size,AVG(tcp.window_size)tcp.window_size,MAX(tcp.len)tcp.len,MIN(tcp.len)tcp.len,AVG(tcp.len)tcp.len,MAX(ip.len)ip.len,MIN(ip.len)ip.len,AVG(ip.len)ip.len" |& grep -v -E 'WARNING|$^' >& ${DATA_DIR}${HOST_ID}-io-stat-60.txt
echo "👍 Completed I/O statistics"

# Packet lengths
tshark -q -r ${DATA_DIR}tcpdump-report-merged.pcap.gz -z plen,tree |& grep -v -E 'WARNING|$^' >& ${DATA_DIR}${HOST_ID}-plen-tree.txt
echo "👍 Completed packet length statistics"

# IP src-dst summary
tshark -q -r ${DATA_DIR}tcpdump-report-merged.pcap.gz -z ip_hosts,tree |& grep -v -E 'WARNING|$^' >& ${DATA_DIR}${HOST_ID}-ip-hosts-tree.txt
tshark -q -r ${DATA_DIR}tcpdump-report-merged.pcap.gz -z ip_srcdst,tree |& grep -v -E 'WARNING|$^' >& ${DATA_DIR}${HOST_ID}-ip-srcdst-tree.txt
echo "👍 Completed IP tree statistics"
