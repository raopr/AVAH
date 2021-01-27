#!/usr/bin/env bash

CANNOLI_HOME_DIR="/mydata/cannoli"
ADAM_HOME_DIR="/mydata/adam"
SPARK_HOME_DIR="/mydata/spark"
HADOOP_HOME_DIR="/mydata/hadoop"
HOMEBREW_DIR="/home/linuxbrew/.linuxbrew"
HDFS_PREFIX="hdfs://vm0:9000"
LOCAL_PREFIX="file:/"
MASTER_URL="yarn --deploy-mode client"
DATE=$(date "+%Y-%m-%d-%s")
LOGFILE="/mydata/${USER}-denovo-${DATE}.log"
EVA_JAR=${HOME}"/EVA/lib/eva-denovo_2.12-0.1.jar"
DEFAULT_REFERENCE="hs38"
EVA_HOME=${HOME}"/EVA"
BWA_HOME="/mydata/bwa"
FREEBAYES_HOME="/mydata/freebayes"

if [[ $# -lt 5 ]]; then
    echo "Usage: run_variant_analysis.sh <file1> <file2> <num_nodes> <batch_size> <num_partitions> [reference]"
    echo ""
    echo "Required arguments:"
    echo "<file1> - file containing sample IDs (e.g., SRR077487), one per line"
    echo "<file2> or NONE - file containing URLs of FASTQ files to download (one per line)"
    echo "                  NONE means don't download any FASTQ files"
    echo "<num_nodes> - number of cluster nodes"
    echo "<batch_size> - batch size for outstanding futures (if <=0, run one sequence at-a-time)"
    echo "<num_partitions> - number of partitions (of sequences IDs) to create"
    echo ""
    echo "Optional arguments: "
    echo "[reference] - reference genome [default: hs38]"
    exit
elif [[ $# -eq 5 ]]; then
    REF_GENOME=${DEFAULT_REFERENCE}
else
    REF_GENOME=${6}
fi

let NUM_EXECUTORS=${3}-1

COMMAND="W"

# Delete *.ifq, *.bam*, *.vcf* files
$HADOOP_HOME/bin/hdfs dfs -rm -r /*.ifq /*.bam* /*.vcf*

# network timeout for shuffle
TIMEOUT="420s"

# max attempts
MAX_ATTEMPTS=3

# Spark conf
SPARK_CONF="--conf spark.yarn.appMasterEnv.CANNOLI_HOME=${CANNOLI_HOME_DIR}
        --conf spark.yarn.appMasterEnv.ADAM_HOME=${ADAM_HOME_DIR}
        --conf spark.yarn.appMasterEnv.SPARK_HOME=${SPARK_HOME_DIR}
        --conf spark.yarn.appMasterEnv.HADOOP_HOME=${HADOOP_HOME_DIR}
        --conf spark.yarn.appMasterEnv.HOMEBREW_PREFIX=${HOMEBREW_DIR}
        --conf spark.yarn.appMasterEnv.EVA_HOME=${EVA_HOME}
        --conf spark.yarn.appMasterEnv.BWA_HOME=${BWA_HOME}
        --conf spark.yarn.appMasterEnv.FREEBAYES_HOME=${FREEBAYES_HOME}
        --conf spark.executorEnv.CANNOLI_HOME=${CANNOLI_HOME_DIR}
        --conf spark.executorEnv.ADAM_HOME=${ADAM_HOME_DIR}
        --conf spark.executorEnv.SPARK_HOME=${SPARK_HOME_DIR}
        --conf spark.executorEnv.HADOOP_HOME=${HADOOP_HOME_DIR}
        --conf spark.executorEnv.HOMEBREW_PREFIX=${HOMEBREW_DIR}
        --conf spark.executorEnv.EVA_HOME=${EVA_HOME}
        --conf spark.executorEnv.BWA_HOME=${BWA_HOME}
        --conf spark.executorEnv.FREEBAYES_HOME=${FREEBAYES_HOME}
        --conf spark.network.timeout=${TIMEOUT}
        --conf spark.yarn.maxAppAttempts=${MAX_ATTEMPTS} "

echo ${SPARK_CONF}

if [[ $4 -gt 0 ]]; then
    $SPARK_HOME/bin/spark-submit --master ${MASTER_URL} --num-executors ${NUM_EXECUTORS} \
        ${SPARK_CONF} \
        ${EVA_JAR} -i ${LOCAL_PREFIX}/${1} -d ${LOCAL_PREFIX}/${2} -c ${COMMAND} -r ${REF_GENOME} -n ${3} -b ${4} -p ${5} &> ${LOGFILE} &
else
    $SPARK_HOME/bin/spark-submit --master ${MASTER_URL} --num-executors ${NUM_EXECUTORS} \
        ${SPARK_CONF} \
        ${EVA_JAR} -i ${LOCAL_PREFIX}/${1} -d ${LOCAL_PREFIX}/${2} -c ${COMMAND} -r ${REF_GENOME} -n ${3} -b ${4} -p ${5} -s &> ${LOGFILE} &
fi

echo "See log file for progress: "${LOGFILE}
