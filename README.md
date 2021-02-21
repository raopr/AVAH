# EVA-denovo

# Environment
Spark 3.0.0, Hadoop 3.2.0, Scala 2.12.8

Hadoop 3+ must use `etc/hadoop/workers` to list the data nodes; always check using `hdfs dfsadmin -report`

# Setup and variant analysis execution using futures

1. First create a cluster on CloudLab using `EVA-multi-node-profile`.

2. Run the following commands on `vm0`.

```
$ git clone https://github.com/MU-Data-Science/EVA.git
$ cd EVA/cluster_config; ./cluster_config <num_nodes> spark3
```

3. Make sure the reference sequence files (`hs38.*`) are copied to each cluster node on `/mydata`.

4. On `vm0`, do the following:

```
$ git clone https://github.com/raopr/EVA-denovo.git
$ cp EVA-denovo/misc/sample*-vlarge.txt /proj/eva-public-PG0/
```

When YARN runs the job, it will needs these files on all the cluster nodes.

5. Now run the variant analysis.

```
$ ${HOME}/EVA-denovo/scripts/run_variant_analysis.sh /proj/eva-public-PG0/sampleIDs-vlarge.txt /proj/eva-public-PG0/sampleURLs-vlarge.txt <num_cluster_nodes> <futures_batch_size>
```

Use an integer for `futures_batch_size`. If `<= 0`, then we will process one sequence at-a-time like in the EVA/scripts.

6. If you want to run variant analysis again but don't want to re-download the sequences, use `NONE` as shown below:
```
$ ${HOME}/EVA-denovo/scripts/run_variant_analysis.sh /proj/eva-public-PG0/sampleIDs-vlarge.txt NONE <num_cluster_nodes> <futures_batch_size>
```




## How to run the JAR directly if needed

```
$SPARK_HOME/bin/spark-submit --master yarn --deploy-mode cluster --num-executors 3 eva-denovo_2.12-0.1.jar -i hdfs://vm0:9000/sampleIDs.txt -d hdfs://vm0:9000/sampleURLs.txt
```
OR
```
$SPARK_HOME/bin/spark-submit --master yarn --deploy-mode client --num-executors 3 eva-denovo_2.12-0.1.jar -i hdfs://vm0:9000/sampleIDs.txt -d hdfs://vm0:9000/sampleURLs.txt
```
OR
```
$SPARK_HOME/bin/spark-submit --master yarn --deploy-mode client --num-executors 3 --conf spark.yarn.appMasterEnv.CANNOLI_HOME=/mydata/cannoli --conf spark.yarn.appMasterEnv.SPARK_HOME=/mydata/spark --conf spark.executorEnv.CANNOLI_HOME=/mydata/cannoli --conf spark.executorEnv.SPARK_HOME=/mydata/spark eva-denovo_2.12-0.1.jar -i hdfs://vm0:9000/sampleIDs.txt
```

# Useful YARN commands

To check YARN jobs:

```
yarn application -list
```

To kill YARN jobs:

```
yarn application -kill <application_ID>
```

To see YARN queues:

```
mapred queue -list
```

To change YARN's scheduler configuration via command line

```
yarn schedulerconf
```

Examples:

```
yarn schedulerconf -global yarn.scheduler.maximum-allocation-mb=16384
```

```
yarn schedulerconf -global yarn.scheduler.maximum-allocation-vcores=32
```

```
yarn schedulerconf -global yarn.scheduler.maximum-allocation-mb=16384,yarn.scheduler.maximum-allocation-vcores=32
```

## To check status

```
yarn queue -status default
```

## To view YARN logs

```
yarn logs -applicationId <application_ID>
```

## To view cluster usage

```
yarn top
```

```
yarn node -all -list
```

```
yarn node -showDetails -list
```

## Monitoring process execution

`dstat --cpu --mem --load --top-cpu --top-mem -dn --output report.csv 2 10`