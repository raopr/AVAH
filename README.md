# EVA-denovo

## Environment
Spark 3.0.0, Hadoop 3.2.0, Scala 2.12.8

Hadoop 3+ must use `etc/hadoop/workers` to list the data nodes; always check using `hdfs dfsadmin -report`

## Setup

1. After creating a cluster on CloudLab

`cd EVA/cluster_config; ./cluster_config <num_nodes> spark3`

## How to run the JAR

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