# AVAH

## Environment
Spark 3.0.0, Hadoop 3.2.0, Scala 2.12.8

Hadoop 3+ must use `etc/hadoop/workers` to list the data nodes; check using `hdfs dfsadmin -report`

## Setup and execution of variant analysis using AVAH

The instructions are [here](https://github.com/MU-Data-Science/EVA#running-variant-analysis-on-a-cluster-of-cloudlab-nodes-using-avah).

## Rebuilding the JAR if needed

This is a Scala project. You can use `sbt` to `compile` and `package` the project. The JAR file should be copied manually to `lib/` before executing AVAH.

If you wish to change the `scalaVersion` in build.sbt, run `reload` before rebuilding the JAR.

<!--
1. First create a cluster on CloudLab using `EVA-multi-node-profile`.
See instructions [here](https://github.com/MU-Data-Science/EVA/tree/master/cluster_config).

2. Run the following commands on `vm0`.

```
$ git clone https://github.com/MU-Data-Science/EVA.git
$ cd EVA/cluster_config
$ ./cluster_configure.sh <num_nodes> spark3
```
If the cluster size is large (e.g., 16+ nodes), use the `screen` command first and then run `cluster_configure.sh`.

3. Make sure the reference sequence files (`hs38.*`) are copied to each cluster node on `/mydata`.

4. On `vm0`, do the following:

```
$ git clone https://github.com/raopr/AVAH.git
$ cp AVAH/misc/sample*-vlarge.txt /proj/eva-public-PG0/
```

When YARN runs the job, it will needs these files on all the cluster nodes.

5. Make sure known SNPs and known INDELs folders are on HDFS. Otherwise, use `EVA/scripts/convert_known_snps_indels_to_adam.sh`.

6. Now run the variant analysis.

```
$ ${HOME}/AVAH/scripts/run_variant_analysis_at_scale.sh -i /proj/eva-public-PG0/sampleIDs-vlarge.txt -d /proj/eva-public-PG0/sampleURLs-vlarge.txt -n 16 -b 2 -p 15 -P D
```

6. If you want to run variant analysis again but don't want to re-download the sequences, use `NONE` as shown below:
```
$ ${HOME}/AVAH/scripts/run_variant_analysis.sh -i /proj/eva-public-PG0/sampleIDs-vlarge.txt -d /proj/eva-public-PG0/sampleURLs-vlarge.txt -n 16 -b 2 -p 15 -P D
```




## How to run the JAR directly if needed

```
$SPARK_HOME/bin/spark-submit --master yarn --deploy-mode cluster --num-executors 3 avah_2.12-0.1.jar -i hdfs://vm0:9000/sampleIDs.txt -d hdfs://vm0:9000/sampleURLs.txt
```
OR
```
$SPARK_HOME/bin/spark-submit --master yarn --deploy-mode client --num-executors 3 avah_2.12-0.1.jar -i hdfs://vm0:9000/sampleIDs.txt -d hdfs://vm0:9000/sampleURLs.txt
```
OR
```
$SPARK_HOME/bin/spark-submit --master yarn --deploy-mode client --num-executors 3 --conf spark.yarn.appMasterEnv.CANNOLI_HOME=/mydata/cannoli --conf spark.yarn.appMasterEnv.SPARK_HOME=/mydata/spark --conf spark.executorEnv.CANNOLI_HOME=/mydata/cannoli --conf spark.executorEnv.SPARK_HOME=/mydata/spark avah_2.12-0.1.jar -i hdfs://vm0:9000/sampleIDs.txt
```
-->

## Useful YARN commands

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

```
dstat --cpu --mem --load --top-cpu --top-mem -dn --output report.csv 2 10
```

or

```
dstat --cpu --mem --load --top-cpu --top-mem -dn --noupdate --output report.csv 2 10
```

