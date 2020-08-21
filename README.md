# EVA-denovo

# How to run the jar

```
$SPARK_HOME/bin/spark-submit --master yarn --deploy-mode cluster
--num-executors 3 eva_2.12-0.1.jar -i hdfs://vm0:9000/sampleIDs.txt
```
