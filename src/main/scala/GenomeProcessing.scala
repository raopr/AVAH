/**
 * University of Missouri-Columbia
 * 2020
 */

import org.apache.spark.sql.SparkSession

import scala.math.{max, min}
import org.apache.log4j.Logger
import org.apache.spark.HashPartitioner
import org.apache.spark.RangePartitioner

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import sys.process._
import scala.concurrent.ExecutionContext.Implicits.global
import GenomeTasks._
import ConcurrentContext._
import scala.util.control.Breaks._

object GenomeProcessing {
  def usage(): Unit = {
    println("""
    Usage: spark-submit [Spark options] eva_some_version.jar [jar options]

    Spark options: --master, --num-executors, etc.

    Required jar options:
      -i | --input <file>         input file containing sample IDs; one per line
      -c | --command <D|W|R|E>    D: denovo sequence generation;
                                  W: variant analysis on whole genome sequences;
                                  R: variant analysis on RNA-seq sequences;
                                  E: variant analysis on whole exome sequences
    Optional jar options:
      -d | --download <filename>  input file containing URLs of FASTQ files to download (one per line)
                                  if filename is 'file://NONE', then no FASTQ files are downloaded
      -k | --kmer <INT>           k-mer length [default: 51]
      -b | --batch <INT>          minimum batch size for outstanding futures [default: 3]
      -p | --partitions <INT>     number of partitions (of sequence IDs) to create [default: 2]
      -n | --numnodes <INT>       size of cluster [default: 2]
      -r | --reference <name>     reference genome [default: hs38]
      -P | --partitioner <R|H|D|S>  [R]ange or [H]ash or [D]efault or [S]orted default [default: D]
      -f                          use fork-join approach
      -s                          naive, one sequence at-a-time
    """)
  }

  def main(args: Array[String]): Unit = {

    if (args.length < 1) {
      usage()
      sys.exit(2)
    }

    val argList = args.toList
    type OptionMap = Map[Symbol, Any]

    def nextOption(map: OptionMap, list: List[String]): OptionMap = {
      list match {
        case Nil => map
        case ("-h" | "--help") :: tail => usage(); sys.exit(0)
        case ("-i" | "--input") :: value :: tail => nextOption(map ++ Map('input -> value), tail)
        case ("-c" | "--command") :: value :: tail => nextOption(map ++ Map('command -> value), tail)
        case ("-d" | "--download") :: value :: tail => nextOption(map ++ Map('download -> value), tail)
        case ("-k" | "--kmer") :: value :: tail => nextOption(map ++ Map('kmer -> value), tail)
        case ("-b" | "--batch") :: value :: tail => nextOption(map ++ Map('batch -> value), tail)
        case ("-n" | "--numnodes") :: value :: tail => nextOption(map ++ Map('numnodes -> value), tail)
        case ("-p" | "--partitions") :: value :: tail => nextOption(map ++ Map('numpartitions -> value), tail)
        case ("-r" | "--reference") :: value :: tail => nextOption(map ++ Map('reference -> value), tail)
        case ("-P" | "--partitioner") :: value :: tail => nextOption(map ++ Map('partitioner -> value), tail)
        case ("-s") :: tail => nextOption(map ++ Map('single -> true), tail)
        case ("-f") :: tail => nextOption(map ++ Map('forkjoin -> true), tail)
        case value :: tail => println("Unknown option: " + value)
          usage()
          sys.exit(1)
      }
    }

    val options = nextOption(Map(), argList)

    val spark = SparkSession.builder.appName("Large-scale genome processing").getOrCreate()
    spark.sparkContext.setLogLevel("INFO")
    val log = Logger.getLogger(getClass.getName)
    log.info("\uD83D\uDC49 Starting the generation")
    val numExecutors = spark.conf.get("spark.executor.instances").toInt
    val sampleFileName = options('input)
    val downloadFileName = options.getOrElse('download, null)
    val kmerVal = options.getOrElse('kmer, 51)
    val minBatchSize = options.getOrElse('batch, 3).toString.toInt
    val commandToExecute = options.getOrElse('command, null)
    val numNodes = options.getOrElse('numnodes, 2).toString.toInt
    val numPartitions = options.getOrElse('numpartitions, 2).toString.toInt
    val referenceGenome = options.getOrElse('reference, "hs38").toString
    val singleMode = options.getOrElse('single, false)
    val forkjoinMode = options.getOrElse('forkjoin, false)
    val partitioner = options.getOrElse('partitioner, "D")

    println("Reference genome: ", referenceGenome)
    println("Num. nodes: ", numNodes)
    println("Num. partitions: ", numPartitions)
    println("Batch size: ", minBatchSize)
    println("Partitioner: ", partitioner)
    println("Fork-join approach: ", forkjoinMode)

    if (commandToExecute == null) {
      println("Option -c | --command is required.")
      usage()
      sys.exit(1)
    }

    val FILE_NONE = "file://NONE"
    if (downloadFileName != null && downloadFileName.toString() != FILE_NONE) {
      println(s"Starting to download FASTQ files in ${downloadFileName.toString}...")

      val downloadList =
        spark.sparkContext.textFile(downloadFileName.toString).repartition(numExecutors)

      val partitionCounts = downloadList.glom.map(_.length).collect()
      println("==== No. of files in each partition to download ====")
      for (x <- partitionCounts) {
        println(" [", x, "] ")
      }
      println("Min: ", partitionCounts.min, " Max: ", partitionCounts.max, " Avg: ",
        partitionCounts.sum / partitionCounts.length)
      val maxDownloadTasks = partitionCounts.sum / partitionCounts.length

      // first download files using curl and store in HDFS
      downloadList
        .map(x => executeAsync(runDownload(x)))
        .mapPartitions(it => awaitSliding(it, batchSize = max(maxDownloadTasks, minBatchSize)))
        .collect()
        .foreach(x => println(s"Finished downloading $x"))

      println("Completed all downloads")
    }
    else {
      println(s"FASTQ files in ${downloadFileName.toString} are assumed to be in HDFS")
    }

    val sampleIDList = spark.sparkContext.textFile(sampleFileName.toString).repartition(numPartitions)
    val itemCounts = sampleIDList.glom.map(_.length).collect()
    println("==== No. of sample IDs in each partition ====")
    for (x <- itemCounts) {
      println(" [", x, "] ")
    }
    println("Min: ", itemCounts.min, " Max: ", itemCounts.max, " Avg: ",
      itemCounts.sum / itemCounts.length)
    val maxTasks = itemCounts.sum / itemCounts.length

    //val pairList = sampleIDList.map(x => (x, 10)).partitionBy(
    //  new HashPartitioner(numExecutors))

    def splitLine(x: String): (Long, String) = {
      val arr = x.split(" ")
      return (arr(0).toLong, arr(1))
    }

    val sizeNameList = sampleIDList.map(x => splitLine(x))

    val sortedSampleIDList = {
      partitioner match {
        case "D" => sizeNameList // default partitioning
        case "S" => sizeNameList.mapPartitions( // with sorting
          iterator => {
            val myList = iterator.toArray
            myList.sortWith(_._1 < _._1).iterator
          }
        )
        case "R" => sizeNameList.repartitionAndSortWithinPartitions(new RangePartitioner(numPartitions, sizeNameList))
        case "H" => sizeNameList.repartitionAndSortWithinPartitions(new HashPartitioner(numPartitions))
        case _ => println("Unknown option"); sys.exit(1)
      }
    }

    // Print the partitions
    println("Partitions:")
    val output = sortedSampleIDList.glom.collect()
    for (i <- 0 to output.length-1) {
      for (j <- 0 to output(i).length-1) {
        print(output(i)(j))
      }
      println("\n")
    }

    var processingCompleted = false

    commandToExecute.toString() match {
      case "D" =>
        if (singleMode==false) { // parallel
          sortedSampleIDList
            .map(s => executeAsync(runInterleave(s._2)))
            .mapPartitions(it => await(it, batchSize = min(maxTasks, minBatchSize)))
            .map(x => executeAsync(runDenovo(x._1, kmerVal.toString.toInt)))
            .mapPartitions(it => await(it, batchSize = min(maxTasks, minBatchSize)))
            .collect()
            .foreach(x => println(s"Finished interleaved FASTQ and de novo assembly of $x"))
        }
        else {
          sortedSampleIDList.repartition(1)
            .map(s => executeAsync(runInterleave(s._2)))
            .mapPartitions(it => await(it, batchSize = 1))
            .map(x => executeAsync(runDenovo(x, kmerVal.toString.toInt)))
            .mapPartitions(it => await(it, batchSize = 1))
            .collect()
            .foreach(x => println(s"Finished interleaved FASTQ and de novo assembly of $x"))
        }

      case "W" =>
        if (singleMode==false && forkjoinMode==false) { // parallel

          // Future that checks load average and decides to rerun sequences
          def earlyRetry() : Array[(String, Int)] = {
            val initialSleepTime = 1000 * 60 * 60 * 12 // 12 hrs in milliseconds
            Thread.sleep(initialSleepTime)

            var yarnOutput = ""
            var capacityValue = "100.0%"

            while (true) {
              try {
                yarnOutput = Seq("yarn", "queue", "-status", "default").!!
                val pattern = "Current Capacity : [0-9]+.[0-9]+%".r
                capacityValue = pattern.findFirstIn(yarnOutput).getOrElse("100.0%").toString.split(' ').last
              }
              catch {
                case e: Exception => print(s"Exception in YARN load check")
              }

              val loadThreshold = 75.0
              if (capacityValue.dropRight(1).toFloat < loadThreshold) {
                print("Load less than threshold")
                val retryExt = ".retry"
                var retryFiles = ""
                try {
                  retryFiles = Seq("hdfs", "dfs", "-ls", s"/*$retryExt").!!
                  val retryPattern = s"/[A-Z]+[0-9]+$retryExt".r
                  val retrySequenceList =
                    spark.sparkContext.parallelize(retryPattern.findAllIn(retryFiles).toArray
                      .map(x => x.drop(1).dropRight(retryExt.length())))

                  val res = retrySequenceList
                    .map(x => executeAsync(cleanupFiles(x)))
                    .mapPartitions(it => await(it, batchSize = minBatchSize))
                    .map(x => executeAsync(runInterleave(x._1)))
                    .mapPartitions(it => await(it, batchSize = minBatchSize))
                    .map(x => executeAsync(runBWA(x._1, referenceGenome)))
                    .mapPartitions(it => await(it, batchSize = minBatchSize))
                    .map(x => executeAsync(runSortMarkDup(x._1)))
                    .mapPartitions(it => await(it, batchSize = minBatchSize))
                    .map(x => executeAsync(runFreebayes(x._1, referenceGenome)))
                    .mapPartitions(it => await(it, batchSize = minBatchSize))
                    .collect()

                  return res
                }
                catch {
                  case e: Exception => print(s"Exception in HDFS listing of .retry files")
                }
              }

              val sleepTime = 1000 * 10 * 60 // 10 mins in milliseconds
              Thread.sleep(sleepTime)
              if (processingCompleted == true) {
                //break
                return Array[(String, Int)](("NONE", 1))
              }
            }
            return Array[(String,Int)](("NONE", 1))
          }

          // Asynchronously execute early retries
          val earlyRetryFuture = executeAsync(earlyRetry)

          val finalRes = sortedSampleIDList
            .map(s => executeAsync(runInterleave(s._2)))
            .mapPartitions(it => await(it, batchSize = min(maxTasks, minBatchSize)))
            .map(x => executeAsync(runBWA(x._1, referenceGenome)))
            .mapPartitions(it => await(it, batchSize = min(maxTasks, minBatchSize)))
            .map(x => executeAsync(runSortMarkDup(x._1)))
            .mapPartitions(it => await(it, batchSize = min(maxTasks, minBatchSize)))
            .map(x => executeAsync(runFreebayes(x._1, referenceGenome)))
            .mapPartitions(it => await(it, batchSize = min(maxTasks, minBatchSize)))
            .collect()

          val successfulRes = finalRes.filter(x => x._2 == 0)

          successfulRes.foreach(x => println(s"👉 Finished basic variant analysis of whole genome sequence $x"))

          val failedRes = finalRes.filter(x => x._2 > 0)

          failedRes.foreach(x => println(s"😡 Failed to process whole genome sequence $x"))

          // indicate to earlyRetryFuture that the main thread has completed processing
          processingCompleted = true

          // Await for early retry to finish
          Await.result(earlyRetryFuture, Duration.Inf)

          println("Completed earlyRetryFuture")

          earlyRetryFuture.foreach(x => println(s"👉 Early retry completed for whole genome sequence $x"))

//          var retryBatchSize = minBatchSize
//          var retryNumPartitions = numPartitions
//          if (failedRes.length < numPartitions * minBatchSize) {
//            retryBatchSize = failedRes.length
//            retryNumPartitions = 1
//          }
//
//          val retrySampleIDList = spark.sparkContext.parallelize(failedRes, retryNumPartitions)
//
//          // Print the partitions
//          println("RETRY partitions:")
//
//          val output = retrySampleIDList.glom.collect()
//          for (i <- 0 to output.length-1) {
//            for (j <- 0 to output(i).length-1) {
//              print(output(i)(j))
//            }
//            println("\n")
//          }
//
//          val retryRes = retrySampleIDList
//            .map(x => executeAsync(cleanupFiles(x._1)))
//            .mapPartitions(it => await(it, batchSize = retryBatchSize))
//            .map(x => executeAsync(runInterleave(x._1)))
//            .mapPartitions(it => await(it, batchSize = retryBatchSize))
//            .map(x => executeAsync(runBWA(x._1, referenceGenome)))
//            .mapPartitions(it => await(it, batchSize = retryBatchSize))
//            .map(x => executeAsync(runSortMarkDup(x._1)))
//            .mapPartitions(it => await(it, batchSize = retryBatchSize))
//            .map(x => executeAsync(runFreebayes(x._1, referenceGenome)))
//            .mapPartitions(it => await(it, batchSize = retryBatchSize))
//            .collect()
//
//          val successfulRetryRes = retryRes.filter(x => x._2 == 0)
//          successfulRetryRes
//            .foreach(x => println(s"👉 RETRY: Finished basic variant analysis of whole genome sequence $x"))
        }
        else if (singleMode==false && forkjoinMode==true) {
          val interleaveRes = sortedSampleIDList
            .map(s => executeAsync(runInterleave(s._2)))
            .mapPartitions(it => await(it, batchSize = min(maxTasks, minBatchSize)))

          val joinInterleave = interleaveRes.collect()

          val bwaRes = interleaveRes
            .map(x => executeAsync(runBWA(x._1, referenceGenome)))
            .mapPartitions(it => await(it, batchSize = min(maxTasks, minBatchSize)))

          val joinBWA = bwaRes.collect()

          val sortDupRes = bwaRes
            .map(x => executeAsync(runSortMarkDup(x._1)))
            .mapPartitions(it => await(it, batchSize = min(maxTasks, minBatchSize)))

          val joinSortDup = sortDupRes.collect()

          val freeBayesRes = sortDupRes
            .map(x => executeAsync(runFreebayes(x._1, referenceGenome)))
            .mapPartitions(it => await(it, batchSize = min(maxTasks, minBatchSize)))

          val joinFreebayes = freeBayesRes.collect()

          joinFreebayes
            .foreach(x => println(s"Finished basic variant analysis of whole genome sequence $x"))
        }
        else {
          sortedSampleIDList.repartition(1)
            .map(s => executeAsync(runInterleave(s._2)))
            .mapPartitions(it => await(it, batchSize = 1))
            .map(x => executeAsync(runBWA(x._1, referenceGenome)))
            .mapPartitions(it => await(it, batchSize = 1))
            .map(x => executeAsync(runSortMarkDup(x._1)))
            .mapPartitions(it => await(it, batchSize = 1))
            .map(x => executeAsync(runFreebayes(x._1, referenceGenome)))
            .mapPartitions(it => await(it, batchSize = 1))
            .collect()
            .foreach(x => println(s"Finished basic variant analysis of whole genome sequence $x"))
        }

      case "R" => println("Option is still under development")

      case "E" => println("Option is still under development")

      case _ => println("Invalid command"); usage()
    }

    log.info("\uD83D\uDC49 Completed the genome processing successfully.")
    spark.stop()
  }
}