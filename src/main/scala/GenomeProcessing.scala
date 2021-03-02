/**
 * University of Missouri-Columbia
 * 2020
 */

import org.apache.spark.sql.SparkSession
import scala.math.{max, min}

import org.apache.log4j.Logger
import org.apache.spark.HashPartitioner
import org.apache.spark.RangePartitioner

import GenomeTasks._
import ConcurrentContext._

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

    commandToExecute.toString() match {
      case "D" =>
        if (singleMode==false) { // parallel
          sortedSampleIDList
            .map(s => executeAsync(runInterleave(s._2)))
            .mapPartitions(it => await(it, batchSize = min(maxTasks, minBatchSize)))
            .map(x => executeAsync(runDenovo(x, kmerVal.toString.toInt)))
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
      case "E" =>
        if (singleMode==false) { // parallel
          sortedSampleIDList
            .map(x => executeAsync(runDenovo(x, kmerVal.toString.toInt)))
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
          sortedSampleIDList
            .map(s => executeAsync(runInterleave(s._2)))
            .mapPartitions(it => await(it, batchSize = min(maxTasks, minBatchSize)))
            .map(x => executeAsync(runBWA(x, referenceGenome)))
            .mapPartitions(it => await(it, batchSize = min(maxTasks, minBatchSize)))
            .map(x => executeAsync(runSortMarkDup(x)))
            .mapPartitions(it => await(it, batchSize = min(maxTasks, minBatchSize)))
            .map(x => executeAsync(runFreebayes(x, referenceGenome)))
            .mapPartitions(it => await(it, batchSize = min(maxTasks, minBatchSize)))
            .collect()
            .foreach(x => println(s"Finished basic variant analysis of whole genome sequence $x"))
        }
        else if (singleMode==false && forkjoinMode==true) {
          val interleaveRes = sortedSampleIDList
            .map(s => executeAsync(runInterleave(s._2)))
            .mapPartitions(it => await(it, batchSize = min(maxTasks, minBatchSize)))

          val joinInterleave = interleaveRes.collect()

          val bwaRes = interleaveRes
            .map(x => executeAsync(runBWA(x, referenceGenome)))
            .mapPartitions(it => await(it, batchSize = min(maxTasks, minBatchSize)))

          val joinBWA = bwaRes.collect()

          val sortDupRes = bwaRes
            .map(x => executeAsync(runSortMarkDup(x)))
            .mapPartitions(it => await(it, batchSize = min(maxTasks, minBatchSize)))

          val joinSortDup = sortDupRes.collect()

          val freeBayesRes = sortDupRes
            .map(x => executeAsync(runFreebayes(x, referenceGenome)))
            .mapPartitions(it => await(it, batchSize = min(maxTasks, minBatchSize)))

          val joinFreebayes = freeBayesRes.collect()

          joinFreebayes
            .foreach(x => println(s"Finished basic variant analysis of whole genome sequence $x"))
        }
        else {
          sortedSampleIDList.repartition(1)
            .map(s => executeAsync(runInterleave(s._2)))
            .mapPartitions(it => await(it, batchSize = 1))
            .map(x => executeAsync(runBWA(x, referenceGenome)))
            .mapPartitions(it => await(it, batchSize = 1))
            .map(x => executeAsync(runSortMarkDup(x)))
            .mapPartitions(it => await(it, batchSize = 1))
            .map(x => executeAsync(runFreebayes(x, referenceGenome)))
            .mapPartitions(it => await(it, batchSize = 1))
            .collect()
            .foreach(x => println(s"Finished basic variant analysis of whole genome sequence $x"))
        }
      case "R" =>
        if (singleMode==false) { // parallel
          sortedSampleIDList
            .map(s => executeAsync(runInterleave(s._2)))
            .mapPartitions(it => await(it, batchSize = min(maxTasks, minBatchSize)))
            .map(x => executeAsync(runDenovo(x, kmerVal.toString.toInt)))
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
      case _ => println("Invalid command"); usage()
    }

    log.info("\uD83D\uDC49 Completed the genome processing successfully.")
    spark.stop()
  }
}