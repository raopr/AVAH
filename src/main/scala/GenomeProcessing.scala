/**
 * University of Missouri-Columbia
 * 2020
 */

import sys.process._
import scala.sys.process
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import org.apache.spark.sql.SparkSession
import scala.math.{max, min}

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer
import org.apache.log4j.Logger
import java.util.Calendar

import org.apache.spark.HashPartitioner

// Futures code is taken from http://www.russellspitzer.com/2017/02/27/Concurrency-In-Spark/
object ConcurrentContext {
  import scala.util._
  import scala.concurrent._
  import scala.concurrent.ExecutionContext.Implicits.global
  import scala.concurrent.duration.Duration
  import scala.concurrent.duration.Duration._
  /** Wraps a code block in a Future and returns the future */
  def executeAsync[T](f: => T): Future[T] = {
    Future(f)
  }

  /** Awaits only a set of elements at a time. At most batchSize futures will ever
   * be in memory at a time*/
  def awaitBatch[T](it: Iterator[Future[T]], batchSize: Int = 3, timeout: Duration = Inf) = {
    it.grouped(batchSize)
      .map(batch => Future.sequence(batch))
      .flatMap(futureBatch => Await.result(futureBatch, timeout))
  }

  def awaitSliding[T](it: Iterator[Future[T]], batchSize: Int = 3, timeout: Duration = Inf): Iterator[T] = {
    //val slidingIterator = it.sliding(batchSize - 1).withPartial(true) //Our look ahead (hasNext) will auto start the nth future in the batch
    val slidingIterator = it.sliding(batchSize - 1) //Our look ahead (hasNext) will auto start the nth future in the batch
    val (initIterator, tailIterator) = slidingIterator.span(_ => slidingIterator.hasNext)
    initIterator.map( futureBatch => Await.result(futureBatch.head, timeout)) ++
      tailIterator.flatMap( lastBatch => Await.result(Future.sequence(lastBatch), timeout))
  }

  // Switch between sliding window vs batch
  def await[T](it: Iterator[Future[T]], batchSize: Int = 3, timeout: Duration = Inf) = {
    val ONE = 1
    if (batchSize.equals(ONE)) {
      awaitBatch(it, batchSize)
    }
    else {
      awaitSliding(it, batchSize)
    }
  }
}

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
      -n | --numnodes <INT>       size of cluster [default: 16]
      -r | --reference <name>     reference genome [default: hs38]
      -s                          naive, one sequence at-a-time
    """)
  }

  // Download
  def runDownload[T](x: T):T = {
    val beginTime = Calendar.getInstance().getTime()
    println(s"Starting to download $x at $beginTime")
    val outputFileName = x.toString.split("/").last
    println("Output filename: ", outputFileName)
    val hdfsCmd = sys.env("HADOOP_HOME") + "/bin/hdfs"
    val ret = (Seq("curl", "-sS", s"$x") #| Seq(s"$hdfsCmd", "dfs", "-put", "-", s"/$outputFileName")).!
    val curlCmd =
      s"curl -sS $x | " + sys.env("HADOOP_HOME") + s"/bin/hdfs dfs -put - /$outputFileName "
    val endTime = Calendar.getInstance().getTime()
    println(s"Completed download command: $curlCmd $ret at $endTime")
    x
  }

  // Interleave FASTQ
  def runInterleave[T](x: T):T = {
    val beginTime = Calendar.getInstance().getTime()
    println(s"Starting Interleave FASTQ on ($x) at $beginTime")
    val sampleID = x.toString

    // Create interleaved fastq files
    val cannoliSubmit = sys.env("CANNOLI_HOME") + "/exec/cannoli-submit"
    //val sparkMaster = "spark://vm0:7077"
    val hdfsPrefix = "hdfs://vm0:9000"
    val retInterleave = Seq(s"$cannoliSubmit", "--master", "yarn", "--", "interleaveFastq",
                            s"$hdfsPrefix/${sampleID}_1.fastq.gz",
                            s"$hdfsPrefix/${sampleID}_2.fastq.gz",
                            s"$hdfsPrefix/${sampleID}.ifq").!
    val endTime = Calendar.getInstance().getTime()
    println(s"Completed interleave FASTQ on ($x) at ${endTime}, return values: $retInterleave")
    x
  }

  // Variant analysis
  def runVariantAnalysis[T](x: T, referenceGenome: String, numNodes: Int):T = {
    val beginTime = Calendar.getInstance().getTime()
    println(s"Starting variant analysis on ($x) at $beginTime")
    val sampleID = x.toString

    val VASubmit = sys.env("EVA_HOME") + "/scripts/run_variant_analysis_adam_basic.sh"
    //val sparkMaster = "spark://vm0:7077"

    val useYARN = "y"
    val hdfsPrefix = "hdfs://vm0:9000"
    val retVA = Seq(s"$VASubmit", s"$referenceGenome",
      s"$hdfsPrefix/${sampleID}_1.fastq.gz",
      s"$hdfsPrefix/${sampleID}_2.fastq.gz",
      s"$numNodes",
      s"$sampleID",
      s"$useYARN").!

    val endTime = Calendar.getInstance().getTime()
    println(s"Completed variant analysis on ($x) ended at $endTime; return values $retVA")

    x
  }

  // BWA
  def runBWA[T](x: T, referenceGenome: String):T = {
    val beginTime = Calendar.getInstance().getTime()
    println(s"Starting BWA on ($x) at $beginTime")
    val sampleID = x.toString

    val cannoliSubmit = sys.env("CANNOLI_HOME") + "/exec/cannoli-submit"
    //val sparkMaster = "spark://vm0:7077"
    val hdfsPrefix = "hdfs://vm0:9000"
    val bwaCmd = sys.env("BWA_HOME") + "/bwa"
    val retBWA = Seq(s"$cannoliSubmit", "--master", "yarn", "--", "bwaMem",
      s"$hdfsPrefix/${sampleID}.ifq",
      s"$hdfsPrefix/${sampleID}.bam",
      "-executable",
      s"$bwaCmd",
      "-sample_id",
      "mysample",
      "-index",
      s"file:///mydata/$referenceGenome.fa",
      "-sequence_dictionary",
      s"file:///mydata/$referenceGenome.dict",
      "-single",
      "-add_files").!

    // Delete $sampleId.bam_* files
    val hdfsCmd = sys.env("HADOOP_HOME") + "/bin/hdfs"
    val retDel = Seq(s"$hdfsCmd", "dfs", "-rm", "-r", s"/${sampleID}.bam_*").!
    val endTime = Calendar.getInstance().getTime()
    println(s"Completed BWA on ($x) ended at $endTime; return values bwa: $retBWA, delete: $retDel")

    x
  }

  // Sort and Mark Duplicates
  def runSortMarkDup[T](x: T):T = {
    val beginTime = Calendar.getInstance().getTime()
    println(s"Starting sort/mark duplicates on ($x) at $beginTime")
    val sampleID = x.toString

    val adamSubmit = sys.env("ADAM_HOME") + "/exec/adam-submit"
    //val sparkMaster = "spark://vm0:7077"
    val hdfsPrefix = "hdfs://vm0:9000"
    val retSortDup = Seq(s"$adamSubmit", "--master", "yarn", "--", "transformAlignments",
      s"$hdfsPrefix/${sampleID}.bam",
      s"$hdfsPrefix/${sampleID}.bam.adam",
      "-mark_duplicate_reads",
      "-sort_by_reference_position_and_index").!

    val endTime = Calendar.getInstance().getTime()
    println(s"Completed sort/mark duplicates on ($x) ended at $endTime; return values $retSortDup")

    x
  }

  // Sort and Mark Duplicates
  def runFreebayes[T](x: T, referenceGenome: String):T = {
    val beginTime = Calendar.getInstance().getTime()
    println(s"Starting Freebayes on ($x) at $beginTime")
    val sampleID = x.toString

    val cannoliSubmit = sys.env("CANNOLI_HOME") + "/exec/cannoli-submit"
    //val sparkMaster = "spark://vm0:7077"
    val hdfsPrefix = "hdfs://vm0:9000"
    val freeBayesCmd = sys.env("FREEBAYES_HOME") + "/bin/freebayes"
    val retFreebayes = Seq(s"$cannoliSubmit", "--master", "yarn", "--", "freebayes",
      s"$hdfsPrefix/${sampleID}.bam.adam",
      s"$hdfsPrefix/${sampleID}.vcf",
      "-executable",
      s"$freeBayesCmd",
      "-reference",
      s"file:///mydata/$referenceGenome.fa",
      "-add_files",
      "-single").!

    val endTime = Calendar.getInstance().getTime()
    println(s"Completed Freebayes on ($x) ended at $endTime; return values $retFreebayes")

    x
  }


  // Denovo assembly
  def runDenovo[T](x: T, kmerVal: Int):T = {
    val beginTime = Calendar.getInstance().getTime()
    println(s"Starting Abyss on ($x) at $beginTime")
    val sampleID = x.toString
    val cleanUp = "rm -rf /mydata/$sampleID*"
    val cleanRet = Process(cleanUp).!

    // First copy the file from HDFS to /mydata
    val dataDir = "/mydata"
    val copyCmd =
      sys.env("HADOOP_HOME") + "/bin/hdfs dfs -get -f " +
        s" /$sampleID.ifq $dataDir"
    val retCopy = Process(copyCmd).!

    println(s"Completed HDFS copy...")

    // Run Abyss; only interleaved FASTQ works with Scala Process call

    val abyssDir = sys.env("HOMEBREW_PREFIX")
    val cmd =
      s"$abyssDir/bin/abyss-pe j=30 k=$kmerVal -C $dataDir " +
      s" name=$sampleID " +
      s" in=$dataDir/$sampleID.ifq"
    println(cmd)
    val abyssRet = Process(cmd).!

    // Although abyss-pe takes two paired-end files, it fails later inside the script
    //val ret = Seq(s"$abyssDir/abyss-pe", "j=30", "k=71", "-C", s"$dataDir",
    //  s"name=$sampleID", s"in='${sampleID}_1.filt.fastq.gz ${sampleID}_2.filt.fastq.gz'").!

    // Copy .fa to HDFS
    val cmdToCopyFa =
      sys.env("HADOOP_HOME") + "/bin/hdfs dfs -put -f " +
        s" $dataDir/$sampleID-scaffolds.fa /"
    println(cmdToCopyFa)
    val facopyRet = Process(cmdToCopyFa).!
    val endTime = Calendar.getInstance().getTime()
    println(s"Abyss ended at ${endTime}, return values: ", cleanRet, retCopy, abyssRet, facopyRet)
    x
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
        case ("-r" | "--reference") :: value :: tail => nextOption(map ++ Map('reference -> value), tail)
        case ("-s") :: tail => nextOption(map ++ Map('single -> true), tail)
        case value :: tail => println("Unknown option: "+value)
          usage()
          sys.exit(1)
      }
    }

    val options = nextOption(Map(),argList)

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
    val numNodes = options.getOrElse('numnodes, 16).toString.toInt
    val referenceGenome = options.getOrElse('reference, "hs38").toString
    val singleMode = options.getOrElse('single, false)

    println("Reference genome: ", referenceGenome)
    println("Num. nodes: ", numNodes)

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
        partitionCounts.sum/partitionCounts.length)
      val maxDownloadTasks = partitionCounts.sum/partitionCounts.length

      // first download files using curl and store in HDFS
      downloadList
        .map(x => ConcurrentContext.executeAsync(runDownload(x)))
        .mapPartitions(it => ConcurrentContext.awaitSliding(it, batchSize = max(maxDownloadTasks, minBatchSize)))
        .collect()
        .foreach(x => println(s"Finished downloading $x"))

      println("Completed all downloads")
    }
    else {
      println(s"FASTQ files in ${downloadFileName.toString} are assumed to be in HDFS")
    }

    val sampleIDList = spark.sparkContext.textFile(sampleFileName.toString).repartition(numExecutors)
    val itemCounts = sampleIDList.glom.map(_.length).collect()
    println("==== No. of sample IDs in each partition ====")
    for (x <- itemCounts) {
      println(" [", x, "] ")
    }
    println("Min: ", itemCounts.min, " Max: ", itemCounts.max, " Avg: ",
      itemCounts.sum/itemCounts.length)
    val maxTasks = itemCounts.sum/itemCounts.length

    //val pairList = sampleIDList.map(x => (x, 10)).partitionBy(
    //  new HashPartitioner(numExecutors))

    def splitLine(x: String): (Long, String) = {
      val arr = x.split(" ")
      return (arr(0).toLong, arr(1))
    }

    val sortedSampleIDList = sampleIDList.map(x => splitLine(x)).repartitionAndSortWithinPartitions(
      new HashPartitioner(numExecutors))
    //val temp = sizeList.glom.collect()

    commandToExecute.toString() match {
      case "D" =>
        if (singleMode==false) { // parallel
          sortedSampleIDList
            .map(s => ConcurrentContext.executeAsync(runInterleave(s._2)))
            .mapPartitions(it => ConcurrentContext.await(it, batchSize = min(maxTasks, minBatchSize)))
            .map(x => ConcurrentContext.executeAsync(runDenovo(x, kmerVal.toString.toInt)))
            .mapPartitions(it => ConcurrentContext.await(it, batchSize = min(maxTasks, minBatchSize)))
            .collect()
            .foreach(x => println(s"Finished interleaved FASTQ and de novo assembly of $x"))
        }
        else {
          sortedSampleIDList.repartition(1)
            .map(s => ConcurrentContext.executeAsync(runInterleave(s._2)))
            .mapPartitions(it => ConcurrentContext.await(it, batchSize = 1))
            .map(x => ConcurrentContext.executeAsync(runDenovo(x, kmerVal.toString.toInt)))
            .mapPartitions(it => ConcurrentContext.await(it, batchSize = 1))
            .collect()
            .foreach(x => println(s"Finished interleaved FASTQ and de novo assembly of $x"))
        }
      case "E" =>
        if (singleMode==false) { // parallel
          sortedSampleIDList
            .map(s => ConcurrentContext.executeAsync(runInterleave(s._2)))
            .mapPartitions(it => ConcurrentContext.await(it, batchSize = min(maxTasks, minBatchSize)))
            .map(x => ConcurrentContext.executeAsync(runDenovo(x, kmerVal.toString.toInt)))
            .mapPartitions(it => ConcurrentContext.await(it, batchSize = min(maxTasks, minBatchSize)))
            .collect()
            .foreach(x => println(s"Finished interleaved FASTQ and de novo assembly of $x"))
        }
        else {
          sortedSampleIDList.repartition(1)
            .map(s => ConcurrentContext.executeAsync(runInterleave(s._2)))
            .mapPartitions(it => ConcurrentContext.await(it, batchSize = 1))
            .map(x => ConcurrentContext.executeAsync(runDenovo(x, kmerVal.toString.toInt)))
            .mapPartitions(it => ConcurrentContext.await(it, batchSize = 1))
            .collect()
            .foreach(x => println(s"Finished interleaved FASTQ and de novo assembly of $x"))
        }
      case "W" =>
        if (singleMode==false) { // parallel
          sortedSampleIDList
            .map(s => ConcurrentContext.executeAsync(runInterleave(s._2)))
            .mapPartitions(it => ConcurrentContext.await(it, batchSize = min(maxTasks, minBatchSize)))
            .map(x => ConcurrentContext.executeAsync(runBWA(x, referenceGenome)))
            .mapPartitions(it => ConcurrentContext.await(it, batchSize = min(maxTasks, minBatchSize)))
            .map(x => ConcurrentContext.executeAsync(runSortMarkDup(x)))
            .mapPartitions(it => ConcurrentContext.await(it, batchSize = min(maxTasks, minBatchSize)))
            .map(x => ConcurrentContext.executeAsync(runFreebayes(x, referenceGenome)))
            .mapPartitions(it => ConcurrentContext.await(it, batchSize = min(maxTasks, minBatchSize)))
            .collect()
            .foreach(x => println(s"Finished basic variant analysis of whole genome sequence $x"))
        }
        else {
          sortedSampleIDList.repartition(1)
            .map(s => ConcurrentContext.executeAsync(runInterleave(s._2)))
            .mapPartitions(it => ConcurrentContext.await(it, batchSize = 1))
            .map(x => ConcurrentContext.executeAsync(runBWA(x, referenceGenome)))
            .mapPartitions(it => ConcurrentContext.await(it, batchSize = 1))
            .map(x => ConcurrentContext.executeAsync(runSortMarkDup(x)))
            .mapPartitions(it => ConcurrentContext.await(it, batchSize = 1))
            .map(x => ConcurrentContext.executeAsync(runFreebayes(x, referenceGenome)))
            .mapPartitions(it => ConcurrentContext.await(it, batchSize = 1))
            .collect()
            .foreach(x => println(s"Finished basic variant analysis of whole genome sequence $x"))
        }
      case "R" =>
        if (singleMode==false) { // parallel
          sortedSampleIDList
            .map(s => ConcurrentContext.executeAsync(runInterleave(s._2)))
            .mapPartitions(it => ConcurrentContext.await(it, batchSize = min(maxTasks, minBatchSize)))
            .map(x => ConcurrentContext.executeAsync(runDenovo(x, kmerVal.toString.toInt)))
            .mapPartitions(it => ConcurrentContext.await(it, batchSize = min(maxTasks, minBatchSize)))
            .collect()
            .foreach(x => println(s"Finished interleaved FASTQ and de novo assembly of $x"))
        }
        else {
          sortedSampleIDList.repartition(1)
            .map(s => ConcurrentContext.executeAsync(runInterleave(s._2)))
            .mapPartitions(it => ConcurrentContext.await(it, batchSize = 1))
            .map(x => ConcurrentContext.executeAsync(runDenovo(x, kmerVal.toString.toInt)))
            .mapPartitions(it => ConcurrentContext.await(it, batchSize = 1))
            .collect()
            .foreach(x => println(s"Finished interleaved FASTQ and de novo assembly of $x"))
        }
      case _ => println("Invalid command"); usage()
    }

    log.info("\uD83D\uDC49 Completed the genome processing successfully.")
    spark.stop()
  }
}