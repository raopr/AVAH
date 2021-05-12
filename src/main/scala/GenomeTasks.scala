/**
 * University of Missouri-Columbia
 * 2020
 */

import java.util.Calendar
import sys.process._

object GenomeTasks {

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
  def runInterleave[T](x: T):(T, Int) = {
    val beginTime = Calendar.getInstance().getTime()
    println(s"Starting Interleave FASTQ on ($x) at $beginTime")
    val sampleID = x.toString

    var retInterleave = -1
    try {
      // Create interleaved fastq files
      val cannoliSubmit = sys.env("CANNOLI_HOME") + "/exec/cannoli-submit"
      //val sparkMaster = "spark://vm0:7077"
      val hdfsPrefix = "hdfs://vm0:9000"
      retInterleave = Seq(s"$cannoliSubmit", "--master", "yarn", "--", "interleaveFastq",
        s"$hdfsPrefix/${sampleID}_1.fastq.gz",
        s"$hdfsPrefix/${sampleID}_2.fastq.gz",
        s"$hdfsPrefix/${sampleID}.ifq").!
    } catch {
      case e: Exception => print(s"Exception in Interleave FASTQ, check sequence ID $x")
    }

    val endTime = Calendar.getInstance().getTime()
    println(s"Completed Interleave FASTQ on ($x) at ${endTime}, return values: $retInterleave")

    (x, retInterleave)
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
  def runBWA[T](x: T, referenceGenome: String):(T, Int) = {
    val beginTime = Calendar.getInstance().getTime()
    println(s"Starting BWA on ($x) at $beginTime")
    val sampleID = x.toString

    val cannoliSubmit = sys.env("CANNOLI_HOME") + "/exec/cannoli-submit"
    //val sparkMaster = "spark://vm0:7077"
    val hdfsPrefix = "hdfs://vm0:9000"
    val bwaCmd = sys.env("BWA_HOME") + "/bwa"
    val hdfsCmd = sys.env("HADOOP_HOME") + "/bin/hdfs"

    var retBWA = -1
    try {
      // Delete $sampleId.bam_* files
      val retDel = Seq(s"$hdfsCmd", "dfs", "-rm", "-r", "-skipTrash", s"/${sampleID}.bam_*")

      val execBWA = Seq(s"$cannoliSubmit", "--master", "yarn", "--", "bwaMem",
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
        "-add_files")

      retBWA = (retDel #&& execBWA #|| execBWA).!

    }
    catch {
      case e: Exception => print(s"Exception in BWA, check sequence ID $x")
    }

    val endTime = Calendar.getInstance().getTime()
    println(s"Completed BWA on ($x) ended at $endTime; return values bwa: $retBWA")

    (x, retBWA)
  }

  // Sort and Mark Duplicates
  def runSortMarkDup[T](x: T, bqsrIndelMode: Any):(T, Int) = {
    val beginTime = Calendar.getInstance().getTime()
    println(s"Starting sort/mark duplicates on ($x) at $beginTime")
    val sampleID = x.toString

    val adamSubmit = sys.env("ADAM_HOME") + "/exec/adam-submit"
    //val sparkMaster = "spark://vm0:7077"
    val hdfsPrefix = "hdfs://vm0:9000"

    var retSortDup = -1
    if (bqsrIndelMode==false) {
      try {
        retSortDup = Seq(s"$adamSubmit", "--master", "yarn", "--", "transformAlignments",
          s"$hdfsPrefix/${sampleID}.bam",
          s"$hdfsPrefix/${sampleID}.bam.adam",
          "-mark_duplicate_reads",
          "-sort_by_reference_position_and_index").!
      } catch {
        case e: Exception => print(s"Exception in sort/mark duplicates, check sequence ID $x")
      }
    }
    else  {
      val known_snps_hdfs = hdfsPrefix + "/known_snps"
      val known_indels_hdfs = hdfsPrefix + "/known_indels"

      try {
        retSortDup = Seq(s"$adamSubmit", "--master", "yarn",
          "--num-executors", "5",
          "--executor-memory", "40g",
          "--driver-memory", "40g",
          "--", "transformAlignments",
          s"$hdfsPrefix/${sampleID}.bam",
          s"$hdfsPrefix/${sampleID}.bam.adam",
          "-recalibrate_base_qualities",
          "-known_snps",
          s"$known_snps_hdfs",
          "-realign_indels",
          "-known_indels",
          s"$known_indels_hdfs",
          "-mark_duplicate_reads",
          "-sort_by_reference_position_and_index").!
      } catch {
        case e: Exception => print(s"Exception in sort/mark duplicates/BQSR/Indel realign, check sequence ID $x")
      }
    }

    val endTime = Calendar.getInstance().getTime()
    println(s"Completed sort/mark duplicates on ($x) ended at $endTime; mode: $bqsrIndelMode return values $retSortDup")

    (x, retSortDup)
  }

  // Sort, Mark Duplicates, BSQR, indel realignment
  def runSortMarkDupBQSRIndel[T](x: T):(T, Int) = {
    val beginTime = Calendar.getInstance().getTime()
    println(s"Starting sort/mark duplicates/BQSR/Indel realign on ($x) at $beginTime")
    val sampleID = x.toString

    val adamSubmit = sys.env("ADAM_HOME") + "/exec/adam-submit"
    //val sparkMaster = "spark://vm0:7077"
    val hdfsPrefix = "hdfs://vm0:9000"
    val known_snps_hdfs = hdfsPrefix + "/known_snps"
    val known_indels_hdfs = hdfsPrefix + "/known_indels"

    var retSortDupBQSRIndel = -1
    try {
      retSortDupBQSRIndel = Seq(s"$adamSubmit", "--master", "yarn",
        "--num-executors", "5",
        "--executor-memory", "40g",
        "--driver-memory", "40g",
        "--", "transformAlignments",
        s"$hdfsPrefix/${sampleID}.bam",
        s"$hdfsPrefix/${sampleID}.bam.adam",
        "-recalibrate_base_qualities",
        "-known_snps",
        s"$known_snps_hdfs",
        "-realign_indels",
        "-known_indels",
        s"$known_indels_hdfs",
        "-mark_duplicate_reads",
        "-sort_by_reference_position_and_index").!
    } catch {
      case e: Exception => print(s"Exception in sort/mark duplicates/BQSR/Indel realign, check sequence ID $x")
    }

    val endTime = Calendar.getInstance().getTime()
    println(s"Completed sort/mark duplicates/BQSR/Indel realign on ($x) ended at $endTime; return values $retSortDupBQSRIndel")

    (x, retSortDupBQSRIndel)
  }

  // Freebayes
  def runFreebayes[T](x: T, referenceGenome: String):(T, Int) = {
    val beginTime = Calendar.getInstance().getTime()
    println(s"Starting Freebayes on ($x) at $beginTime")
    val sampleID = x.toString

    val cannoliSubmit = sys.env("CANNOLI_HOME") + "/exec/cannoli-submit"
    //val sparkMaster = "spark://vm0:7077"
    val hdfsPrefix = "hdfs://vm0:9000"
    val freeBayesCmd = sys.env("FREEBAYES_HOME") + "/bin/freebayes"

    var retFreebayes = -1
    try {
      retFreebayes = Seq(s"$cannoliSubmit", "--master", "yarn", "--", "freebayes",
        s"$hdfsPrefix/${sampleID}.bam.adam",
        s"$hdfsPrefix/${sampleID}.vcf",
        "-executable",
        s"$freeBayesCmd",
        "-reference",
        s"file:///mydata/$referenceGenome.fa",
        "-add_files",
        "-single").!
    } catch {
      case e: Exception => print(s"Exception in Freebayes, check sequence ID $x")
    }

    // Delete all intermediate files as they consume a lot of space
    val hdfsCmd = sys.env("HADOOP_HOME") + "/bin/hdfs"
    val retDelifq = Seq(s"$hdfsCmd", "dfs", "-rm", "-r", "-skipTrash", s"/${sampleID}.ifq").!
    val retDelbam = Seq(s"$hdfsCmd", "dfs", "-rm", "-r", "-skipTrash", s"/${sampleID}.bam*").!
    val retDelvcf = Seq(s"$hdfsCmd", "dfs", "-rm", "-r", "-skipTrash", s"/${sampleID}.vcf_*").!

    // Create a empty .retry file
    val retryExt = ".retry"

    val retCreateRetryExt =
      if (retFreebayes != 0) {Seq(s"$hdfsCmd", "dfs", "-touchz", s"/${sampleID}${retryExt}").!} else {0}

    val endTime = Calendar.getInstance().getTime()
    println(s"Completed Freebayes on ($x) ended at $endTime; return values $retFreebayes; " +
      s"delete return values: ${retDelvcf}+${retDelifq}+${retDelbam} " +
      s"create $retryExt file return value: ${retCreateRetryExt} ")

    (x, retFreebayes)
  }

  // Cleanup temporary files before retrying variant analysis
  def cleanupFiles[T](x: T):(T, Int) = {
    val beginTime = Calendar.getInstance().getTime()

    println(s"Starting Cleanup on ($x) at $beginTime")

    val sampleID = x.toString
    // Delete all intermediate files
    val hdfsCmd = sys.env("HADOOP_HOME") + "/bin/hdfs"
    val retDelifq = Seq(s"$hdfsCmd", "dfs", "-rm", "-r", "-skipTrash", s"/${sampleID}.ifq").!
    val retDelbam = Seq(s"$hdfsCmd", "dfs", "-rm", "-r", "-skipTrash", s"/${sampleID}.bam*").!
    val retDelvcf = Seq(s"$hdfsCmd", "dfs", "-rm", "-r", "-skipTrash", s"/${sampleID}.vcf*").!

    val finalRes = retDelifq + retDelbam + retDelvcf
    val endTime = Calendar.getInstance().getTime()
    println(s"Completed Cleanup on ($x) ended at $endTime; return value $finalRes; " +
      s"delete return values: ${retDelvcf}+${retDelifq}+${retDelbam}")

    (x, finalRes)
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
}
