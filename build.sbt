val sparkVersion = "3.0.0"
scalaVersion := "2.12.8"

version := "0.1"
name := "avah_" + sparkVersion

sparkVersion match {
  case "3.0.0" =>
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.0";
    libraryDependencies += "org.apache.spark" %% "spark-repl" % "3.0.0";
  case "2.4.7" =>
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.7";
    libraryDependencies += "org.apache.spark" %% "spark-repl" % "2.4.7";
}