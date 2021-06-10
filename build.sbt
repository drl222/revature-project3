name := "revature-project3"

version := "0.1"

scalaVersion := "2.12.14"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.7"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.7"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.4.7"

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.10.1"
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.10.1"
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "2.10.1"