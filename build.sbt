name := "revature-project3"

version := "0.1"

scalaVersion := "2.12.14"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.7"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.7"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.4.7"

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.10.1"
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.10.1"
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "2.10.1"

// https://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk
libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.11.970"



// https://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk-emr
libraryDependencies += "com.amazonaws" % "aws-java-sdk-emr" % "1.11.970"
