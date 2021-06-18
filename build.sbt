name := "revature-project3"

version := "0.01"

scalaVersion := "2.11.0"


libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.7"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.7"
//libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.4.7"

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.10.1"
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.10.1"
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "2.10.1"

// https://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk-core
//libraryDependencies += "com.amazonaws" % "aws-java-sdk-core" % "1.11.970" % "provided"

// https://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk-emr
//libraryDependencies += "com.amazon" % "aws-java-sdk-emr" % "1.11.970" % "provided"

// https://mvnrepository.com/artifact/org.apache.maven.plugins/maven-assembly-plugin
//libraryDependencies += "org.apache.maven.plugins" % "maven-assembly-plugin" % "3.3.0"