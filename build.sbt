name := "ScalaLearn"

version := "0.1"

scalaVersion := "2.12.14"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "2.4.0" % "provided",
    "org.apache.spark" %% "spark-sql" % "2.4.0" % "provided"
)