name := "user-item-ratings"

version := "1.0"

scalaVersion := "2.11.12"

mainClass in Global := Some("Main")

assemblyJarName in assembly := "useritemratings.jar"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.0",
  "org.apache.spark" %% "spark-sql" % "2.4.0",
  "com.typesafe" % "config" % "1.3.2",

  "org.scalatest" %% "scalatest" % "3.0.4" % "test",
  "org.mockito" % "mockito-core" % "2.23.0" % "test"
)
