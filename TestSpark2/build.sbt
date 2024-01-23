ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.10"


lazy val root = (project in file("."))
  .settings(
    name := "TestSpark2",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.3.2", // or the appropriate Scala version
      "org.apache.spark" %% "spark-sql" % "3.3.2"  // or the version you are using
    )
  )

libraryDependencies += "com.crealytics" %% "spark-excel" % "0.13.5"
