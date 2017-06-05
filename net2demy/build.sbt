lazy val root = (project in file(".")).
  settings(
    name := "Scala url import",
    scalaVersion := "2.11.8",
    version := "1.0",
    libraryDependencies += "org.apache.httpcomponents" % "httpclient" % "4.5.3",
    libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.1.0",
    libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.8.0",
    libraryDependencies += "org.apache.commons" % "commons-compress" % "1.14"
  )
