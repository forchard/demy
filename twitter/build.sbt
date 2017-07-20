lazy val root = (project in file(".")).
  settings(
    name := "Scala twitter extract",
    scalaVersion := "2.11.8",
    version := "1.0",
    libraryDependencies += "org.twitter4j" % "twitter4j-core" % "4.0.4",
    libraryDependencies += "org.twitter4j" % "twitter4j-stream" % "4.0.4",
    libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.1.0"
  )
