lazy val root = (project in file(".")).
  settings(
    name := "Demy machine learning library",
    scalaVersion := "2.11.8",
    version := "1.0",
    libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0",
    libraryDependencies += "org.apache.lucene" % "lucene-core" % "7.2.1", 
    libraryDependencies += "org.apache.lucene" % "lucene-queryparser" % "7.2.1" 
  )
