lazy val core = RootProject(file("../core"))
lazy val storage = RootProject(file("../storage"))

lazy val root = (project in file("."))
  .dependsOn(core)
  .dependsOn(storage)
  .settings(
    name := "demy-machine-learning-library",
    scalaVersion := "2.11.8",
    version := "1.0",
    libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.2" % "provided",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.2" % "provided",
    libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.3.2" % "provided",
    libraryDependencies += "org.apache.lucene" % "lucene-core" % "7.2.1", 
    libraryDependencies += "org.apache.lucene" % "lucene-queryparser" % "7.2.1", 
    libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.5",
    libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test",
//    scalacOptions ++= Seq("-deprecation", "-feature"),

    assemblyMergeStrategy in assembly := {
      case PathList("org","aopalliance", xs @ _*) => MergeStrategy.last
      case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
      case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
      case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
      case PathList("org", "apache", xs @ _*) => MergeStrategy.last
      case PathList("com", "google", xs @ _*) => MergeStrategy.last
      case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
      case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
      case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
      case "about.html" => MergeStrategy.rename
      case "overview.html" => MergeStrategy.rename
      case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
      case "META-INF/mailcap" => MergeStrategy.last
      case "META-INF/mimetypes.default" => MergeStrategy.last
      case "plugin.properties" => MergeStrategy.last
      case "log4j.properties" => MergeStrategy.last
      case "git.properties" => MergeStrategy.last
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
    }
  )


