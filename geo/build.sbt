lazy val root = (project in file(".")).
  settings(
    name := "Geograpgic processing",
    scalaVersion := "2.11.8",
    version := "1.0",
    libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.1.0",
    libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.8.0",
    libraryDependencies += "org.geotools" % "gt-main" % "17.1",
    libraryDependencies += "org.geotools" % "gt-shapefile" % "17.1"
  )
