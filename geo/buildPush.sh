sbt package

scp target/scala-2.11/geograpgic-processing_2.11-1.0.jar sparkrunner:~/geograpgic-processing_2.11-1.0.jar
scp ~/.ivy2/cache/org.geotools/gt-main/jars/gt-main-17.1.jar sparkrunner:~/gt-main-17.1.jar
scp ~/.ivy2/cache/org.geotools/gt-shapefile/jars/gt-shapefile-17.1.jar sparkrunner:~/gt-shapefile-17.1.jar
scp ~/.ivy2/cache/org.geotools/gt-referencing/jars/gt-referencing-17.1.jar sparkrunner:~/gt-referencing-17.1.jar
scp ~/.ivy2/cache/org.geotools/gt-metadata/jars/gt-metadata-17.1.jar sparkrunner:~/gt-metadata-17.1.jar
scp ~/.ivy2/cache/org.geotools/gt-api/jars/gt-api-17.1.jar sparkrunner:~/gt-api-17.1.jar
scp ~/.ivy2/cache/org.geotools/gt-opengis/jars/gt-opengis-17.1.jar sparkrunner:~/gt-opengis-17.1.jar
scp ~/.ivy2/cache/org.geotools/gt-data/jars/gt-data-17.1.jar sparkrunner:~/gt-data-17.1.jar
scp ~/.ivy2/cache/com.vividsolutions/jts-core/jars/jts-core-1.14.0.jar sparkrunner:~/jts-core-1.14.0.jar
scp ~/.ivy2/cache/net.java.dev.jsr-275/jsr-275/jars/jsr-275-1.0-beta-2.jar sparkrunner:~/jsr-275-1.0-beta-2.jar
scp ~/.ivy2/cache/com.googlecode.efficient-java-matrix-library/core/jars/core-0.26.jar sparkrunner:~/core-0.26.jar
scp ~/.ivy2/cache/org.geotools/gt-epsg-hsql/jars/gt-epsg-hsql-17.1.jar sparkrunner:~/gt-epsg-hsql-17.1.jar
scp ~/.ivy2/cache/org.hsqldb/hsqldb/jars/hsqldb-2.3.0.jar sparkrunner:~/hsqldb-2.3.0.jar

#ssh sparkrunner 'echo "/space/hadoop/spark_home/bin/spark-submit --class \"demy.geo.Console\" --master yarn --executor-cores 1 --driver-cores 1 --num-executors 1 --deploy-mode client --name \"Geo Console\" --driver-memory 520M --executor-memory 520M --jars \"jts-core-1.14.0.jar,gt-shapefile-17.1.jar,gt-main-17.1.jar,gt-referencing-17.1.jar,gt-metadata-17.1.jar,gt-api-17.1.jar,gt-opengis-17.1.jar,gt-data-17.1.jar,jsr-275-1.0-beta-2.jar,gt-epsg-hsql-17.1.jar,core-0.26.jar,hsqldb-2.3.0.jar\" \"/home/spark/geograpgic-processing_2.11-1.0.jar\" shape2parquet /data/geo/ContourIris/CONTOURS-IRIS_2-1__SHP_LAMB93_FXX_2016-11-10-CONTOURS-IRIS-1_DONNEES_LIVRAISON_2015-CONTOURS-IRIS_2-1_SHP_LAMB93_FE-2015-CONTOURS-IRIS.shp /tmp/testShape.parquet" > run.sh; chmod 700 run.sh'

ssh sparkrunner 'echo "/space/hadoop/spark_home/bin/spark-submit --class \"demy.geo.Console\" --master yarn --executor-cores 1 --driver-cores 1 --num-executors 1 --deploy-mode client --name \"Geo Console\" --driver-memory 520M --executor-memory 520M --jars \"jts-core-1.14.0.jar,gt-shapefile-17.1.jar,gt-main-17.1.jar,gt-referencing-17.1.jar,gt-metadata-17.1.jar,gt-api-17.1.jar,gt-opengis-17.1.jar,gt-data-17.1.jar,jsr-275-1.0-beta-2.jar,gt-epsg-hsql-17.1.jar,core-0.26.jar,hsqldb-2.3.0.jar\" \"/home/spark/geograpgic-processing_2.11-1.0.jar\" shape2parquet /data/geo/ContourIris/CONTOURS-IRIS_2-1__SHP_LAMB93_FXX_2016-11-10-CONTOURS-IRIS-1_DONNEES_LIVRAISON_2015-CONTOURS-IRIS_2-1_SHP_UTM20W84GUAD_D971-2015-CONTOURS-IRIS.shp /tmp/testShape.parquet" > run.sh; chmod 700 run.sh'

