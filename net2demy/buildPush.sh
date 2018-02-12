cd ../geo
sbt publish-local
cd ../net2demy
sbt package

scp target/scala-2.11/scala-url-import_2.11-1.0.jar sparkrunner:~/scala-url-import_2.11-1.0.jar
scp ~/.ivy2/local/geograpgic-processing/geograpgic-processing_2.11/1.0/jars/geograpgic-processing_2.11.jar sparkrunner:~/geograpgic-processing_2.11.jar
scp ~/.ivy2/cache/commons-io/commons-io/jars/commons-io-2.4.jar sparkrunner:~/commons-io-2.4.jar
scp ~/.ivy2/cache/org.apache.commons/commons-compress/jars/commons-compress-1.14.jar sparkrunner:~/commons-compress-1.14.jar 
scp ~/.ivy2/cache/org.tukaani/xz/jars/xz-1.6.jar sparkrunner:~/xz-1.6.jar
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
scp  ~/.ivy2/cache/org.jsoup/jsoup/jars/jsoup-1.11.2.jar sparkrunner:~/jsoup-1.11.2.jar

ssh sparkrunner 'echo "/space/hadoop/spark_home/bin/spark-submit --class \"demy.webReader.Execute\" --master yarn --executor-cores 2 --driver-cores 2 --num-executors 2 --deploy-mode client --name \"Net 2 demy import\" --driver-memory 520M --executor-memory 520M --conf \"spark.driver.userClassPathFirst=true\" --jars \"commons-compress-1.14.jar,commons-io-2.4.jar,xz-1.6.jar,jts-core-1.14.0.jar,gt-shapefile-17.1.jar,gt-main-17.1.jar,gt-referencing-17.1.jar,gt-metadata-17.1.jar,gt-api-17.1.jar,gt-opengis-17.1.jar,gt-data-17.1.jar,jsr-275-1.0-beta-2.jar,core-0.26.jar,gt-epsg-hsql-17.1.jar,hsqldb-2.3.0.jar,geograpgic-processing_2.11.jar,jsoup-1.11.2.jar\" \"/home/spark/scala-url-import_2.11-1.0.jar\" " > run.sh; chmod 700 run.sh'

