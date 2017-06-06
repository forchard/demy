sbt package

scp target/scala-2.11/scala-geo-processing_3.11-1.0.jar sparkrunner:~/scala-geo-processing_2.11-1.0.jar

ssh sparkrunner 'echo "/space/hadoop/spark_home/bin/spark-submit --class \"geodemy.Execute\" --master yarn --executor-cores 1 --driver-cores 1 --num-executors 1 --deploy-mode client --name \"Geo Processing\" --driver-memory 520M --executor-memory 520M  \"/home/spark/scala-geo-processing_2.11-1.0.jar\" " > run.sh; chmod 700 run.sh'


