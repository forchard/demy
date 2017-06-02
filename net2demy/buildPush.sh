sbt package

scp target/scala-2.11/scala-url-import_2.11-1.0.jar sparkrunner:~/scala-url-import_2.11-1.0.jar

ssh sparkrunner 'echo "/space/hadoop/spark_home/bin/spark-submit --class \"net2demy.Execute\" --master yarn --executor-cores 1 --driver-cores 1 --num-executors 1 --deploy-mode client --name \"Net 2 demy import\" --driver-memory 520M --executor-memory 520M \"/home/spark/scala-url-import_2.11-1.0.jar\" " > run.sh; chmod 700 run.sh'

