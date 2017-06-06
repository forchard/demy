sbt package

scp target/scala-2.11/scala-url-import_2.11-1.0.jar sparkrunner:~/scala-url-import_2.11-1.0.jar
scp commons-compress-1.14.jar sparkrunner:~/commons-compress-1.14.jar 
scp commons-io-2.4.jar sparkrunner:~/commons-io-2.4.jar
scp xz-1.6.jar sparkrunner:~/xz-1.6.jar

ssh sparkrunner 'echo "/space/hadoop/spark_home/bin/spark-submit --class \"net2demy.Execute\" --master yarn --executor-cores 1 --driver-cores 1 --num-executors 1 --deploy-mode cluster --name \"Net 2 demy import\" --driver-memory 520M --executor-memory 520M --conf \"spark.driver.userClassPathFirst=true\" --jars \"commons-compress-1.14.jar,commons-io-2.4.jar,xz-1.6.jar\" \"/home/spark/scala-url-import_2.11-1.0.jar\" " > run.sh; chmod 700 run.sh'


