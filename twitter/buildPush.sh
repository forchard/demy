sbt assembly

scp target/scala-2.11/demy-twitter-track-assembly-1.0.jar sparkrunner:~

ssh sparkrunner 'echo "/space/hadoop/spark_home/bin/spark-submit --class \"twitemy.Execute\" --master yarn --executor-cores 1 --driver-cores 1 --num-executors 1 --deploy-mode cluster --name \"Twitter imports\" --driver-memory 520M --executor-memory 520M \"/home/spark/demy-twitter-track-assembly-1.0.jar\" " > run.sh; chmod 700 run.sh'

