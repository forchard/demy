sbt assembly

cd ../deploy
ansible spark_edge -b -m copy -a "src=../twitter/target/scala-2.11/demy-twitter-track-assembly-1.0.jar dest={{zeppelin_run}}/custom-libs/"
cd ../twitter

ssh sparkrunner 'echo "nohup /space/hadoop/spark_home/bin/spark-submit --class \"demy.twitter.Execute\" --master yarn --executor-cores 1 --driver-cores 1 --num-executors 1 --deploy-mode client --name \"Twitter imports\" --driver-memory 520M --executor-memory 520M \"/space/hadoop/zeppelin_run/custom-libs/demy-twitter-track-assembly-1.0.jar\" >/dev/null 2>&1 &" > run.sh; chmod 700 run.sh'

