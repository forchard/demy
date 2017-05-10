scp target/scala-2.11/scala-twitter-extract_2.11-1.0.jar sparkrunner:~/scala-twitter-extract_2.11-1.0.jar
scp twitter4j-stream-4.0.4.jar sparkrunner:~/twitter4j-stream-4.0.4.jar
scp twitter4j-core-4.0.4.jar sparkrunner:~/twitter4j-core-4.0.4.jar

ssh sparkrunner "/space/hadoop/spark_home/bin/spark-submit --class \"twitemy.Execute\" --master yarn --jars \"twitter4j-stream-4.0.4.jar,twitter4j-core-4.0.4.jar\" \"/home/spark/scala-twitter-extract_2.11-1.0.jar\" "
