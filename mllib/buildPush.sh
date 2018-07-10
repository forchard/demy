sbt package
cd ../deploy
ansible nodes -b -m copy -a "src=../mllib/target/scala-2.11/demy-machine-learning-library_2.11-1.0.jar dest={{zeppelin_run}}/custom-libs/"
cd ../mllib
