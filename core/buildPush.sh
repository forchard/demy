sbt package
cd ../deploy
ansible bgdtz2.oxa -b -m copy -a "src=../mllib/target/scala-2.11/demy-storage-layer_2.11-1.0.jar dest={{zeppelin_run}}/custom-libs/"
cd ../mllib
