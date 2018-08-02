#!/bin/bash

pids=""
commandlog="logcommand.out"
ssh sparkrunner "/space/hadoop/hadoop_home/bin/yarn application -list | grep RUNNING | grep "$1" | awk '{print \$1}' | xargs /space/hadoop/hadoop_home/bin/yarn applicationattempt -list | grep appattempt | grep RUNNING | awk '{print \$1}' | xargs /space/hadoop/hadoop_home/bin/yarn container -list | grep container_ | awk '{print \$1 \" \" \$10}' | awk '{split(\$1, a, \"_\");split(\$2, b, \":\");split(b[1], c, \"-\");print \"ssh yarn@\" c[1] \".oxa tail -n 10 -f /space/hadoop/hadoop_run/logs/yarn/application_\" a[3] \"_\" a[4] \"/\" \$1 }'"> $commandlog

filename="$commandlog"
while read -r line
do
  $line/stderr & 
  pids="$pids $!"
  $line/stdout & 
  pids="$pids $!"
done < "$filename"
rm $commandlog
echo "PIDS: oo$pids oo"

trap "kill -9 $pids" SIGINT SIGTERM
wait $pids
