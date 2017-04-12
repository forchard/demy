#!/bin/bash
# Script to ensure an xml option is present on a file

file=""
container_xpath=""
node=""
property_node=""
property_text=""
value_node=""
value=""
 
for ((i=1;i<=$#;i++)); 
do
  if [ ${!i} == "--container-xpath" -o ${!i} = "-c" ] 
    then ((i++)) 
      container_xpath=${!i};
  elif [ ${!i} == "--node" -o ${!i} = "-n" ] 
    then ((i++)) 
      node=${!i};
  elif [ ${!i} == "--property-node" -o ${!i} = "-P" ] 
    then ((i++)) 
      property_node=${!i};
  elif [ ${!i} == "--property-text" -o ${!i} = "-T" ] 
    then ((i++)) 
      property_text=${!i};
  elif [ ${!i} == "--value-node" -o ${!i} = "-V" ] 
    then ((i++)) 
      value_node=${!i};
  elif [ ${!i} == "--value" -o ${!i} = "-v" ] 
    then ((i++)) 
      value=${!i};
  elif [ ${!i} == "--file" -o ${!i} = "-f" ] 
    then ((i++)) 
      file=${!i};
  else 
    file=${!i};
  fi
done;

equal_xpath="$container_xpath/$node[$property_node='$property_text' and $value_node='$value']"
update_xpath="$container_xpath/$node[$property_node='$property_text']"
insert_xpath="$container_xpath"
new_xpath="$container_xpath/temp$node"


#echo $file
#echo $equal_xpath
#echo $update_xpath
#echo $empty_xpath
equal=`xmlstarlet sel -t -i "$equal_xpath" -o "EQUAL" $file`
if [ "$equal" == "EQUAL" ]
then
  echo NO-CHANGE
  exit 0
fi

update=`xmlstarlet sel -t -i "$update_xpath" -o "UPDATE" $file`
if [ "$update" == "UPDATE" ]
then 
  cmd="xmlstarlet ed --inplace -O -a \"$update_xpath\" -t elem -n temp$node"
  cmd="$cmd -d \"$update_xpath\"" 
  cmd="$cmd -s \""$new_xpath"\" -t elem -n $property_node -v \"$property_text\"" 
  cmd="$cmd -s \""$new_xpath"\" -t elem -n $value_node -v \"$value\"" 
  cmd="$cmd -r \""$new_xpath"\" -v \"$node\"" 
  cmd="$cmd \"$file\""
  eval $cmd
  echo CHANGED:DIFFERENT
  exit 0
fi

cmd="xmlstarlet ed --inplace -O -s \"$insert_xpath\" -t elem -n temp$node"
cmd="$cmd -s \""$new_xpath"\" -t elem -n $property_node -v \"$property_text\"" 
cmd="$cmd -s \""$new_xpath"\" -t elem -n $value_node -v \"$value\"" 
cmd="$cmd -r \""$new_xpath"\" -v \"$node\"" 
cmd="$cmd \"$file\""
eval $cmd
echo CHANGED:NOT-PRESENT
exit 0
