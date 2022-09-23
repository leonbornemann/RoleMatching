sourceDir="/home/leon/data/dataset_versioning/finalExperiments/subsampledRolesets"
fNames=("football.json_0.1" "football.json_0.2" "football.json_0.3" "football.json_0.4" "football.json_0.5" "football.json_0.6" "football.json_0.7" "football.json_0.8" "football.json_0.9" "football.json_1.0")
#rm "runtimes.csv"
timeMeasurementFile="runtimes.csv"
echo "file,time [s],task" >> $timeMeasurementFile
for i in "${!fNames[@]}";
do
  fName=${fNames[i]}
  fullPath="$sourceDir/$fName"
  start=$(date +%s)
  echo $fullPath
  #run Program
  bash cbrbSingleExecute.sh $fullPath 32
  end=$(date +%s)
  runtime=$( echo "$end - $start" | bc -l )
  echo "Took $runtime seconds for $taskname"
  echo "$fName,$runtime,wait" >> $timeMeasurementFile
done
