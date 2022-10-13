sourceDir="../time_scaled_rolesets_for_rm/"
timeFactors=("2" "3" "4" "5" "6")
timeMeasurementFile="runtimesScalabilityRMTime.csv"
baseName="football.json_"
echo "file,time [s],task" >> $timeMeasurementFile
for i in "${!timeFactors[@]}";
do
  timeFactor=${timeFactors[i]}
  fName="${baseName}${timeFactor}"
  fullPath="$sourceDir/$fName"
  start=$(date +%s)
  echo $fullPath
  #run Program
  bash rmSingleExecute.sh $fName > logsRM/${fName}.log 2>&1
  #bash rmSingleExecute.sh $fName 32 > logsRM/$fName.log 2>&1
  end=$(date +%s)
  runtime=$( echo "$end - $start" | bc -l )
  echo "Took $runtime seconds for $taskName"
  echo "$fName,$runtime,$taskName" >> $timeMeasurementFile
done
