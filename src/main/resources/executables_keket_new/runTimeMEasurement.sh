sourceDir="time_scaled_rolesets/"
#trainTimeEnds=("2016-05-07" "2029-09-08" "2043-01-10" "2056-05-13" "2069-09-14" "2083-01-16" "2096-05-19" "2109-09-21" "2123-01-23" "2136-05-26")
trainTimeEnds=("2109-09-21" "2123-01-23" "2136-05-26")
#timeFactors=("1" "2" "3" "4" "5" "6" "7" "8" "9" "10")
timeFactors=("8" "9" "10")
timeMeasurementFile="runtimesScalability.csv"
baseName="football.json_"
echo "file,time [s],task" >> $timeMeasurementFile
for i in "${!trainTimeEnds[@]}";
do
  trainTimeEnd=${trainTimeEnds[i]}
  timeFactor=${timeFactors[i]}
  fName="${baseName}${timeFactor}"
  fullPath="$sourceDir/$fName"
  start=$(date +%s)
  echo $fullPath
  #run Program
  bash cbrbSingleExecute.sh $fName $timeFactor 32 $trainTimeEnd > logsGroupingTimeExtended/${taskName}_${fName}.log 2>&1
  #bash rmSingleExecute.sh $fName 32 > logsRM/$fName.log 2>&1
  end=$(date +%s)
  runtime=$( echo "$end - $start" | bc -l )
  echo "Took $runtime seconds for $taskName"
  echo "$fName,$runtime,$taskName" >> $timeMeasurementFile
done

