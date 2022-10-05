sourceDir="/data/changedata/roleMerging/final_experiments/scalability_experiments_time_scaled/"
trainTimeEnds=("2016-05-07" "2029-09-08" "2043-01-10" "2056-05-13" "2069-09-14" "2083-01-16" "2096-05-19" "2109-09-21" "2123-01-23" "2136-05-26")
timeFactors=("1" "2" "3" "4" "5" "6" "7" "8" "9" "10")
timeMeasurementFile="runtimesScalability.csv"
baseName="football.json_"
taskNames=("EM" "CQM" "TSM" "VSM")
echo "file,time [s],task" >> $timeMeasurementFile
for i in "${!trainTimeEnds[@]}";
do
  trainTimeEnd=${trainTimeEnds[i]}
  timeFactor=${timeFactors[i]}
  fName="${baseName}${timeFactor}"
  fullPath="$sourceDir/$fName"
  for j in "${!taskNames[@]}";
  do
    taskName=${taskNames[j]}
    start=$(date +%s)
    echo $fullPath
    #run Program
    bash cbrbSingleExecute.sh $fName $taskName 32 $trainTimeEnd > logsGroupingTimeExtended/${taskName}_${fName}.log 2>&1
    #bash rmSingleExecute.sh $fName 32 > logsRM/$fName.log 2>&1
    end=$(date +%s)
    runtime=$( echo "$end - $start" | bc -l )
    echo "Took $runtime seconds for $taskName"
    echo "$fName,$runtime,$taskName" >> $timeMeasurementFile
  done
done

