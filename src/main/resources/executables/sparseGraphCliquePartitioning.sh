#input directories
graphDir="<substitute the path to a directory containing the graph set files (output of tuning Data Export)>"
rolesetDir="<substitute the path to a directory contained the role sets json files here>"
resultDir="<substitute the path to a directory for the output here>"
jarFile="<substitute the path to the jar file obtained by sbt assembly here>"

weightSettingSocrata="../weightSettings/socrata.json"
weightSettingWikipedia="../weightSettings/wikipedia.json"
weightSettings=( "$weightSettingSocrata" "$weightSettingSocrata" "$weightSettingSocrata" "$weightSettingSocrata" "$weightSettingSocrata" "$weightSettingWikipedia" "$weightSettingWikipedia" "$weightSettingWikipedia" "$weightSettingWikipedia" "$weightSettingWikipedia")
datasetNames=("austintexas" "chicago" "gov.maryland" "oregon" "utah" "education" "football" "military" "politics" "tv_and_film")
dataSources=("socrata" "socrata" "socrata" "socrata" "socrata" "wikipedia" "wikipedia" "wikipedia" "wikipedia" "wikipedia")
matchingEndTimes=("2020-04-30" "2020-04-30" "2020-04-30" "2020-04-30" "2020-04-30" "2011-05-07" "2011-05-07" "2011-05-07" "2011-05-07" "2011-05-07")

#settinng up logging:
mkdir logs/

for i in "${!datasetNames[@]}";
do
        datasetName=${datasetNames[i]}
        #setting up result directories:
        currentResultDir="$resultDir/$datasetName/"
        matchingEndTime=${matchingEndTimes[i]}
        weightConfig=${weightSettings[i]}
        currentResultDir="$resultDir/$datasetName/sgcp/"
        rolesetFile="$rolesetDir/$datasetName.json"
        graphsetFile="$graphDir/$datasetName/$datasetName.json"
        mkdir $resultDir/$datasetName/
        mkdir $currentResultDir
        logFile="logs/${datasetName}_sgcp.log"
        dataSource=${dataSources[i]}
        #starting the process
        echo "Running $datasetName"
        java -ea -Xmx96g -cp $jarFile de.hpi.role_matching.cbrm.sgcp.SparseGraphCliquePartitioningMain $dataSource $graphsetFile $matchingEndTime $weightConfig $currentResultDir $rolesetFile  > $logFile 2<&1
        echo "Finished with exit code $?"
done