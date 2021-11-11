TODO: change this code

rolesetDir="<substitute the path to a diretory contained the role sets json files here>"
resultDir="<substitute the path to a directory for the output here>"
jarFile="<substitute the path to the jar file obtained by sbt assembly here>"
delta_fork="30"
delta_pair="75"
datasetNames=("austintexas" "chicago" "gov.maryland" "oregon" "utah" "education" "football" "military" "politics" "tv_and_film")
dataSources=("socrata" "socrata" "socrata" "socrata" "socrata" "wikipedia" "wikipedia" "wikipedia" "wikipedia" "wikipedia")
matchingEndTimes=("2020-04-30" "2020-04-30" "2020-04-30" "2020-04-30" "2020-04-30" "2011-05-07" "2011-05-07" "2011-05-07" "2011-05-07" "2011-05-07")
samplingRateRoles="0.1"
samplingRateTimestamps="0.1"
nThreads=16

#settinng up logging:
mkdir logs/

for i in "${!datasetNames[@]}";
do
        datasetName=${datasetNames[i]}
        #setting up result directories:
        currentResultDir="$resultDir/$datasetName/"
        graphDir="$currentResultDir/graph/"
        statDir="$currentResultDir/stats/"
        timeMeasurementDir="$currentResultDir/timeMeasurement/"
        logFile="logs/$datasetName.log"
        mkdir currentResultDir
        mkdir graphDir
        mkdir statDir
        mkdir timeMeasurementDir
        #setting up input file variables:
        rolesetFile="$rolesetDir/$datasetName.json"
        matchingEndTime=${matchingEndTimes[i]}
        dataSource=${dataSources[i]}
        #starting the process
        echo "Running $datasetName with nThreads $nThreads"
        java -ea -Xmx96g -cp $jarFile de.hpi.role_matching.cbrm.compatibility_graph.role_tree.CompatibilityGraphCreationMain $dataSource $rolesetFile $graphDir $statDir $timeMeasurementDir $matchingEndTime $nThreads $delta_fork $delta_pair $samplingRateRoles $samplingRateTimestamps > $logFile 2<&1
        echo "Finished with exit code $?"
done