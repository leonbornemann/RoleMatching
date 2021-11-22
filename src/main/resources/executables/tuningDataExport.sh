#file paths:
graphDir="<substitute the path to a diretory contained the simple graph files>"
resultDir="<substitute the path to a directory for the output here>"
jarFile="<substitute the path to the jar file obtained by sbt assembly here>"
#other inputs:
datasetNames=("austintexas" "chicago" "gov.maryland" "oregon" "utah" "education" "football" "military" "politics" "tv_and_film")
dataSources=("socrata" "socrata" "socrata" "socrata" "socrata" "wikipedia" "wikipedia" "wikipedia" "wikipedia" "wikipedia")
wikipediaTrainEndTimes="2011-05-07;2012-05-05;2013-05-04;2014-05-03;2015-05-02;2016-04-30;2017-04-29;2018-04-28"
socrataTrainEndTimes="2020-04-30;2020-05-31;2020-06-30;2020-07-31;2020-08-31;2020-09-30;2020-10-31"
matchingEndTimes=($socrataTrainEndTimes $socrataTrainEndTimes $socrataTrainEndTimes $socrataTrainEndTimes $socrataTrainEndTimes $wikipediaTrainEndTimes $wikipediaTrainEndTimes $wikipediaTrainEndTimes $wikipediaTrainEndTimes $wikipediaTrainEndTimes)
#settinng up logging:
mkdir logs/

for i in "${!datasetNames[@]}";
do
        datasetName=${datasetNames[i]}
        #setting up result directories:
        currentResultDir="$resultDir/$datasetName/"
        mkdir $currentResultDir
        resultFileStats="$currentResultDir/${datasetName}_stats.csv"
        graphResultFile="$currentResultDir/${datasetName}/"
        logFile="logs/${datasetName}_tuningDataExport.log"
        #setting up input file variables:
        simpleGraphDir="$graphDir/$datasetName/graph/"
        dataSource=${dataSources[i]}
        matchingEndTime=${matchingEndTimes[i]}
        #starting the process
        echo "Running Tuning and Transformation to memory efficient representation for $datasetName"
        java -ea -Xmx96g -cp $jarFile de.hpi.role_matching.cbrm.evidence_based_weighting.TuningDataExportMain $dataSource $simpleGraphDir $resultFileStats $graphResultFile $matchingEndTime > $logFile 2<&1
        echo "Finished with exit code $?"
done
