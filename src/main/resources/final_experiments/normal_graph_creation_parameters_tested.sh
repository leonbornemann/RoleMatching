nthreadsValues=("1" "2" "4" "8" "16" "32")
thresholdForFork="30"
maxPairwiseListSizeForSingleThread="75"
roleSamplingRate="0.1"
timestampSamplingRate="0.1"

#dsNames=("oregon" "austintexas" "utah" "gov.maryland" "chicago" "military" "education" "tv_and_film" "football" "politics")
#endDateTrainPhases=("2020-04-30" "2020-04-30" "2020-04-30" "2020-04-30" "2020-04-30" "2016-05-07" "2016-05-07" "2016-05-07" "2016-05-07" "2016-05-07")
#datasources=("socrata" "socrata" "socrata" "socrata" "socrata" "wikipedia" "wikipedia" "wikipedia" "wikipedia" "wikipedia")

dsNames=("chicago" "football")
endDateTrainPhases=("2020-04-30" "2016-05-07")
datasources=("socrata" "wikipedia")

dt=$(date +"%Y_%m_%d_%I_%M_%p")
mkdir logs/
logDir="logs/$dt/"
mkdir $logDir

for j in "${!nthreadsValues[@]}";
do
    nthreads=${nthreadsValues[j]}
    for i in "${!dsNames[@]}";
    do
            dsName=${dsNames[i]}
            echo "Processing $dsName"
            datasource=${datasources[i]}
            endDateTrainPhase=${endDateTrainPhases[i]}
            rolesetFile="/data/changedata/roleMerging/final_experiments/rolesets/${dsName}.json"
            resultDirEdges="/data/changedata/roleMerging/final_experiments/graphCreation/$dsName/edges/"
            resultDirStats="/data/changedata/roleMerging/final_experiments/graphCreation/$dsName/stats/"
            resultDirTime="/data/changedata/roleMerging/final_experiments/graphCreation/$dsName/time/"
            start=$(date +%s)
            logFileName="${dsName}_$nthreads.log"
            java -ea -Xmx64g -cp jar/DatasetVersioning-assembly-0.1.jar de.hpi.role_matching.cbrm.compatibility_graph.role_tree.CompatibilityGraphCreationMain $datasource $rolesetFile $resultDirEdges $resultDirStats $resultDirTime $endDateTrainPhase $nthreads $thresholdForFork $maxPairwiseListSizeForSingleThread $roleSamplingRate $timestampSamplingRate false > "$logDir/$logFileName" 2>&1
            end=$(date +%s)
            runtime=$( echo "$end - $start" | bc -l )
            echo "Took $runtime seconds for $dsName"
            echo "$nthreads,$dsName,$runtime"
    done
done


