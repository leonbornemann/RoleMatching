nthreads="16"
thresholdForFork="30"
maxPairwiseListSizeForSingleThread="75"
roleSamplingRate="0.1"
timestampSamplingRate="0.1"

dsNames=("oregon" "austintexas" "utah" "gov.maryland" "chicago" "military" "education" "tv_and_film" "football" "politics")
endDateTrainPhases=("2020-04-30" "2020-04-30" "2020-04-30" "2020-04-30" "2020-04-30" "2016-05-07" "2016-05-07" "2016-05-07" "2016-05-07" "2016-05-07")
datasources=("socrata" "socrata" "socrata" "socrata" "socrata" "wikipedia" "wikipedia" "wikipedia" "wikipedia" "wikipedia")
mkdir logs/

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
        java -ea -Xmx64g -cp jar/DatasetVersioning-assembly-0.1.jar de.hpi.role_matching.cbrm.compatibility_graph.role_tree.CompatibilityGraphCreationMain $datasource $rolesetFile $resultDirEdges $resultDirStats $resultDirTime $endDateTrainPhase $nthreads $thresholdForFork $maxPairwiseListSizeForSingleThread $roleSamplingRate $timestampSamplingRate false > logs/$dsName.log 2>&1
        end=$(date +%s)
        runtime=$( echo "$end - $start" | bc -l )
        echo "Took $runtime seconds for $dsName"
        echo "$dsName,$runtime"
done
