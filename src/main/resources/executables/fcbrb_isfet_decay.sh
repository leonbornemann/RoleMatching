nthreads="16"
thresholdForFork="30"
maxPairwiseListSizeForSingleThread="75"
roleSamplingRate="0.1"
timestampSamplingRate="0.1"

dsNames=("oregon" "austintexas" "utah" "gov.maryland" "chicago" "military" "education" "tv_and_film" "football" "politics")
endDateTrainPhases=("2020-04-30" "2020-04-30" "2020-04-30" "2020-04-30" "2020-04-30" "2016-05-07" "2016-05-07" "2016-05-07" "2016-05-07" "2016-05-07")
datasources=("socrata" "socrata" "socrata" "socrata" "socrata" "wikipedia" "wikipedia" "wikipedia" "wikipedia" "wikipedia")
rolesetPaths=("/san2/data/change-exploration/roleMerging/finalExperiments/rolesetsSocrataWithDecay/" "/san2/data/change-exploration/roleMerging/finalExperiments/rolesetsSocrataWithDecay/" "/san2/data/change-exploration/roleMerging/finalExperiments/rolesetsSocrataWithDecay/" "/san2/data/change-exploration/roleMerging/finalExperiments/rolesetsSocrataWithDecay/" "/san2/data/change-exploration/roleMerging/finalExperiments/rolesetsSocrataWithDecay/" "/san2/data/change-exploration/roleMerging/finalExperiments/newWikipediaRolesets/PROBABILISTIC_DECAY_FUNCTION_0.7_7_2016-05-07/" "/san2/data/change-exploration/roleMerging/finalExperiments/newWikipediaRolesets/PROBABILISTIC_DECAY_FUNCTION_0.7_7_2016-05-07/" "/san2/data/change-exploration/roleMerging/finalExperiments/newWikipediaRolesets/PROBABILISTIC_DECAY_FUNCTION_0.7_7_2016-05-07/" "/san2/data/change-exploration/roleMerging/finalExperiments/newWikipediaRolesets/PROBABILISTIC_DECAY_FUNCTION_0.7_7_2016-05-07/" "/san2/data/change-exploration/roleMerging/finalExperiments/newWikipediaRolesets/PROBABILISTIC_DECAY_FUNCTION_0.7_7_2016-05-07/")
mkdir logsFCBRBDECAY/

# decay:

for i in "${!dsNames[@]}";
do
        dsName=${dsNames[i]}
        echo "Processing $dsName"
        datasource=${datasources[i]}
        rolesetPath=${rolesetPaths[i]}
        endDateTrainPhase=${endDateTrainPhases[i]}
        rolesetFile="${rolesetPath}/${dsName}.json"
        resultDirEdges="/san2/data/change-exploration/roleMerging/finalExperiments/finalGraphsDecay/$dsName/edges/"
        resultDirStats="/san2/data/change-exploration/roleMerging/finalExperiments/finalGraphsDecay/$dsName/stats/"
        resultDirTime="/san2/data/change-exploration/roleMerging/finalExperiments/finalGraphsDecay/$dsName/time/"
        start=$(date +%s)
        java -ea -Xmx64g -cp jar/DatasetVersioning-assembly-0.1.jar de.hpi.role_matching.cbrm.compatibility_graph.role_tree.CompatibilityGraphCreationMain $datasource $rolesetFile $resultDirEdges $resultDirStats $resultDirTime $endDateTrainPhase $nthreads $thresholdForFork $maxPairwiseListSizeForSingleThread $roleSamplingRate $timestampSamplingRate false false > logsFCBRBDECAY/$dsName.log 2>&1
        end=$(date +%s)
        runtime=$( echo "$end - $start" | bc -l )
        echo "Took $runtime seconds for $dsName"
        resultCount=$(wc -l $resultDirEdges/* | tail -n 1 | cut -d' ' -f3)
        echo "$dsName,$runtime,$resultCount"
done

