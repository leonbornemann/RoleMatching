dsNames=("education" "military" "politics" "tv_and_film" "football")

nthreads="16"
thresholdForFork="30"
maxPairwiseListSizeForSingleThread="75"
roleSamplingRate="0.1"
timestampSamplingRate="0.1"

endDateTrainPhase="2016-05-07"

for i in "${!dsNames[@]}";
do
        dsName=${dsNames[i]}
        rolesetFile="/san2/data/change-exploration/roleMerging/finalExperiments/newWikipediaRolesets/PROBABILISTIC_DECAY_FUNCTION_0.7_7_2016-05-07/${dsName}.json"
        resultDirEdges="/san2/data/change-exploration/roleMerging/finalExperiments/wikipediaGraphGroups/$dsName/edges/"
        resultDirStats="/san2/data/change-exploration/roleMerging/finalExperiments/wikipediaGraphGroups/$dsName/stats/"
        resultDirTime="/san2/data/change-exploration/roleMerging/finalExperiments/wikipediaGraphGroups/$dsName/time/"
        java -ea -Xmx64g -cp ../../code/CompatibilityBasedRoleMatching/target/scala-2.13/DatasetVersioning-assembly-0.1.jar de.hpi.role_matching.cbrm.compatibility_graph.role_tree.CompatibilityGraphCreationMain wikipedia $rolesetFile $resultDirEdges $resultDirStats $resultDirTime $endDateTrainPhase $nthreads $thresholdForFork $maxPairwiseListSizeForSingleThread $roleSamplingRate $timestampSamplingRate true
done
