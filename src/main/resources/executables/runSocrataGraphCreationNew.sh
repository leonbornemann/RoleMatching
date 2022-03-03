dsNames=("oregon" "austintexas" "utah" "gov.maryland" "chicago")

nthreads="16"
thresholdForFork="30"
maxPairwiseListSizeForSingleThread="75"
roleSamplingRate="0.1"
timestampSamplingRate="0.1"

endDateTrainPhase="2020-04-30"

for i in "${!dsNames[@]}";
do
        dsName=${dsNames[i]}
        rolesetFile="/san2/data/change-exploration/roleMerging/finalExperiments/rolesetsFilteredSocrata/${dsName}.json"
        resultDirEdges="/san2/data/change-exploration/roleMerging/finalExperiments/newSocrataGraphs/$dsName/edges/"
        resultDirStats="/san2/data/change-exploration/roleMerging/finalExperiments/newSocrataGraphs/$dsName/stats/"
        resultDirTime="/san2/data/change-exploration/roleMerging/finalExperiments/newSocrataGraphs/$dsName/time/"
        java -ea -Xmx64g -cp ../../code/CompatibilityBasedRoleMatching/target/scala-2.13/DatasetVersioning-assembly-0.1.jar de.hpi.role_matching.cbrm.compatibility_graph.role_tree.CompatibilityGraphCreationMain socrata $rolesetFile $resultDirEdges $resultDirStats $resultDirTime $endDateTrainPhase $nthreads $thresholdForFork $maxPairwiseListSizeForSingleThread $roleSamplingRate $timestampSamplingRate
done