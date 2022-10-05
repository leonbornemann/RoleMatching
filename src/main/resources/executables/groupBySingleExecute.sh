dsName=$1
groupingAlg=$2
nthreads=$3

endDateTrainPhase="2016-05-07"

rolesetFile="/data/changedata/roleMerging/final_experiments/scalability_experiments/${dsName}"
resultDirEdges="/data/changedata/roleMerging/final_experiments/scalability_experimentResults_grouping/$dsName/edges/"
java -ea -Xmx64g -cp DatasetVersioning-assembly-0.1.jar de.hpi.role_matching.blocking.group_by_blockers.RunGroupByBlocker $rolesetFile $groupingAlg $endDateTrainPhase $nthreads $resultDir