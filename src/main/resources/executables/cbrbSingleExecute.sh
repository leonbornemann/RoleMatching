dsName=$1
nthreads=$2

thresholdForFork="30"
maxPairwiseListSizeForSingleThread="75"
roleSamplingRate="0.1"
timestampSamplingRate="0.1"
endDateTrainPhase="2016-05-07"

rolesetFile="/data/changedata/roleMerging/final_experiments/scalability_experimentResults/${dsName}"
resultDirEdges="/data/changedata/roleMerging/final_experiments/scalability_experimentResults/$dsName/edges/"
resultDirStats="/data/changedata/roleMerging/final_experiments/scalability_experimentResults/$dsName/stats/"
resultDirTime="/data/changedata/roleMerging/final_experiments/scalability_experimentResults/$dsName/time/"
java -ea -Xmx64g -cp ../../code/CompatibilityBasedRoleMatching/target/scala-2.13/DatasetVersioning-assembly-0.1.jar de.hpi.role_matching.blocking.cbrb.CBRBMain wikipedia,1.0 $rolesetFile $resultDirEdges $resultDirStats $resultDirTime $endDateTrainPhase $nthreads $thresholdForFork $maxPairwiseListSizeForSingleThread $roleSamplingRate $timestampSamplingRate false true
