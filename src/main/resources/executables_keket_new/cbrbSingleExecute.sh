dsName=$1
timeFactor=$2
nthreads=$3
endDateTrainPhase=$4

thresholdForFork="30"
maxPairwiseListSizeForSingleThread="10"
roleSamplingRate="0.1"
timestampSamplingRate="0.1"

rolesetFile="scalability_experimentResults/${dsName}"
resultDirEdges="scalability_experimentResults/$dsName/edges/"
resultDirStats="scalability_experimentResults/$dsName/stats/"
resultDirTime="scalability_experimentResults/$dsName/time/"
java -ea -Xmx64g -cp DatasetVersioning-assembly-0.1.jar de.hpi.role_matching.blocking.cbrb.CBRBMain wikipedia,$timeFactor $rolesetFile $resultDirEdges $resultDirStats $resultDirTime $endDateTrainPhase $nthreads $thresholdForFork $maxPairwiseListSizeForSingleThread $roleSamplingRate $timestampSamplingRate false true
