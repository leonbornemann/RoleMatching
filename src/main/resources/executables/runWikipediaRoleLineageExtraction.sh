uuid=$(uuidgen)
inputZipFile=$1
granularityInDays=$2
mode=$3
minDecayProbability=$4
trainTimeEnd=$5
resultDir=/san2/data/change-exploration/wikipedia/infobox/finalExperiments/
workingDir=/san2/data/change-exploration/wikipedia/infobox/workingDir/
mkdir $resultDir
mkdir $workingDir

7zr e $inputZipFile -o$workingDir/$uuid/
fname=$(find $workingDir/$uuid/ -type f)
echo $fname
java -ea -Xmx64g -cp ../../code/CompatibilityBasedRoleMatching/target/scala-2.13/DatasetVersioning-assembly-0.1.jar de.hpi.wikipedia_data_preparation.WikipediaRoleLineageExtractionMain $fname $resultDir $granularityInDays $mode $minDecayProbability $trainTimeEnd
returnValue=$?
rm $fname
rm -r $workingDir/$uuid/
exit $returnValue

