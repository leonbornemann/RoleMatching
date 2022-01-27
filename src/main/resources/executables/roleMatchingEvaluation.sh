
#input directories:
rolesetDir="/san2/data/change-exploration/roleMerging/finalExperiments/rolesets/data/changedata/roleMerging/rolesetsTransformed/"
inputRoleMatchingDir="/san2/data/change-exploration/roleMerging/finalExperiments/optimizationResults/"
inputRoleMatchingFromMDMCPDir="/san2/data/change-exploration/roleMerging/finalExperiments/optimizationResults/MDMCPOutput/"
graphDir="/san2/data/change-exploration/roleMerging/finalExperiments/resultsFromTuning/data/changedata/roleMerging/tuningDataExport/"
resultDir="/san2/data/change-exploration/roleMerging/finalExperiments/finalResultFiles/"
jarFile="jar/DatasetVersioning-assembly-0.1.jar"

#TODO: generic input directories:
#rolesetDir="<substitute the path to a diretory contained the role sets json files here>"
#inputRoleMatchingDir="<substitute the path to a directory for the output here>"
#inputRoleMatchingFromMDMCPDir="<substitute the path to a directory for the output here>"
#graphDir="<substitute the path to a directory for the output here>"
#resultDir="<substitute the path to a directory for the output here>"
#jarFile="<substitute the path to the jar file obtained by sbt assembly here>"
#further config
weightSettingSocrata="../weightSettings/socrata.json"
weightSettingWikipedia="../weightSettings/wikipedia.json"
weightSettings=( "$weightSettingSocrata" "$weightSettingSocrata" "$weightSettingSocrata" "$weightSettingSocrata" "$weightSettingSocrata" "$weightSettingWikipedia" "$weightSettingWikipedia" "$weightSettingWikipedia" "$weightSettingWikipedia" "$weightSettingWikipedia")
datasetNames=("austintexas" "chicago" "gov.maryland" "oregon" "utah" "education" "football" "military" "politics" "tv_and_film")
dataSources=("socrata" "socrata" "socrata" "socrata" "socrata" "wikipedia" "wikipedia" "wikipedia" "wikipedia" "wikipedia")
matchingEndTimes=("2020-04-30" "2020-04-30" "2020-04-30" "2020-04-30" "2020-04-30" "2011-05-07" "2011-05-07" "2011-05-07" "2011-05-07" "2011-05-07")
configNames=( "alpha_3.1E-5" "alpha_3.1E-5" "alpha_3.1E-5" "alpha_3.1E-5" "alpha_3.1E-5" "alpha_5.18E-4" "alpha_5.18E-4" "alpha_5.18E-4" "alpha_5.18E-4" "alpha_5.18E-4")

#settinng up logging:
mkdir logs/

for i in "${!datasetNames[@]}";
do
        datasetName=${datasetNames[i]}
        #setting up result directories:
        dataSource=${dataSources[i]}
        curConfigName=${configNames[i]}
        matchingEndTime=${matchingEndTimes[i]}
        weightConfig=${weightSettings[i]}
        currentInputRoleMatchingDir="$inputRoleMatchingDir/$datasetName/sgcp/$curConfigName/role_merges/"
        currentInputRoleMatchingDirFromMDMCP="$inputRoleMatchingFromMDMCPDir/$datasetName/$curConfigName/"
        currentVertexMappingDir="$inputRoleMatchingDir/$datasetName/sgcp/$curConfigName/MDMCPVertexMapping/"
        graphFile="$graphDir/$datasetName/$datasetName.json/"
        currentResultDir="$resultDir/$datasetName/"
        rolesetFile="$rolesetDir/$datasetName.json"
        mkdir $currentResultDir
        logFile="logs/${datasetName}_evaluation.log"
        #starting the process
        echo "Running $datasetName"
        java -ea -Xmx128g -cp $jarFile de.hpi.role_matching.evaluation.matching.RoleMatchingEvaluationMain $dataSource $currentInputRoleMatchingDir $currentInputRoleMatchingDirFromMDMCP $currentVertexMappingDir $graphFile $matchingEndTime $weightConfig $currentResultDir $rolesetFile > $logFile 2<&1
        echo "Finished with exit code $?"
done