#input directories:
rolesetDir="<substitute the path to a diretory contained the role sets json files here>"
inputRoleMatchingDir="<substitute the path to a directory for the output here>"
inputRoleMatchingFromMDMCPDir="<substitute the path to a directory for the output here>"
vertexMappingDirMDMCP="<substitute the path to a directory for the output here>"
graphDir="<substitute the path to a directory for the output here>"
resultDir="<substitute the path to a directory for the output here>"
jarFile="<substitute the path to the jar file obtained by sbt assembly here>"
#further config
weightSettingSocrata="../weightSettings/socrata.json"
weightSettingWikipedia="../weightSettings/wikipedia.json"
weightSettings=( "$weightSettingSocrata" "$weightSettingSocrata" "$weightSettingSocrata" "$weightSettingSocrata" "$weightSettingSocrata" "$weightSettingWikipedia" "$weightSettingWikipedia" "$weightSettingWikipedia" "$weightSettingWikipedia" "$weightSettingWikipedia")
datasetNames=("austintexas" "chicago" "gov.maryland" "oregon" "utah" "education" "football" "military" "politics" "tv_and_film")
dataSources=("socrata" "socrata" "socrata" "socrata" "socrata" "wikipedia" "wikipedia" "wikipedia" "wikipedia" "wikipedia")
matchingEndTimes=("2020-04-30" "2020-04-30" "2020-04-30" "2020-04-30" "2020-04-30" "2011-05-07" "2011-05-07" "2011-05-07" "2011-05-07" "2011-05-07")

#settinng up logging:
mkdir logs/

for i in "${!datasetNames[@]}";
do
        datasetName=${datasetNames[i]}
        #setting up result directories:
        dataSource=${dataSources[i]}
        currentInputRoleMatchingDir="$inputRoleMatchingDir/$datasetName/"
        currentInputRoleMatchingDirFromMDMCP="$inputRoleMatchingFromMDMCPDir/$datasetName/"
        currentVertexMappingDir="$vertexMappingDirMDMCP/$datasetName/"
        graphFile="$graphDir/$datasetName.json/"
        matchingEndTime=${matchingEndTimes[i]}
        weightConfig=${weightSettings[i]}
        currentResultDir="$resultDir/$datasetName/"
        rolesetFile="$rolesetDir/$datasetName.json"
        mkdir currentResultDir
        logFile="logs/${datasetName}_evaluation.log"
        #starting the process
        echo "Running $datasetName with nThreads $nThreads"
        java -ea -Xmx96g -cp $jarFile de.hpi.role_matching.cbrm.compatibility_graph.role_tree.CompatibilityGraphCreationMain $dataSource $currentInputRoleMatchingDir $currentInputRoleMatchingDirFromMDMCP $currentVertexMappingDir $graphFile $matchingEndTime $weightConfig $currentResultDir $rolesetFile > $logFile 2<&1
        echo "Finished with exit code $?"
done