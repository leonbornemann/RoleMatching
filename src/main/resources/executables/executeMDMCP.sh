rootPathSGCP="<Replace this with the path to the optimization results root directory (output from sparseGraphCliquePartitioning.sh)>"
mdmcpOutputRootDir="$rootPathSGCP/MDMCPOutput/"
mkdir $mdmcpOutputRootDir
datasetNames=( "chicago" "austintexas" "gov.maryland"  "oregon"  "utah" "politics"  "military" "football" "tv_and_film" "education")
configNames=( "alpha_3.1E-5" "alpha_3.1E-5" "alpha_3.1E-5" "alpha_3.1E-5" "alpha_3.1E-5" "alpha_5.18E-4" "alpha_5.18E-4" "alpha_5.18E-4" "alpha_5.18E-4" "alpha_5.18E-4")
shellScriptDir="." #Replace this with the directory in which the runMDCMPWithMemLimitAndTimeout.sh is located

for i in "${!datasetNames[@]}";
do
   datasetName=${datasetNames[i]}
   configName=${configNames[i]}
   mdmcpInputFiles="$rootPathSGCP/$datasetName/sgcp/$configName/GeneratedMDMCPInputFiles/"
   outputDir="$mdmcpOutputRootDir/$datasetName/$configName/"
   mkdir "$mdmcpOutputRootDir/$datasetName/"
   mkdir $outputDir
   echo "Running $datasetName"
   logDir="logs/${datasetName}_mdmcp/$configName/"
   mkdir "logs/${datasetName}_mdmcp"
   mkdir $logDir
   mkdir parallelLogs/
   mkdir "parallelLogs/${datasetName}_mdmcp/"
   echo "Running MDMCP for input files $mdmcpInputFiles"
   find $mdmcpInputFiles -type f | parallel --delay 1 --eta -j12 -v --joblog "parallelLogs/${datasetName}_mdmcp/$configName.log" "bash $shellScriptDir/runMDCMPWithMemLimitAndTimeout.sh 6m {} $outputDir > $logDir/{/.}.log 2>&1"
   bash runMDMCPForDataset.bash $datasetName $configNameSocrata
done