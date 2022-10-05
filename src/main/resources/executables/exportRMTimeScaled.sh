sourceDir="/data/changedata/roleMerging/final_experiments/scalability_experiments_time_scaled/"
trainTimeEnds=("2016-05-07" "2029-09-08" "2043-01-10" "2056-05-13" "2069-09-14" "2083-01-16" "2096-05-19" "2109-09-21" "2123-01-23" "2136-05-26")
timeFactors=("1" "2" "3" "4" "5" "6" "7" "8" "9" "10")
basename="football.json_"

for i in "${!trainTimeEnds[@]}";
do
    trainTimeEnd=${trainTimeEnds[i]}
    timeFactor=${timeFactors[i]}
    java -ea -Xmx250g -cp DatasetVersioning-assembly-0.1.jar de.hpi.role_matching.blocking.rm.RoleDomainExportMain wikipedia $sourceDir/${basename}${timeFactor} $trainTimeEnd 1.0 /data/changedata/roleMerging/final_experiments/scalability_experiments_for_rm_time_scaled/ rm $timeFactor
done