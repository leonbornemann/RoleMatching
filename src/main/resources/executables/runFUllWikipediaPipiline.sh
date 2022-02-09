#bash runAllWikipediaRoleLineageExtraction.sh
#echo "Done with weekly extraction"
configs=("NO_DECAY_7_2011-05-07"  "PROBABILISTIC_DECAY_FUNCTION_0.3_7_2011-05-07"  "PROBABILISTIC_DECAY_FUNCTION_0.7_7_2011-05-07" "NO_DECAY_7_2017-04-29"  "PROBABILISTIC_DECAY_FUNCTION_0.3_7_2017-04-29"  "PROBABILISTIC_DECAY_FUNCTION_0.7_7_2017-04-29")
trainTimeEnds=("2011-05-07" "2011-05-07" "2011-05-07" "2017-04-29" "2017-04-29" "2017-04-29")
timestampGranularities=("7" "7" "7" "7" "7" "7")

datasetNames=("military" "education" "tv_and_film" "football" "politics" )
militaryTemplates="infobox military person,infobox military conflict,infobox military unit,infobox weapon"
educationTemplates="infobox book,infobox school,infobox scientist,infobox university,infobox_university,infobox secondary school,infobox uk school"
tvTemplateString="infobox television,infobox film,infobox_film,infobox actor,infobox character,infobox television episode,infobox tv channel,infobox soap character,infobox_movie"
footballTemplateString="infobox football biography,football player infobox,infobox football biography 2,football club infobox,infobox football club season,infobox football club,infobox football league season,infobox stadium,infobox college football player"
politicsTemplateString="infobox officeholder,infobox election,infobox politician,infobox_politician,infobox political party,infobox monarch,infobox_president"

nthreads="16"
thresholdForFork="30"
maxPairwiseListSizeForSingleThread="75"
roleSamplingRate="0.1"
timestampSamplingRate="0.1"

templateStrings=("$militaryTemplates" "$educationTemplates" "$tvTemplateString" "$footballTemplateString" "$politicsTemplateString" )

for i in "${!configs[@]}";
do
        config=${configs[i]}
        timestampGranularity=${timestampGranularities[i]}
        trainTimeEnd=${trainTimeEnds[i]}
        echo "Running $config"
        templateDir="/san2/data/change-exploration/wikipedia/infobox/finalExperiments/${config}/byTemplate/"
        inputDir="/san2/data/change-exploration/wikipedia/infobox/finalExperiments/${config}/byTemplate/wikipediaLineages/"
        mkdir templateDir
        java -ea -Xmx64g -cp ../../code/CompatibilityBasedRoleMatching/target/scala-2.13/DatasetVersioning-assembly-0.1.jar de.hpi.wikipedia_data_preparation.IndexByTemplateMain top100Templates.txt $inputDir $templateDir
        echo "Done with Template Indexing"
        for j in "${!templateStrings[@]}";
        do
          templateString=${templateStrings[i]}
          datasetName=${datasetNames[i]}
          resultDir="/san2/data/change-exploration/roleMerging/finalExperiments/newWikipediaGraphs/$config/$datasetName"
          java -ea -Xmx64g -cp ../../code/CompatibilityBasedRoleMatching/target/scala-2.13/DatasetVersioning-assembly-0.1.jar de.hpi.wikipedia_data_preparation.compatiblity_graph.CompatibilityGraphByTemplateCreationMain $templateString $templateDir $resultDir $trainTimeEnd $timestampGranularity $nthreads $thresholdForFork $maxPairwiseListSizeForSingleThread $roleSamplingRate $timestampSamplingRate $datasetName
        done
done

#mkdir /san2/data/change-exploration/wikipedia/infobox/weeklyGracePeriod/mergeResults/
#java -ea -Xmx64g -cp ../code/SocrataChangeExploration/target/scala-2.13/DatasetVersioning-assembly-0.1.jar de.hpi.tfm.data.wikipedia.infobox.fact_merging.FactMergingByTemplateMain "infobox_president;infobox person;infobox military unit" /san2/data/change-exploration/wikipedia/infobox/weeklyGracePeriod/byTemplate/ /san2/data/change-exploration/wikipedia/infobox/weeklyGracePeriod/mergeResults/ 2011-05-05 7
