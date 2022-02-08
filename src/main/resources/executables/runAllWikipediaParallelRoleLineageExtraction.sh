granularityInDays=7
mode="PROBABILISTIC_DECAY_FUNCTION;NO_DECAY"
minDecayProbability="0.3;0.7"
trainTimeEnd="2011-05-07;2017-04-29"
mkdir parallelLogs
mkdir logs
mkdir logs/WikipediaHistoryCreation/
rm logs/WikipediaHistoryCreation/*
rm parallelLogs/WikipediaHistoryCreation
find /san2/data/change-exploration/wikipedia/infobox/changesZipped/ -type f | parallel --joblog parallelLogs/WikipediaHistoryCreation --delay 1 --eta -j8 -v "bash /home/leon.bornemann/dataset_versioning/wikipedia/runWikipediaHistoryCreation.sh {} $granularityInDays $mode $minDecayProbability $trainTimeEnd > logs/WikipediaHistoryCreation/{/}.log 2>&1"
