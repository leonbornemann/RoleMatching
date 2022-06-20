#wikipedia
targetRecall=77
mkdir ../logsRM_${targetRecall}
mkdir /data/changedata/roleMerging/final_experiments/rmResults/targetRecall_${targetRecall}/politics/
mkdir /data/changedata/roleMerging/final_experiments/rmResults/targetRecall_${targetRecall}/education/
mkdir /data/changedata/roleMerging/final_experiments/rmResults/targetRecall_${targetRecall}/military/
mkdir /data/changedata/roleMerging/final_experiments/rmResults/targetRecall_${targetRecall}/tv_and_film/
mkdir /data/changedata/roleMerging/final_experiments/rmResults/targetRecall_${targetRecall}/football/
go run Test.go /data/changedata/roleMerging/final_experiments/rmInput/target_recall_85/politics.json /data/changedata/roleMerging/final_experiments/rmInput/target_recall_85/politics.json /data/changedata/roleMerging/final_experiments/rmResults/targetRecall_${targetRecall}/politics/ 0.878049 "rm" > ../logsRM_${targetRecall}/politics.log 2>&1
go run Test.go /data/changedata/roleMerging/final_experiments/rmInput/target_recall_85/education.json /data/changedata/roleMerging/final_experiments/rmInput/target_recall_85/education.json /data/changedata/roleMerging/final_experiments/rmResults/targetRecall_${targetRecall}/education/ 0.878049 "rm" > ../logsRM_${targetRecall}/education.log 2>&1
go run Test.go /data/changedata/roleMerging/final_experiments/rmInput/target_recall_85/military.json /data/changedata/roleMerging/final_experiments/rmInput/target_recall_85/military.json /data/changedata/roleMerging/final_experiments/rmResults/targetRecall_${targetRecall}/military/ 0.878049 "rm" > ../logsRM_${targetRecall}/military.log 2>&1
go run Test.go /data/changedata/roleMerging/final_experiments/rmInput/target_recall_85/tv_and_film.json /data/changedata/roleMerging/final_experiments/rmInput/target_recall_85/tv_and_film.json /data/changedata/roleMerging/final_experiments/rmResults/targetRecall_${targetRecall}/tv_and_film/ 0.878049 "rm" > ../logsRM_${targetRecall}/tv_and_film.log 2>&1
go run Test.go /data/changedata/roleMerging/final_experiments/rmInput/target_recall_85/football.json /data/changedata/roleMerging/final_experiments/rmInput/target_recall_85/football.json /data/changedata/roleMerging/final_experiments/rmResults/targetRecall_${targetRecall}/football/ 0.878049 "rm" > ../logsRM_${targetRecall}/football.log 2>&1
