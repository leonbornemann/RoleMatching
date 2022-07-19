{
  "name": "education_final",
  "task_type": "classification",
  "vocab": ["0", "1"],
  "trainset": "../../data/dittoExport/wikipedia/final_with_rm/education.json.txt_train.txt",
  "validset": "../../data/dittoExport/wikipedia/final_with_rm/education.json.txt_validation.txt",
  "testset": "../../data/dittoExport/wikipedia/final_with_rm/education.json.txt_test.txt"
},
{
"name": "politics_final_rm",
"task_type": "classification",
"vocab": ["0", "1"],
"trainset": "../../data/dittoExport/wikipedia/final_with_rm/politics.json.txt_train.txt",
"validset": "../../data/dittoExport/wikipedia/final_with_rm/politics.json.txt_validation.txt",
"testset": "../../data/dittoExport/wikipedia/final_with_rm/politics.json.txt_test.txt"
},
{
"name": "tv_and_film_final_rm",
"task_type": "classification",
"vocab": ["0", "1"],
"trainset": "../../data/dittoExport/wikipedia/final_with_rm/tv_and_film.json.txt_train.txt",
"validset": "../../data/dittoExport/wikipedia/final_with_rm/tv_and_film.json.txt_validation.txt",
"testset": "../../data/dittoExport/wikipedia/final_with_rm/tv_and_film.json.txt_test.txt"
},
{
"name": "military_final_rm",
"task_type": "classification",
"vocab": ["0", "1"],
"trainset": "../../data/dittoExport/wikipedia/final_with_rm/military.json.txt_train.txt",
"validset": "../../data/dittoExport/wikipedia/final_with_rm/military.json.txt_validation.txt",
"testset": "../../data/dittoExport/wikipedia/final_with_rm/military.json.txt_test.txt"
},
{
"name": "football_final_rm",
"task_type": "classification",
"vocab": ["0", "1"],
"trainset": "../../data/dittoExport/wikipedia/final_with_rm/football.json.txt_train.txt",
"validset": "../../data/dittoExport/wikipedia/final_with_rm/football.json.txt_validation.txt",
"testset": "../../data/dittoExport/wikipedia/final_with_rm/football.json.txt_test.txt"
},


maxLen=256
nEpochs=20


for i in "${!tasknames[@]}";
do
        taskname=${tasknames[i]}
        echo "Processing $taskname"
        start=$(date +%s)
        CUDA_VISIBLE_DEVICES=0 python train_ditto.py   --task $taskname  --batch_size 64   --max_len $maxLen   --lr 3e-5   --n_epochs $nEpochs   --finetuning   --lm roberta   --fp16 --save_model --summarize --dk general --da drop_col
        end=$(date +%s)
        runtime=$( echo "$end - $start" | bc -l )
        echo "Took $runtime seconds for $taskname"
done

maxLen=256
tasknames=("education_final_rm" "military_final_rm" "politics_final_rm" "tv_and_film_final_rm" "football_final_rm" )
inputFileNames=("education" "military" "politics" "tv_and_film" "football" )

for i in "${!tasknames[@]}";
do
        taskname=${tasknames[i]}
        inputFileName=${inputFileNames[i]}
        echo "$taskname $inputFileName"
        echo "CUDA_VISIBLE_DEVICES=0 python matcher.py --task $taskname  --input_path ../../data/dittoExport/wikipedia/final_with_rm/${inputFileName}.json.txt_test.txt  --output_path output/${taskname}_result.json  --lm roberta  --max_len $maxLen  --use_gpu  --fp16  --summarize  --dk general --checkpoint_path checkpoints/"
done

#maxLen=256
#tasknames=("education_final" "military_final" "politics_final" "tv_and_film_final" "football_final" )
#inputFileNames=("education" "military" "politics" "tv_and_film" "football" )
