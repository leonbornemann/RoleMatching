maxLen=256
nEpochs=3

tasknames=("education_PROBABILISTIC_DECAY_FUNCTION_0.7_7_2016-05-07_withIDAndScore" "military_PROBABILISTIC_DECAY_FUNCTION_0.7_7_2016-05-07_withIDAndScore" "politics_PROBABILISTIC_DECAY_FUNCTION_0.7_7_2016-05-07_withIDAndScore" "tv_and_film_PROBABILISTIC_DECAY_FUNCTION_0.7_7_2016-05-07_withIDAndScore" "oregon_withIDAndScore" "chicago_withIDAndScore" "utah_withIDAndScore" "gov.maryland_withIDAndScore" "austintexas_withIDAndScore" "football_PROBABILISTIC_DECAY_FUNCTION_0.7_7_2016-05-07_withIDAndScore" )

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