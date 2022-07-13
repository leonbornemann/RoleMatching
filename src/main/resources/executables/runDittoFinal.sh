maxLen=256
nEpochs=3

tasknames=("education_final" "military_final" "politics_final" "tv_and_film_final" "football_final" )

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
