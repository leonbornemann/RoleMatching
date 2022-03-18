maxLen=256
nEpochs=3

echo "Processing education_PROBABILISTIC_DECAY_FUNCTION_0.7_7_2016-05-07"
start=$(date +%s)
CUDA_VISIBLE_DEVICES=0 python train_ditto.py   --task education_PROBABILISTIC_DECAY_FUNCTION_0.7_7_2016-05-07   --batch_size 64   --max_len $maxLen   --lr 3e-5   --n_epochs $nEpochs   --finetuning   --lm roberta   --fp16 --save_model --summarize --dk general --da drop_col
end=$(date +%s)
runtime=$( echo "$end - $start" | bc -l )
echo "Took $runtime seconds for education_PROBABILISTIC_DECAY_FUNCTION_0.7_7_2016-05-07"

echo "Processing military_PROBABILISTIC_DECAY_FUNCTION_0.7_7_2016-05-07"
start=$(date +%s)
CUDA_VISIBLE_DEVICES=0 python train_ditto.py   --task military_PROBABILISTIC_DECAY_FUNCTION_0.7_7_2016-05-07   --batch_size 64   --max_len $maxLen   --lr 3e-5   --n_epochs $nEpochs   --finetuning   --lm roberta   --fp16 --save_model --summarize --dk general --da drop_col
end=$(date +%s)
runtime=$( echo "$end - $start" | bc -l )
echo "Took $runtime seconds for military_PROBABILISTIC_DECAY_FUNCTION_0.7_7_2016-05-07"

echo "Processing politics_PROBABILISTIC_DECAY_FUNCTION_0.7_7_2016-05-07"
start=$(date +%s)
CUDA_VISIBLE_DEVICES=0 python train_ditto.py   --task politics_PROBABILISTIC_DECAY_FUNCTION_0.7_7_2016-05-07   --batch_size 64   --max_len $maxLen   --lr 3e-5   --n_epochs $nEpochs   --finetuning   --lm roberta   --fp16 --save_model --summarize --dk general --da drop_col
end=$(date +%s)
runtime=$( echo "$end - $start" | bc -l )
echo "Took $runtime seconds for politics_PROBABILISTIC_DECAY_FUNCTION_0.7_7_2016-05-07"

echo "Processing tv_and_film_PROBABILISTIC_DECAY_FUNCTION_0.7_7_2016-05-07"
start=$(date +%s)
CUDA_VISIBLE_DEVICES=0 python train_ditto.py   --task tv_and_film_PROBABILISTIC_DECAY_FUNCTION_0.7_7_2016-05-07   --batch_size 64   --max_len $maxLen   --lr 3e-5   --n_epochs $nEpochs   --finetuning   --lm roberta   --fp16 --save_model --summarize --dk general --da drop_col
end=$(date +%s)
runtime=$( echo "$end - $start" | bc -l )
echo "Took $runtime seconds for tv_and_film_PROBABILISTIC_DECAY_FUNCTION_0.7_7_2016-05-07"

echo "Processing football_PROBABILISTIC_DECAY_FUNCTION_0.7_7_2016-05-07"
start=$(date +%s)
CUDA_VISIBLE_DEVICES=0 python train_ditto.py   --task football_PROBABILISTIC_DECAY_FUNCTION_0.7_7_2016-05-07   --batch_size 64   --max_len $maxLen   --lr 3e-5   --n_epochs $nEpochs   --finetuning   --lm roberta   --fp16 --save_model --summarize --dk general --da drop_col
end=$(date +%s)
runtime=$( echo "$end - $start" | bc -l )
echo "Took $runtime seconds for football_PROBABILISTIC_DECAY_FUNCTION_0.7_7_2016-05-07"