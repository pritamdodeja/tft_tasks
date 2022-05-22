# tft_tasks

A task based approach to using tensorflow transform. 

Usage instructions:

```
usage: tft_tasks.py [-h] --task TASKS

A task based approach to using tensorflow transform.

optional arguments:
  -h, --help    show this help message and exit
  --task TASKS  Pick tasks from {'train_embedding_model',
                'train_and_predict_embedding_model', 'view_original_sample_data',
                'write_raw_tfrecords', 'train_non_embedding_model',
                'view_transformed_sample_data', 'clean_directory',
                'transform_tfrecords'}
```

To execute the full lifecycle, enter the below:

```
python tft_tasks.py --task clean_directory --task write_raw_tfrecords --task view_original_sample_data --task transform_tfrecords --task view_transformed_sample_data --task train_non_embedding_model --task train_embedding_model --task clean_directory
```
