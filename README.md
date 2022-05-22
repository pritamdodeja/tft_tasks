# tft_tasks

A task based approach to using tensorflow transform. 

Usage instructions:

```
python tft_tasks.py -h

Example: python taxi_pipeline_refactored.py --task transform_tfrecords --task train_and_predict_embedding_model

optional arguments:
  -h, --help    show this help message and exit
  --task TASKS  Pick tasks from {'train_and_predict_embedding_model', 'view_transformed_sample_data', 'transform_tfrecords',
                'write_raw_tfrecords', 'view_original_sample_data', 'train_non_embedding_model', 'clean_directory',
                'train_embedding_model'}

```

To execute the full lifecycle, enter the below:

```
python tft_tasks.py --task clean_directory --task write_raw_tfrecords --task view_original_sample_data --task transform_tfrecords --task view_transformed_sample_data --task train_non_embedding_model --task train_embedding_model --task clean_directory
```
