export MLFLOW_S3_ENDPOINT_URL=https://storage.yandexcloud.net
export AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID
export AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY
export AWS_BUCKET_NAME=$S3_BUCKET_NAME

mlflow server \
  --backend-store-uri postgresql://mle_20250822_5fbea5500a_freetrack:9ad242eec151435baaffc8c8573a8d7b@rc1b-uh7kdmcx67eomesf.mdb.yandexcloud.net:6432/playground_mle_20250822_5fbea5500a\
    --default-artifact-root s3://s3-student-mle-20250822-5fbea5500a-freetrack \
    --no-serve-artifacts