/usr/lib/spark/bin/spark-submit \
--master yarn-client \
--driver-memory 8g \
--executor-memory 80G  --executor-cores 5 \
--num-executors 20 \
  abridge.py \
  $@

