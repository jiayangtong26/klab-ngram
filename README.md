# klab-ngram

python ngram_mrjob.py s3://klabatla/textfiles/ -r emr --no-output --output-dir='s3://klabtong/mrjobresult1/' --bootstrap-action="s3://elasticmapreduce/bootstrap-actions/configure-hadoop -M s3://klabtong/ngram/hadoop_config.xml" 1>log 2>&1 &
