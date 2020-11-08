Project is part of the curriculum of Udacity's data streaming course.


Uncompress the producer data file `police-department-calls-for-service.json.gz`.  
Run  `docker-compose up -d` to start up kafka,console producer and spark structured streaming.

In order to inspect the kafka-console-output from `kafka_server.py` simply
run `PYTHONPATH=./src python3 src/crime/consumer/consumer_server.py` from the projects directory.

The `spark-ui` can be accessed on `localhost:4040` in your browser. 


Alternatively, you can run:
`docker-compose exec kafka0 kafka-console-consumer --bootstrap-server localhost:9092 --topic crime.police.call [--from-beginning]`


### Q1: 
#### How did changing values on the SparkSession property parameters affect the throughput and latency of the data?

Analyzing spark-ui or the console report in particular `processedRowsPerSecond` and `inputRowsPerSecond`.
I didn't see much impact tweaking my local spark-driver/executor, however I would expect increasing 
`spark.executor.memory` in a multi-node setup to minimize data shuffling, and therefore increasing the `throughput`

### Q2:
#### 
As state above one should look for positive/negative impacts on `processedRowsPerSecond` and `inputRowsPerSecond`
in spark-ui/in the progress report.

You, can tweak the parallelism by setting `spark.default.parallelism` to the number of partitions of the topic.
As noted in the classroom increasing `spark.streaming.kafka.maxRatePerPartition` also might have a positive effect,
as it increases the maximum receiving rate.


Additionally, increasing `spark.executor.memory` should be very beneficial in a multi-node cluster setup since this 
can avoid data-shuffling between nodes, when some nodes reach their memory limits.