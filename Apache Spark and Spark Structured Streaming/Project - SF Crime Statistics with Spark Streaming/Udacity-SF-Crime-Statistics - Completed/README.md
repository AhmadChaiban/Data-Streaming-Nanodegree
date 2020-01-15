# Udacity-SF-Crime-Statistics

## Answers to Project Questions

1. How did changing values on the SparkSession property parameters affect the throughput and latency of the data?

    Answer: The processedRowsPerSecond parameter allows for increasing the number of rows that are being processed per second. This allows for higher Throughput. 

2. What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal?

    Answer: Through using the parameter processedRowsPerSecond to measure the how efficient the stream was, I manipulated the following three config parameters.

        i. spark.default.parallelism
        ii. spark.streaming.kafka.maxRatePerPartition
        iii. spark.sql.shuffle.partitions

    With spark.default.parallelism = 11000, spark.streaming.kafka.maxRatePerPartition = 15 and spark.sql.shuffle.partitions = 15, I was able to process up to 13.51 rows per second. When these values were changed to 15,000 for (i), 20 for (ii) and 20 for (iii), I was able to process up to 145.78754578754578 rows per second. These parameters seemed to be the best at making the stream more efficient. 