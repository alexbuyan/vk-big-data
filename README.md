### Producer

Run in producer in one CLI (Similar for every task)

```commandline
python hw3_flink/producer.py
```

## Task1

### Consumer

In other CLI first configure checkpoints

#### Local checkpoints

```commandline
docker-compose exec jobmanager ./bin/flink run -py /opt/pyflink/hw3_flink/device_job_local_checkpoints.py -d
```

#### HDFS checkpoints

Doesn't work, some internal config error

```commandline
docker-compose exec jobmanager ./bin/flink run -py /opt/pyflink/hw3_flink/device_job_hdfs_checkpoints.py -d
```

Then run consumer

```commandline
python hw3_flink/consumer_1.py
```

### Screenshots

Local - `hw3_flink/images/local_checkpoints.png`

# Next parts

For next tasks the process of running consumer is similar, but we just change the job and the consumer files. I provide
their names in the description to the task

## Task2
Jobs
- Tumbling - `/opt/pyflink/hw3_flink/device_job_local_tumbling.py`
- Sliding - `/opt/pyflink/hw3_flink/device_job_local_sliding.py`
- Session - `/opt/pyflink/hw3_flink/device_job_local_session.py`

Consumer - `hw3_flink/consumer_2.py`

## Task3
Job - `/opt/pyflink/hw3_flink/device_job_3.py`


Consumer - `hw3_flink/consumer_3.py`

### Kafka CheatSheet

```commandline
docker-compose build
```

```commandline
docker-compose up -d
```

```commandline
docker-compose ps
```

```
http://localhost:8081/#/overview
```

```commandline
docker-compose down -v
```

```commandline
docker-compose exec kafka kafka-topics.sh --bootstrap-server kafka:9092 --create --topic topic_name --partitions 1 --replication-factor 1
```

```commandline
docker-compose exec kafka kafka-topics.sh --bootstrap-server kafka:9092 --describe itmo  
```

```commandline
 docker-compose exec kafka kafka-topics.sh --bootstrap-server kafka:9092 --alter --topic itmo --partitions 2
```

```commandline
docker-compose exec jobmanager ./bin/flink run -py /opt/pyflink/device_job.py -d  
```