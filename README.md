docker logs <cointainer_name>
docker rm -f $(docker ps -aq)
docker system prune -a  --volumes


docker exec -it broker /bin/bash
docker ps
kafka-topics.sh --delete --topic <topic_name> --bootstrap-server localhost:9092
kafka-topics.sh --list --bootstrap-server localhost:9092
kafka-topics.sh --list --bootstrap-server localhost:9092
kafka-topics.sh --describe --topic valid_data_topic --bootstrap-server localhost:9092

kafka-console-consumer.sh \
  --topic valid_data_topic \
  --from-beginning \
  --bootstrap-server localhost:9092

docker exec schema-registry kafka-avro-console-consumer \
  --topic valid_data_topic \
  --from-beginning \
  --bootstrap-server kafka:9092 \
  --property schema.registry.url=http://schema-registry:8081

docker exec -it smart_city_kafka-spark-master-1 spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk:1.11.468 jobs/spark-city.py


mkdir -p ./services/kafka/data ./services/kafka/scripts ./services/kafka/logs
sudo chmod -R 777 ./services/kafka/data ./services/kafka/scripts ./services/kafka/logs

mkdir -p ./services/airflow/dags ./services/airflow/logs ./services/airflow/plugins ./services/airflow/config
sudo chmod -R 777 ./services/airflow/dags ./services/airflow/logs ./services/airflow/plugins ./services/airflow/config
echo -e "AIRFLOW_UID=$(id -u)" > .env


to reset the schema by deleting topic:
curl -X DELETE http://localhost:8081/subjects/api_data_topic-value
