. ./.pws

export LOG_LEVEL=DEBUG
export LOG_FORMAT=

export DEBUGLEVEL=2

########################################################################
# Confluent Kafka Params
export kafka_bootstrap_port=9092
#export kafka_topic_name=SNDBX_TFM_events
export kafka_topic_name=SNDBX_TFM_engineResponse

export kafka_topic_offset=earliest

########################################################################
# Golang  Examples :
#
# https://github.com/confluentinc/confluent-kafka-go/blob/master/examples/consumer_example/consumer_example.go
# https://github.com/confluentinc/confluent-kafka-go/blob/master/examples/confluent_cloud_example/confluent_cloud_example.go
#
### Confluent Cloud Cluster
#export kafka_bootstrap_servers= -> See .pws
export kafka_security_protocol=SASL_SSL
export kafka_sasl_mechanisms=PLAIN
#export kafka_sasl_username= -> See .pws
#export kafka_sasl_password= -> See .pws
export kafka_parseduration=60s
export kafka_consumer_id=consumer_mongo
export kafka_enable_auto_commit=0
export kafka_commit_interval=100

########################################################################
# MongoDB Params
# Mongo -> Kafka -> MongoDB 
# https://blog.ldtalentwork.com/2020/05/26/how-to-sync-your-mongodb-databases-using-kafka-and-mongodb-kafka-connector/
#
#export mongo_url= -> See .pws
export mongo_port=27017
#export mongo_username= -> See .pws
#export mongo_password= -> See .pws
export mongo_datastore=cluster0
#export mongo_collection=paymentnrt                   
export mongo_collection=engineResponse             # Processed through engine

go run -v cmd/consumer.go
#./cmd/consumer

# https://docs.confluent.io/platform/current/app-development/kafkacat-usage.html

# kafkacat -b localhost:9092 -t SNDBX_AppLab
