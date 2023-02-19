/*
*
*	File		: consumer.go
*
* 	Created		: 8 May 2021
*				: 28 Jan 2023
*
*	Description	: Applab Demo, Consumes from a Kafka topc and drop JSON docs into a MongoDB & RedisDB
*				: as a example
*
*	Modified	: Original app simply only consumed and printed to terminal
*
*	By			: George Leonard (georgelza@gmail.com)
*
*	Partly		: https://github.com/sohamkamani/golang-kafka-example/blob/master/main.go
*
*				kubectl -n mongo port-forward service/mongo-nodeport-svc 27017
*
 */
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	// JSON Coloring
	"github.com/TylerBrock/colorjson"

	// Kafka
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"

	// MongoDB

	"go.mongodb.org/mongo-driver/bson/bsonrw"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"

	// Logging bits
	log "github.com/sirupsen/logrus"
	glog "google.golang.org/grpc/grpclog"
)

type tp_general struct {
	hostname   string
	debuglevel int
	loglevel   string
	logformat  string
}

// Kafka Environment values
type tp_kafka struct {
	bootstrapservers   string
	topicname          string
	security_protocol  string
	sasl_mechanisms    string
	sasl_username      string
	sasl_password      string
	consumer_id        string
	auto_offset_reset  string
	enable_auto_commit bool
	commit_interval    int
}

// MongoDB Environment values
type tp_mongodb struct {
	url        string
	port       string
	username   string
	password   string
	datastore  string
	collection string
}

var (
	grpcLog  glog.LoggerV2
	vGeneral tp_general
)

// init always gets called/first before main,
func init() {
	grpcLog = glog.NewLoggerV2(os.Stdout, os.Stdout, os.Stdout)

	// Playing around with Logrus.
	logLevel, err := log.ParseLevel(os.Getenv("LOG_LEVEL"))
	if err != nil {
		logLevel = log.InfoLevel
	}
	log.SetLevel(logLevel)

	log.SetOutput(os.Stdout)
	logFormat := os.Getenv("LOG_FORMAT")
	if logFormat == "JSON" { // if we want logs in JSON for consumption into ELK or SPLUNK, or any other NoSQL DB Store...
		log.SetFormatter(&log.JSONFormatter{})

	}

	fmt.Println("###############################################################")
	fmt.Println("#")
	fmt.Println("#   File      : consumer.go")
	fmt.Println("#")
	fmt.Println("#   Comment   : Applab Demo App")
	fmt.Println("#             : =>")
	fmt.Println("#             : Kafka Consumer - Confluent")
	fmt.Println("#             : MongoDB Client")
	fmt.Println("#")
	fmt.Println("#   By        : George Leonard (georgelza@gmail.com)")
	fmt.Println("#")
	fmt.Println("#   Date/Time :", time.Now().Format("2006-02-01 - 15:04:05"))
	fmt.Println("#")
	fmt.Println("###############################################################")
	fmt.Println("")
	fmt.Println("")

}

func loadGeneralProps() tp_general {

	// Lets identify ourself - helpful concept in a container environment.
	var err interface{}

	vHostname, err := os.Hostname()
	if err != nil {
		grpcLog.Error("Can't retrieve hostname", err)
	}
	vGeneral.hostname = vHostname

	vGeneral.logformat = os.Getenv("LOG_FORMAT")
	vGeneral.loglevel = os.Getenv("LOG_LEVEL")

	// Lets manage how much we print to the screen
	vGeneral.debuglevel, err = strconv.Atoi(os.Getenv("DEBUGLEVEL"))
	if err != nil {
		grpcLog.Error("String to Int convert error: %s", err)

	}

	grpcLog.Info("********************************************")
	grpcLog.Info("*          General Parameters              *")
	grpcLog.Info("*")
	grpcLog.Info("* Hostname is\t\t\t", vGeneral.hostname)
	grpcLog.Info("* Debug Level is\t\t", vGeneral.debuglevel)
	grpcLog.Info("* Log Level is\t\t", vGeneral.loglevel)
	grpcLog.Info("* Log Format is\t\t\t", vGeneral.logformat)
	grpcLog.Info("* ")
	grpcLog.Info("********************************************")
	grpcLog.Info("")

	return vGeneral
}

func loadKafkaProps() tp_kafka {

	var vKafka tp_kafka

	if vGeneral.debuglevel > 0 {
		grpcLog.Info("********************************************")
		grpcLog.Info("*      Kafka Connection Parameters         *")
		grpcLog.Info("*")

	}

	// Broker Configuration
	vKafka.bootstrapservers = fmt.Sprintf("%s:%s", os.Getenv("kafka_bootstrap_servers"), os.Getenv("kafka_bootstrap_port"))
	vKafka.topicname = os.Getenv("kafka_topic_name")

	vKafka.security_protocol = os.Getenv("kafka_security_protocol")
	vKafka.sasl_mechanisms = os.Getenv("kafka_sasl_mechanisms")
	vKafka.sasl_username = os.Getenv("kafka_sasl_username")
	vKafka.sasl_password = os.Getenv("kafka_sasl_password")
	vKafka.consumer_id = os.Getenv("kafka_consumer_id")
	vKafka.auto_offset_reset = os.Getenv("kafka_topic_offset")

	x, err := strconv.Atoi(os.Getenv("kafka_enable_auto_commit"))
	if err != nil {
		grpcLog.Error("2 String to Int convert error: %s", err)

	}

	if x == 0 {
		vKafka.enable_auto_commit = false
	} else {
		vKafka.enable_auto_commit = true
	}

	vKafka.commit_interval, err = strconv.Atoi(os.Getenv("kafka_commit_interval"))
	if err != nil {
		grpcLog.Error("2 String to Int convert error: %s", err)

	}

	if vGeneral.debuglevel > 0 {
		grpcLog.Info("* Kafka bootstrap Server is\t\t", vKafka.bootstrapservers)
		grpcLog.Info("* Kafka Topic is\t\t\t", vKafka.topicname)
		grpcLog.Info("* Kafka Security Protocl is\t\t", vKafka.security_protocol)
		grpcLog.Info("* Kafka Sasl Mechanism is\t\t", vKafka.sasl_mechanisms)
		grpcLog.Info("* Kafka Consumer Group ID is\t\t", vKafka.consumer_id)
		grpcLog.Info("* Kafka Consumer offset is\t\t", vKafka.auto_offset_reset)
		grpcLog.Info("* Kafka Auto Commit is\t\t", vKafka.enable_auto_commit)
		grpcLog.Info("* Kafka Commit Interval is\t\t", vKafka.commit_interval)
		grpcLog.Info("*")
		grpcLog.Info("********************************************")
		grpcLog.Info("")

	}

	return vKafka
}

func loadMongoProps() tp_mongodb {

	var vMongo tp_mongodb

	vMongo.url = os.Getenv("mongo_url")
	vMongo.port = os.Getenv("mongo_port")
	vMongo.username = os.Getenv("mongo_username")
	vMongo.password = os.Getenv("mongo_password")
	vMongo.datastore = os.Getenv("mongo_datastore")
	vMongo.collection = os.Getenv("mongo_collection")

	grpcLog.Infoln("****** MongoDB Connection Parameters *****")
	grpcLog.Info("* Mongo URL is\t\t", vMongo.url)
	grpcLog.Info("* Mongo Port is\t\t", vMongo.port)
	grpcLog.Info("* Mongo DataStore is\t\t", vMongo.datastore)
	grpcLog.Info("* Mongo Collection is\t\t", vMongo.collection)
	grpcLog.Info("* ")

	return vMongo
}

func prettyJSON(ms string) {

	var obj map[string]interface{}
	json.Unmarshal([]byte(ms), &obj)

	// Make a custom formatter with indent set
	f := colorjson.NewFormatter()
	f.Indent = 4

	// Marshall the Colorized JSON
	result, _ := f.Marshal(obj)
	fmt.Println(string(result))

}

func JsonToBson(message []byte) ([]byte, error) {
	reader, err := bsonrw.NewExtJSONValueReader(bytes.NewReader(message), true)
	if err != nil {
		return []byte{}, err
	}
	buf := &bytes.Buffer{}
	writer, _ := bsonrw.NewBSONValueWriter(buf)
	err = bsonrw.Copier{}.CopyDocument(writer, reader)
	if err != nil {
		return []byte{}, err
	}
	marshaled := buf.Bytes()
	return marshaled, nil
}

func main() {

	// Initialize the various environment fefining struct variables.
	loadGeneralProps() // Just a Banner Printer

	////////////////////////////////////////////////////////////
	// Initialize the Kafka Reader
	////////////////////////////////////////////////////////////
	// https://github.com/confluentinc/confluent-kafka-go

	vKafka := loadKafkaProps()

	grpcLog.Infoln("Kafka Creating Confluent Consumer")

	cm := kafka.ConfigMap{
		"bootstrap.servers":  vKafka.bootstrapservers,
		"sasl.mechanisms":    "PLAIN",
		"security.protocol":  "SASL_SSL",
		"sasl.username":      vKafka.sasl_username,
		"sasl.password":      vKafka.sasl_password,
		"session.timeout.ms": 6000,
		"group.id":           vKafka.consumer_id,
		"auto.offset.reset":  vKafka.auto_offset_reset,
		"enable.auto.commit": vKafka.enable_auto_commit,
	}
	grpcLog.Infoln("Kafka Confluent Consumer ConfigMap defined")

	krdr, err := kafka.NewConsumer(&cm)
	if err != nil {
		grpcLog.Errorln(os.Stderr, "Kafka Failed to Create consumer: %s\n", err)
		os.Exit(1)
	}
	grpcLog.Infoln("Kafka Created Confluent Consumer", krdr)

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	grpcLog.Infoln("Kafka Notification Channel created", krdr)

	topics := []string{vKafka.topicname}
	err = krdr.SubscribeTopics(topics, nil)
	if err != nil {
		grpcLog.Errorln(os.Stderr, "Kafka Failed Subscribe to topic: %s\n", err)
		os.Exit(1)
	}
	grpcLog.Infoln("Kafka Subscribed to Topic ", vKafka.topicname)

	defer krdr.Close()
	grpcLog.Infoln("Kafka Consumer Defer Closed Defined ")

	grpcLog.Infoln("")

	////////////////////////////////////////////////////////////
	// Intialise the MongoDB Connection
	////////////////////////////////////////////////////////////
	vMongo := loadMongoProps()

	uri := fmt.Sprintf("mongodb+srv://%s:%s@%s&w=majority", vMongo.username, vMongo.password, vMongo.url)
	grpcLog.Infoln("* MongoDB URI Constructed", uri)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	grpcLog.Infoln("* MongoDB Context Object Created")

	defer cancel()

	Mongoclient, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		grpcLog.Fatal("Mongo Connect Failed ", err)
		panic(err)
	}
	grpcLog.Infoln("* MongoDB Client Connected")

	defer func() {
		if err = Mongoclient.Disconnect(ctx); err != nil {
			grpcLog.Fatal("Mongo Disconected ", err)
			panic(err)
		}
	}()

	// Ping the primary
	if err := Mongoclient.Ping(ctx, readpref.Primary()); err != nil {
		grpcLog.Fatal("There was a error creating the Client object, Ping failed", err)
		panic(err)
	}
	grpcLog.Infoln("* MongoDB Client Pinged")

	// Create go routine to defer the closure
	defer func() {
		if err = Mongoclient.Disconnect(context.TODO()); err != nil {
			grpcLog.Fatal("Mongo Disconected ", err)
			panic(err)
		}
	}()

	// Define the Mongo Datastore
	appLabDatabase := Mongoclient.Database(vMongo.datastore)
	// Define the Mongo Collection Object
	demoCollection := appLabDatabase.Collection(vMongo.collection)

	grpcLog.Infoln("* MongoDB Datastore and Collection Intialized")
	grpcLog.Infoln("*")

	////////////////////////////////////////////////////////////
	// Lets start the loop
	////////////////////////////////////////////////////////////
	run := true
	msg_count := 0
	for run {

		select {
		case sig := <-sigchan:
			grpcLog.Fatalf("Caught signal %v: terminating\n", sig)
			run = false
			os.Exit(0)

		default:
			ev := krdr.Poll(100) // Poll the Cluster ever 100 miliseconds and see if there is anything new to process
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				// ------------- THIS IS WHERE WE WILL DO ALL PROCESSING !!!!!-------------

				// fmt.Printf("%% Message on %s:\n%s\n", e.TopicPartition, string(e.Value))

				// See the various bits of meta data thats included in the message object "e"
				// fmt.Printf("message at topic/partition/offset %v/%v/%v: %s = %s\n", e.Topic, e.Partition, e.Offset, string(e.Key), string(e.Value))

				// In this example, a synchronous commit is triggered every MIN_COMMIT_COUNT messages.
				// You could also trigger the commit on expiration of a timeout to ensure there the committed position is updated
				// regularly.
				msg_count += 1
				if msg_count%vKafka.commit_interval == 0 {
					krdr.Commit()
					if vGeneral.debuglevel > 0 {
						// When you run this file, it should print:
						// Document inserted with ID: ObjectID("...")
						grpcLog.Infoln("Kafka Commit(): ", msg_count)
					}
				}

				// Cast the string'y value that we got from m.value into a string interface object
				var obj map[string]interface{}
				json.Unmarshal([]byte(string(e.Value)), &obj)

				// Cast a byte string to BSon
				// https://stackoverflow.com/questions/39785289/how-to-marshal-json-string-to-bson-document-for-writing-to-mongodb
				// this way we don't need to care what the source structure is, it is all cast and inserted into the defined collection.
				doc, err := JsonToBson(e.Value)
				if err != nil {
					grpcLog.Errorln("Oops, we had a problem JsonToBson converting the payload, ", err)

				}

				// Time to get this into the MondoDB Collection
				result, err := demoCollection.InsertOne(context.TODO(), doc)
				if err != nil {
					grpcLog.Errorln("Oops, we had a problem inserting the document, ", err)

				}

				if vGeneral.debuglevel >= 2 {
					// When you run this file, it should print:
					// Document inserted with ID: ObjectID("...")
					grpcLog.Infoln("Mongo Document inserted with ID: ", result.InsertedID, "\n")

				} else if vGeneral.debuglevel > 2 {
					// prettyJSON takes a string which is actually JSON and makes it's pretty, and prints it.
					prettyJSON(string(e.Value))

				}

			case kafka.Error:
				// Errors should generally be considered
				// informational, the client will try to
				// automatically recover.
				// But in this example we choose to terminate
				// the application if all brokers are down.
				grpcLog.Errorln("Kafka Error: ", e.Code(), e)
				if e.Code() == kafka.ErrAllBrokersDown {
					run = false
				}

			default:
				grpcLog.Errorln("Kafka Ignored ", e)
			}
		}
	}
}
