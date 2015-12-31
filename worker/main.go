package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/joho/godotenv"
	"github.com/mitchellh/goamz/aws"
	"github.com/mitchellh/goamz/s3"
	"github.com/streadway/amqp"
)

type VideoEncodingPayloadJob struct {
	Name string `json:"name"`
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func encodeVideo(videoFilename string, s3bucket *s3.Bucket) {

	pwd, _ := os.Getwd()
	filenameForFile := pwd + "/" + videoFilename

	// Check if Video is already in HDD
	if _, err := os.Stat(filenameForFile); os.IsNotExist(err) {
		log.Printf("File not in memory. Starting Download: %s", filenameForFile)
		videoBinary, err := s3bucket.Get(videoFilename)

		if err != nil {
			log.Fatalf("Error getting file (%s). Error: %s", videoFilename, err)
		}
		log.Printf("Done downloading (%s). Size: %s", videoFilename, binary.Size(videoBinary))
		log.Printf("Wrting file to: %s", filenameForFile)
		// Do we really need to write the file?
		ioerr := ioutil.WriteFile(filenameForFile, videoBinary, 0644)
		if ioerr != nil {
			log.Fatal(ioerr)
		}
	}

	res, err := ConvertVideo(filenameForFile)
	if err != nil {
		log.Fatalf("Error converting video %v", err)
	}
	log.Fatalf("Video converted succesfully: %v", res)
}

func main() {

	// Load env variables
	enverr := godotenv.Load()
	if enverr != nil {
		log.Fatal("Error loading .env file")
	}

	log.Printf("Connecting to AWS")
	auth := aws.Auth{
		AccessKey: os.Getenv("AWS_ACCESS_KEY"),
		SecretKey: os.Getenv("AWS_SECRET_KEY"),
	}
	region := aws.USWest2

	log.Printf("Accessing Bucket")
	connection := s3.New(auth, region)
	s3bucket := connection.Bucket("hiphipjorge-video-encoding")

	// Connect to RabbitMQ
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	// Open Channel
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	// Declare Queue
	task_queue, err := ch.QueueDeclare(
		"task_queue", // name
		true,         // durable
		false,        // delete when unused
		false,        // exclusive
		false,        // no-wait
		nil,          // arguments
	)
	failOnError(err, "Failed to declare a queue")

	err = ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	failOnError(err, "Failed to set QoS")

	msgs, err := ch.Consume(
		task_queue.Name, // queue
		"",              // consumer
		false,           // auto-ack
		false,           // exclusive
		false,           // no-local
		false,           // no-wait
		nil,             // args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			log.Printf("Received a message: %s", d.Body)
			d.Ack(false)

			var job VideoEncodingPayloadJob
			err := json.Unmarshal([]byte(d.Body), &job)
			if err != nil {
				log.Fatalf("Error unmarshalling JSON: %s (%s)", err, d.Body)
			} else {
				log.Printf("Done")
				log.Printf("Start Encoding Video: %v", job.Name)
				encodeVideo(job.Name, s3bucket)
			}
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}
