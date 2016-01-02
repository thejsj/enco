package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"time"

	"github.com/joho/godotenv"
	"github.com/mitchellh/goamz/aws"
	"github.com/mitchellh/goamz/s3"
	"github.com/streadway/amqp"
	"github.com/thejsj/veenco/worker/image-converter"
)

type ImageConverationPayloadJob struct {
	Name string `json:"name"`
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func convertImage(imageFilename string, s3bucket *s3.Bucket) (err error) {

	pwd, _ := os.Getwd()
	filenameForFile := pwd + "/" + imageFilename

	// Check if Video is already in HDD
	if _, err := os.Stat(filenameForFile); os.IsNotExist(err) {
		log.Printf("File not in memory. Starting Download: %s", filenameForFile)
		binary, err := s3bucket.Get(imageFilename)

		if err != nil {
			log.Fatalf("Error getting file (%s). Error: %s", imageFilename, err)
		}
		log.Printf("Done downloading (%s). Size: %s", imageFilename)
		log.Printf("Wrting file to: %s", filenameForFile)
		// Do we really need to write the file?
		ioerr := ioutil.WriteFile(filenameForFile, binary, 0644)
		if ioerr != nil {
			log.Fatal(ioerr)
		}
	}

	err = imageConverter.Resize(filenameForFile)
	if err != nil {
		log.Printf("Error converting video %v", err)
		return err
	}
	log.Printf("Image converted succesfully: %v")
	return nil
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
			time.Sleep(time.Duration(2) * time.Second)
			log.Printf("Received a message: %s", d.Body)

			var job ImageConverationPayloadJob
			err := json.Unmarshal([]byte(d.Body), &job)
			if err != nil {
				d.Nack(false, false)
				log.Printf("Error unmarshalling JSON: %s (%s)", err, d.Body)
			} else {
				log.Printf("Done")
				log.Printf("Start Converting Image: %v", job.Name)
				err := convertImage(job.Name, s3bucket)
				if err != nil {
					d.Nack(false, true)
					log.Printf("Error Converting Image: %v", job.Name)
				}
				d.Ack(false)
				log.Printf("Done Converting Image: %v", job.Name)
			}
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}
