package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path"
	"time"
	"unicode/utf8"

	"code.google.com/p/go-uuid/uuid"

	r "github.com/dancannon/gorethink"
	"github.com/fatih/structs"
	"github.com/joho/godotenv"
	"github.com/julienschmidt/httprouter"
	"github.com/mitchellh/goamz/aws"
	"github.com/mitchellh/goamz/s3"
	"github.com/streadway/amqp"
)

var session *r.Session

type ImageEntry struct {
	Id               string    `gorethink:"id"`
	S3Filename       string    `gorethink:"s3Filename"`
	OriginalFileName string    `gorethink:"originalFileName,omitempty"`
	ContentType      string    `gorethink:"contentType,omitempty"`
	CreatedAt        time.Time `gorethink:"createAt,omitempty"`
}

// Transformation
type TransformationJob struct {
	JobType string                 `json:"jobType"`
	Data    map[string]interface{} `json:"data"`
}

type TransformationJobCollection struct {
	Transformations []TransformationJob `json:"transformations"`
}

// Jobs

type Job struct {
	Id      string `gorethink:"id"`
	ImageId string `gorethink:"imageId"`
	NextJob string `gorethink:"nextJob,omitempty"`
}

type ImageResizeToWidthPxJob struct {
	Job
	Width float64 `gorethink:"width"`
}

type ImageResizeToHeightPxJob struct {
	Height float64
}

type ImageResizeByPercentageJob struct {
	Percentage float64
}

type ImageCropByPercentageJob struct {
	Top    int
	Right  int
	Bottom int
	Left   int
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func IndexHandler(session *r.Session) func(writer http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	return func(writer http.ResponseWriter, req *http.Request, _ httprouter.Params) {
		log.Printf("Get IndexHandler")
		res, err := r.Table("images").Run(session)
		if err != nil {
			http.Error(writer, err.Error(), http.StatusInternalServerError)
			return
		}

		var rows []interface{}
		rResponseErr := res.All(&rows)
		if rResponseErr != nil {
			http.Error(writer, rResponseErr.Error(), http.StatusInternalServerError)
			return
		}

		jsonResponse, jsonMarshalErr := json.Marshal(rows)
		if jsonMarshalErr != nil {
			http.Error(writer, jsonMarshalErr.Error(), http.StatusInternalServerError)
			return
		}

		writer.Header().Set("Content-Type", "application/json")
		writer.Write(jsonResponse)
	}
}

func handleError(writer http.ResponseWriter, err error, message string) {
	if err != nil {
		errorMessage := ""
		if utf8.RuneCountInString(message) > 0 {
			errorMessage = fmt.Sprintf("%s : %s", message, err.Error())
		} else {
			errorMessage = err.Error()
		}
		http.Error(writer, errorMessage, http.StatusInternalServerError)
		return
	}
}

func ImagePostHandler(session *r.Session, s3bucket *s3.Bucket) func(writer http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	return func(writer http.ResponseWriter, req *http.Request, _ httprouter.Params) {
		log.Printf("POST ImagePostHandler")
		log.Printf("Content type", req.Header.Get("Content-Type"))

		req.ParseMultipartForm(32 << 20)
		fieldName := "fileUpload"
		file, fileHeader, formFileError := req.FormFile(fieldName)
		handleError(writer, formFileError, fmt.Sprintf("Error getting %s", fieldName))
		if file == nil {
			errMessage := fmt.Sprintf("`%s` field is required, but is currently empty", fieldName)
			http.Error(writer, errMessage, http.StatusInternalServerError)
			return
		}
		defer file.Close()

		uuid := uuid.New()
		extension := path.Ext(fileHeader.Filename)
		s3UploadFilename := uuid + extension
		buffer, err := ioutil.ReadAll(file)
		handleError(writer, err, "Error reading file")

		contentType := fileHeader.Header.Get("Content-Type")
		log.Printf("Content Type: %s / Filename: %s / Size: %v", contentType, fileHeader.Filename, binary.Size(buffer))
		s3PutErr := s3bucket.Put(s3UploadFilename, buffer, contentType, s3.Private)
		handleError(writer, s3PutErr, "Error uploading object to S3 bucket")

		newImage := ImageEntry{
			Id:               uuid,
			S3Filename:       s3UploadFilename,
			OriginalFileName: fileHeader.Filename,
			ContentType:      contentType,
			CreatedAt:        time.Now(),
		}
		reqlErr := r.Table("images").Insert(newImage).Exec(session)
		handleError(writer, reqlErr, "Error inserting image entry into database")

		log.Printf("Getting URL for object...")
		url := s3bucket.URL(s3UploadFilename)
		var responseMap = map[string]string{
			"id":                uuid,
			"s3-filename":       s3UploadFilename,
			"original-filename": fileHeader.Filename,
			"url":               url,
			"content-type":      contentType,
		}
		jsonResponse, jsonMarshalErr := json.Marshal(responseMap)
		handleError(writer, jsonMarshalErr, "Error Marshalling JSON")

		writer.Header().Set("Content-Type", "application/json")
		writer.Write([]byte(jsonResponse))
	}
}

func TransformationPostHandler(session *r.Session, s3bucket *s3.Bucket, rabbitMQChannel *amqp.Channel) func(writer http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	return func(writer http.ResponseWriter, req *http.Request, params httprouter.Params) {

		imageUuid := uuid.Parse(params.ByName("id"))
		if imageUuid == nil {
			errMessage := fmt.Sprintf("`%s` field is not a valid UUID", params.ByName("id"))
			http.Error(writer, errMessage, http.StatusInternalServerError)
			return
		}
		log.Printf("Querying for document: %s", imageUuid)
		cursor, cursorErr := r.Table("images").Get(imageUuid.String()).Run(session)
		if cursorErr == r.ErrEmptyResult {
			errMessage := fmt.Sprintf("No document with uuid `%s` could be found", imageUuid)
			http.Error(writer, errMessage, http.StatusInternalServerError)
			return
		}
		handleError(writer, cursorErr, "Error reading file")

		var imageEntry ImageEntry
		cursor.One(&imageEntry)
		defer cursor.Close()

		// Parse jobs in body
		body, ioErr := ioutil.ReadAll(req.Body)
		handleError(writer, ioErr, "Error reading body of request")
		var jobCollection TransformationJobCollection
		jsonUnmarshalErr := json.Unmarshal(body, &jobCollection)
		handleError(writer, jsonUnmarshalErr, "Error unmarshalling body into job collection")

		// Parse all jobs in job collection
		var validJobs []interface{}
		var invalidJobs []interface{}
		for _, job := range jobCollection.Transformations {
			if job.JobType == "resizeToWidthPx" {
				var validJob ImageResizeToWidthPxJob
				validJob.Job.Id = uuid.New()
				validJob.Job.ImageId = imageEntry.Id
				err := FillStruct(job.Data, &validJob)
				if err != nil {
					invalidJobs = append(invalidJobs, job.Data)
				} else {
					validJobs = append(validJobs, validJob.Job)
				}
			} else {
				invalidJobs = append(invalidJobs, job.Data)
			}
		}

		// Return error if there are any invalid jobs
		var response map[string][]interface{}
		if len(invalidJobs) > 0 {
			response = map[string][]interface{}{
				"invalidJobs": invalidJobs,
				"validJobs":   validJobs,
			}
		} else {
			response = map[string][]interface{}{
				"jobs": validJobs,
			}
		}

		// Add next jobs to struct
		for i, job := range validJobs {
			log.Printf("Valid Job %v %+v", i, job)
			log.Printf("Len %v", len(validJobs))
			log.Printf("Res %v", len(validJobs) != (i+1))
			if len(validJobs) != (i + 1) {
				nextJob := structs.New(validJobs[i+1])
				nextJobId := nextJob.Field("Id")
				nextJobIdValue := nextJobId.Value().(string)
				log.Printf("NextJobIdValue %v", nextJobIdValue)

				jobStruct := structs.New(job)
				nextJobField := jobStruct.Field("NextJob")
				nextJobField.Set(nextJobIdValue)
				log.Printf("Job %+v", job)
				log.Printf("Job- %+v", jobStruct)
				log.Printf(" --- END ---")
			}
		}

		// Add jobs to the db
		for _, job := range validJobs {
			reqlErr := r.Table("jobs").Insert(job).Exec(session)
			handleError(writer, reqlErr, "Error inserting image entry into database")
		}

		log.Printf("Parsing document into JSON response")
		jsonResponse, jsonMarshalErr := json.Marshal(response)
		handleError(writer, jsonMarshalErr, "Error Marshalling JSON")
		writer.Header().Set("Content-Type", "application/json")
		writer.Write([]byte(jsonResponse))
	}
}

func main() {
	log.Printf("Starting server...")

	log.Printf("Loading ENV Variables...")
	enverr := godotenv.Load()
	if enverr != nil {
		log.Fatal("Error loading .env file")
	}

	log.Printf("Connecting to RethinkDB (%s:%s) ...", os.Getenv("RETHINKDB_HOST"), os.Getenv("RETHINKDB_PORT"))
	session, err := r.Connect(r.ConnectOpts{
		Address:  os.Getenv("RETHINKDB_HOST") + ":" + os.Getenv("RETHINKDB_PORT"),
		Database: os.Getenv("DB_NAME"),
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	log.Printf("Connecting to AWS...")
	auth := aws.Auth{
		AccessKey: os.Getenv("AWS_ACCESS_KEY"),
		SecretKey: os.Getenv("AWS_SECRET_KEY"),
	}

	// Connect to S3
	connection := s3.New(auth, aws.USWest2)
	bucketName := os.Getenv("S3_BUCKET_NAME")
	log.Printf("Accessing Bucket: %s", bucketName)
	s3bucket := connection.Bucket(bucketName)
	s3bucket.PutBucket(s3.PublicReadWrite)

	// Connect to RabbitMQ
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	// Open Channel
	rabbitMQChannel, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer rabbitMQChannel.Close()

	err = rabbitMQChannel.ExchangeDeclare(
		"images", // name
		"direct", // type
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
	failOnError(err, "Failed to declare an exchange")

	log.Printf("Binding Router...")
	router := httprouter.New()
	router.GET("/", IndexHandler(session))
	router.POST("/image", ImagePostHandler(session, s3bucket))
	router.POST("/image/", ImagePostHandler(session, s3bucket))
	router.POST("/image/:id/transformation", TransformationPostHandler(session, s3bucket, rabbitMQChannel))
	router.POST("/image/:id/transformation/", TransformationPostHandler(session, s3bucket, rabbitMQChannel))

	log.Printf("HTTP Server listening on port: %s", os.Getenv("HTTP_PORT"))
	log.Fatal(http.ListenAndServe(":"+os.Getenv("HTTP_PORT"), router))
}
