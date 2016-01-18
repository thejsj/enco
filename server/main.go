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
	"github.com/joho/godotenv"
	"github.com/julienschmidt/httprouter"
	"github.com/mitchellh/goamz/aws"
	"github.com/mitchellh/goamz/s3"
)

var session *r.Session

type ImageEntry struct {
	Id               string    `gorethink:"id,omitempty"`
	S3Filename       string    `gorethink:"s3Filename,omitempty"`
	OriginalFileName string    `gorethink:"originalFileName,omitempty"`
	ContentType      string    `gorethink:"contentType,omitempty"`
	CreatedAt        time.Time `gorethink:"createAt,omitempty"`
}

// Transformation
type TransformationJob struct {
	JobType string      `json:"jobType"`
	Data    interface{} `json:"data"`
}

type TransformationJobCollection struct {
	Transformations []TransformationJob `json:"transformations"`
}

// Jobs

type ImageResizeToWidthPxJob struct {
	Width int `json:"width"`
}

type ImageResizeToHeightPxJob struct {
	Height int
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

func IndexHandler(session *r.Session) func(writer http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	return func(writer http.ResponseWriter, req *http.Request, _ httprouter.Params) {
		log.Printf("Get IndexHandler")
		res, err := r.Expr([]interface{}{1, 2, 3, 4, 5}).Run(session)
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

func ConvertToStruct(obj interface{}, target interface{}) (err error) {
	jsonObj, jsonMarshalErr := json.Marshal(obj)
	if jsonMarshalErr != nil {
		return jsonMarshalErr
	}
	jsonUnmarshalErr := json.Unmarshal(jsonObj, &target)
	if jsonUnmarshalErr != nil {
		return jsonUnmarshalErr
	}
	return nil
}

func TransformationPostHandler(session *r.Session, s3bucket *s3.Bucket) func(writer http.ResponseWriter, req *http.Request, _ httprouter.Params) {
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
		for _, job := range jobCollection.Transformations {
			if job.JobType == "resizeToWidthPx" {
				var validJob ImageResizeToWidthPxJob
				conversionError := ConvertToStruct(job.Data, &validJob)
				if conversionError != nil {
					log.Printf("Conversion NOT succseful: ", validJob, conversionError, job.Data)
				}
				log.Printf("Conversion succseful: ", validJob)
			} else {
				log.Printf("Type not found: %v", job.JobType)
			}
		}

		log.Printf("Parsing document into JSON response")
		jsonResponse, jsonMarshalErr := json.Marshal(jobCollection)
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

	connection := s3.New(auth, aws.USWest2)
	bucketName := os.Getenv("S3_BUCKET_NAME")
	log.Printf("Accessing Bucket: %s", bucketName)
	s3bucket := connection.Bucket(bucketName)
	s3bucket.PutBucket(s3.PublicReadWrite)

	log.Printf("Binding Router...")
	router := httprouter.New()
	router.GET("/", IndexHandler(session))
	router.POST("/image", ImagePostHandler(session, s3bucket))
	router.POST("/image/", ImagePostHandler(session, s3bucket))
	router.POST("/image/:id/transformation", TransformationPostHandler(session, s3bucket))
	router.POST("/image/:id/transformation/", TransformationPostHandler(session, s3bucket))

	log.Printf("HTTP Server listening on port: %s", os.Getenv("HTTP_PORT"))
	log.Fatal(http.ListenAndServe(":"+os.Getenv("HTTP_PORT"), router))
}
