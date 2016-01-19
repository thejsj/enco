package main

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path"
	"reflect"
	"strings"
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
	JobType string                 `json:"jobType"`
	Data    map[string]interface{} `json:"data"`
}

type TransformationJobCollection struct {
	Transformations []TransformationJob `json:"transformations"`
}

// Jobs

type ImageResizeToWidthPxJob struct {
	Width float64 `json:"width,omitempty"`
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

func ConvertToStruct(obj map[string]interface{}, target interface{}) (err error) {
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

func SetField(obj interface{}, name string, value interface{}) error {
	// Convert to Capitalized
	name = strings.Title(name)
	structValue := reflect.ValueOf(obj).Elem()
	structFieldValue := structValue.FieldByName(name)

	if !structFieldValue.IsValid() {
		return fmt.Errorf("No such field: %s in obj", name)
	}

	if !structFieldValue.CanSet() {
		return fmt.Errorf("Cannot set %s field value", name)
	}

	structFieldType := structFieldValue.Type()
	val := reflect.ValueOf(value)
	if structFieldType != val.Type() {
		return errors.New("Provided value type didn't match obj field type")
	}

	structFieldValue.Set(val)
	return nil
}

func FillStruct(data map[string]interface{}, result interface{}) error {
	for key, value := range data {
		err := SetField(result, key, value)
		if err != nil {
			return err
		}
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
		var validJobs []interface{}
		var invalidJobs []interface{}
		for _, job := range jobCollection.Transformations {
			if job.JobType == "resizeToWidthPx" {
				var validJob ImageResizeToWidthPxJob
				err := FillStruct(job.Data, &validJob)
				if err != nil {
					invalidJobs = append(invalidJobs, job.Data)
				} else {
					validJobs = append(validJobs, validJob)
				}
			} else {
				invalidJobs = append(invalidJobs, job.Data)
			}
		}

		// Return error if there are any invalid jobs
		var response map[string][]interface{}
		if len(invalidJobs) > 0 {
			response = map[string][]interface{}{
				"validJobs":   validJobs,
				"invalidJobs": invalidJobs,
			}
		} else {
			response = map[string][]interface{}{
				"jobs": validJobs,
			}
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
