package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"
	"unicode/utf8"

	r "github.com/dancannon/gorethink"
	"github.com/joho/godotenv"
	"github.com/julienschmidt/httprouter"
	"github.com/mitchellh/goamz/aws"
	"github.com/mitchellh/goamz/s3"
)

var session *r.Session

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

type File interface {
	io.Reader
	io.ReaderAt
	io.Seeker
	io.Closer
}

func GetBufferFromFile(file File) (buffer *bytes.Buffer, err error) {
	reader := bufio.NewReader(file)
	buffer = bytes.NewBuffer(make([]byte, 0))

	var chunk []byte
	var eol bool

	for {
		if chunk, eol, err = reader.ReadLine(); err != nil {
			log.Printf("BREAK Error: %v / %v / %v", err, chunk, eol)
			break
		}
		log.Print("write chunk")
		buffer.Write(chunk)
		log.Printf("Buffer size #1: %v", len(buffer.String()))
		// if !eol {
		// log.Printf("eol, %v", len(buffer.String()))
		// str_array = append(str_array, buffer.String())
		// log.Printf("Buffer size #2: %v", len(buffer.String()))
		// buffer.Reset()
		// log.Printf("Buffer size #3: %v", len(buffer.String()))
		// }
	}

	log.Printf("Error: %v", err)
	log.Printf("Buffer size #4: %v", len(buffer.String()))
	if err == io.EOF {
		err = nil
	}
	return buffer, err
}

func ImagePostHandler(session *r.Session, s3bucket *s3.Bucket) func(writer http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	return func(writer http.ResponseWriter, req *http.Request, _ httprouter.Params) {
		log.Printf("POST ImagePostHandler")

		req.ParseMultipartForm(32 << 20)
		file, handler, formFileError := req.FormFile("imageUpload")
		handleError(writer, formFileError, "Error getting imageUpload")
		defer file.Close()

		s3UploadFilename := time.Now().Format(time.RFC850) + "-" + handler.Filename
		buffer, err := ioutil.ReadAll(file)
		handleError(writer, err, "Error reading file")

		contentType := "image/jpeg"
		log.Printf("Content Type: %s / Filename: %s / Size: %v", contentType, handler.Filename, binary.Size(buffer))
		s3PutErr := s3bucket.Put(s3UploadFilename, buffer, contentType, s3.Private)
		handleError(writer, s3PutErr, "Error uploading object to S3 bucket")

		log.Printf("Getting URL for object...")
		url := s3bucket.URL(handler.Filename)
		var responseMap = map[string]string{
			"filename":     handler.Filename,
			"url":          url,
			"content-type": contentType,
		}
		jsonResponse, jsonMarshalErr := json.Marshal(responseMap)
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
		Address: os.Getenv("RETHINKDB_HOST") + ":" + os.Getenv("RETHINKDB_PORT"),
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

	log.Printf("HTTP Server listening on port: %s", os.Getenv("HTTP_PORT"))
	log.Fatal(http.ListenAndServe(":"+os.Getenv("HTTP_PORT"), router))
}
