package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func main() {
	//http://localhost:8080/Directory/FileName
	http.HandleFunc("/", uploadFileHandler())
	// http://localhost:8080/download-file/bucketName/FileName
	http.HandleFunc("/download-file/", download())
	log.Print("Server started on localhost:8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

const downloadPath = "temp/download/"

func uploadFileHandler() http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			renderError(w, "METHOD_NOT_ALLOWED", http.StatusInternalServerError)
			return
		}
		pathFile := r.URL.Path
		str := uploadFileInMinio(pathFile)
		respondWithJson(w, http.StatusOK, str)
	})
}

func respondWithJson(w http.ResponseWriter, code int, payload interface{}) {
	response, _ := json.Marshal(payload)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	w.Write(response)
}

func download() http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			renderError(w, "METHOD_NOT_ALLOWED", http.StatusInternalServerError)
			return
		}
		pathFile := r.URL.Path
		segments := strings.Split(pathFile, "/")
		fileName := segments[len(segments)-1]
		bucketName := segments[len(segments)-2]
		str, err := downloadFileFromMinio(w, fileName, bucketName)
		if err != nil {
			respondWithError(w, http.StatusBadRequest, fmt.Sprintf("%v", err))
			return
		}
		respondWithJson(w, http.StatusOK, str)
	})
}

func respondWithError(w http.ResponseWriter, code int, msg string) {
	respondWithJson(w, code, map[string]string{"error": msg})
}

func renderError(w http.ResponseWriter, message string, statusCode int) {
	w.WriteHeader(http.StatusBadRequest)
	w.Write([]byte(message))
}

func connectToMinio() (*minio.Client, error) {
	endpoint := "127.0.0.1:9000"
	accessKeyID := "minioadmin"
	secretAccessKey := "minioadmin"
	useSSL := false

	// Initialize minio client object.
	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
		Secure: useSSL,
	})
	return minioClient, err
}

func uploadFileInMinio(filePath string) string {
	ctx := context.Background()

	minioClient, err := connectToMinio()

	if err != nil {
		log.Fatalln(err)
	}
	// Make a new bucket called mymusic.
	bucketName := "mymusic"
	location := "us-east-1"

	err = minioClient.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{Region: location})
	if err != nil {
		// Check to see if we already own this bucket (which happens if you run this twice)
		exists, errBucketExists := minioClient.BucketExists(ctx, bucketName)
		if errBucketExists == nil && exists {
			log.Printf("We already own %s\n", bucketName)
		} else {
			log.Fatalln(err)
		}
	} else {
		log.Printf("Successfully created %s\n", bucketName)
	}
	//file path
	segments := strings.Split(filePath, "/")
	fileName := segments[len(segments)-1]
	//fullURLFile := strings.Replace(filePath, "/", "", 1)
	objectName := fileName
	//extension form fileName
	extensions := strings.Split(fileName, ".")
	extension := extensions[len(extensions)-1]
	contentType := "application/" + extension

	// Upload file
	info, err := minioClient.FPutObject(ctx, bucketName, objectName, filePath, minio.PutObjectOptions{ContentType: contentType})
	if err != nil {
		log.Fatalln(err)
	}

	return fmt.Sprintf("Successfully uploaded %s of size %d\n", objectName, info.Size)
}

func downloadFileFromMinio(w http.ResponseWriter, fileName, bucketName string) (string, error) {

	ctx := context.Background()
	minioClient, err := connectToMinio()
	if err != nil {
		log.Fatalln(err)
	}

	exists, errBucketExists := minioClient.BucketExists(ctx, bucketName)
	if errBucketExists == nil && exists {
		log.Printf("Bucket Exists : %s\n", bucketName)
	} else {
		return "Bucket doesn't exists", errBucketExists
	}

	reader, err := minioClient.GetObject(context.Background(), bucketName, fileName, minio.GetObjectOptions{})
	if err != nil {
		log.Println(err)
		return "", err
	}
	defer reader.Close()
	err = os.MkdirAll(downloadPath, os.ModePerm)
	if err != nil {
		respondWithError(w, http.StatusBadRequest, fmt.Sprintf(err.Error()))
	}
	fullPath := downloadPath + fileName
	localFile, err := os.Create(fullPath)
	if err != nil {
		log.Println(err)
		return "", err
	}
	defer localFile.Close()

	stat, err := reader.Stat()
	if err != nil {
		log.Println(err)
		return "", err
	}

	if _, err := io.CopyN(localFile, reader, stat.Size); err != nil {
		log.Println(err)
		return "", err
	}

	return "File Downloaded Successfully", nil
}
