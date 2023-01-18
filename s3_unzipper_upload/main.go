package main

import (
	"archive/zip"
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go/aws"
)

type MyEvent struct {
	Name string `json:"name"`
}

var (
	wg      sync.WaitGroup
	bucket  string = fmt.Sprintf("s3-storage-%s", os.Getenv("stage"))
	prefix  string = fmt.Sprintf("new-bucket/%s/path/", time.Now().Format("2006-01-02"))
	region  string = "eu-west-1"
	maxKeys int    = 1000
)

// Get object from S3 bucket
func getObject(client *s3.Client, bucket string, key string) (*s3.GetObjectOutput, error) {
	params := &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	}

	return client.GetObject(context.TODO(), params)
}

// Uploads unzipped file contents to S3
func uploadUnzippedFile(zipFile *zip.File, obj types.Object, uploader *manager.Uploader) {
	defer wg.Done()
	content, err := zipFile.Open()
	if err != nil {
		log.Fatal(err)
	}
	defer content.Close()

	unzippedFolderName := strings.Replace(*obj.Key, fmt.Sprintf("new-bucket/%s/path/", time.Now().Format("2006-01-02")), "", -1)
	unzippedFolderName = strings.Replace(unzippedFolderName, ".zip", "", -1)

	unzipDate := time.Now().Format("2006-01-02")
	unzippedFolderLocation := "new-bucket/unzipped"

	result, err := uploader.Upload(context.TODO(), &s3.PutObjectInput{
		Bucket: &bucket,
		Key:    aws.String(fmt.Sprintf("%s/%s/%s/%s", unzippedFolderLocation, unzipDate, unzippedFolderName, zipFile.Name)),
		Body:   content,
	})
	if err != nil {
		log.Fatalln("Failed to upload", zipFile.Name, err)
	}

	log.Printf("Uploaded file: %s, to: %s", zipFile.Name, result.Location)
}

func HandleRequest(ctx context.Context, name MyEvent) {
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(region),
	)
	if err != nil {
		panic("configuration error, " + err.Error())
	}

	client := s3.NewFromConfig(cfg)
	params := &s3.ListObjectsV2Input{
		Bucket: &bucket,
		Prefix: &prefix,
	}

	uploader := manager.NewUploader(s3.NewFromConfig(cfg))
	p := s3.NewListObjectsV2Paginator(client, params, func(o *s3.ListObjectsV2PaginatorOptions) {
		if v := int32(maxKeys); v != 0 {
			o.Limit = v
		}
	})

	var i int
	for p.HasMorePages() {
		i++
		page, err := p.NextPage(context.TODO())
		if err != nil {
			log.Fatalf("failed to get page %v, %v", i, err)
		}

		for _, obj := range page.Contents {
			wg.Add(1)
			go func(obj types.Object) {
				defer wg.Done()
				file, err := getObject(client, bucket, *obj.Key)
				if err != nil {
					log.Fatal(err)
				}
				defer file.Body.Close()

				body, err := ioutil.ReadAll(file.Body)
				if err != nil {
					log.Fatal(err)
				}

				zipReader, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
				if err != nil {
					log.Fatal(err)
				}

				// Read all the files from zip archive
				for _, zipFile := range zipReader.File {
					wg.Add(1)
					go uploadUnzippedFile(zipFile, obj, uploader)
				}
			}(obj)
		}
	}

	wg.Wait()
}

func main() {
	lambda.Start(HandleRequest)
}
