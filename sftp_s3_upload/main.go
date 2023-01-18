package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"golang.org/x/crypto/ssh"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/pkg/sftp"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	awsV1 "github.com/aws/aws-sdk-go/aws"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/secretsmanager"
)

type MyEvent struct {
	Name string `json:"name"`
}

type Handlers interface {
	walkPathTree(client *sftp.Client, path string) []string
}

type handler struct {
	Handlers
}

type SFTPCredentials struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

var (
	wg         sync.WaitGroup
	myBucket   string = fmt.Sprintf("s3-storage-%s", os.Getenv("stage"))
	secretName string = os.Getenv("SFTP_SECRET_NAME")
)

const (
	region = "eu-west-1"
	host   = "example.com:22"
	path   = "/path"
)

func createSFTPClient(host string, username string, password string) (*sftp.Client, error) {
	addr := host
	config := &ssh.ClientConfig{
		User: username,
		Auth: []ssh.AuthMethod{
			ssh.Password(password),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         15 * time.Minute,
	}

	conn, err := ssh.Dial("tcp", addr, config)
	if err != nil {
		panic("Failed to dial: " + err.Error())
	}
	client, err := sftp.NewClient(
		conn,
		sftp.MaxConcurrentRequestsPerFile(1),
		sftp.UseConcurrentReads(false),
		sftp.UseConcurrentWrites(false),
	)
	if err != nil {
		panic("Failed to create client: " + err.Error())
	}

	return client, err
}

func getSecret(secretName string) (*secretsmanager.GetSecretValueOutput, error) {
	session, err := session.NewSession()
	if err != nil {
		log.Fatal(err)
		return nil, err
	}

	svc := secretsmanager.New(session, awsV1.NewConfig().WithRegion(region))
	input := &secretsmanager.GetSecretValueInput{
		SecretId:     aws.String(secretName),
		VersionStage: aws.String("AWSCURRENT"),
	}

	result, err := svc.GetSecretValue(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case secretsmanager.ErrCodeResourceNotFoundException:
				fmt.Println(secretsmanager.ErrCodeResourceNotFoundException, aerr.Error())
			case secretsmanager.ErrCodeInvalidParameterException:
				fmt.Println(secretsmanager.ErrCodeInvalidParameterException, aerr.Error())
			case secretsmanager.ErrCodeInvalidRequestException:
				fmt.Println(secretsmanager.ErrCodeInvalidRequestException, aerr.Error())
			case secretsmanager.ErrCodeDecryptionFailure:
				fmt.Println(secretsmanager.ErrCodeDecryptionFailure, aerr.Error())
			case secretsmanager.ErrCodeInternalServiceError:
				fmt.Println(secretsmanager.ErrCodeInternalServiceError, aerr.Error())
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			fmt.Println(err.Error())
		}
		return nil, err
	}

	return result, nil
}

func (h *handler) walkPathTree(client *sftp.Client, path string) []string {
	var paths []string
	w := client.Walk(path)
	for w.Step() {
		if w.Err() != nil {
			continue
		}
		node := w.Stat()

		if !node.IsDir() {
			paths = append(paths, w.Path())
		}
	}

	fmt.Println(paths)
	return paths
}

func fileHandler(client *sftp.Client, uploader *manager.Uploader, path string) {
	file, err := client.Open(path)
	if err != nil {
		log.Println("Failed opening file", path, err)
		return
	}
	defer file.Close()

	importDate := time.Now().Format("2006-01-02")

	result, err := uploader.Upload(context.TODO(), &s3.PutObjectInput{
		Bucket: &myBucket,
		Key:    aws.String("new-bucket/" + importDate + path),
		Body:   file,
	})
	if err != nil {
		log.Fatalln("Failed to upload", path, err)
	}
	log.Println("Uploaded", path, result.Location)
}

func HandleRequest(ctx context.Context, name MyEvent) {
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(region),
	)
	if err != nil {
		log.Fatal(err)
	}

	// Get secret key/value from SSM
	secretOutput, err := getSecret(secretName)
	if err != nil {
		log.Fatal(err)
	}

	bytes := []byte(*secretOutput.SecretString)
	var credentials SFTPCredentials
	err = json.Unmarshal(bytes, &credentials)
	if err != nil {
		log.Fatal(err)
	}

	client, err := createSFTPClient(host, credentials.Username, credentials.Password)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	handler := &handler{}
	paths := handler.walkPathTree(client, path)
	uploader := manager.NewUploader(s3.NewFromConfig(cfg))

	wg.Add(len(paths))
	for _, path := range paths {
		go func(path string) {
			defer wg.Done()
			fileHandler(client, uploader, path)
		}(path)
	}
	wg.Wait()
}

func main() {
	lambda.Start(HandleRequest)
}
