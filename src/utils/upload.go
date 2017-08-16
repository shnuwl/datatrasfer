package utils

import (
	"fmt"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/satori/go.uuid"
)

var sess *session.Session
var svc *s3.S3

func init() {
	sess, _ = session.NewSession(&aws.Config{
		Region:      aws.String("region"),
		Credentials: credentials.NewStaticCredentials("key_id", "access_key", ""),
	})
	svc = s3.New(sess, aws.NewConfig().
		WithRegion("region"),
	)

}

func Commit_to_s3(pending *map[string][]string) {

	// All clients require a Session. The Session provides the client with
	// shared configuration such as region, endpoint, and credentials. A
	// Session should be shared where possible to take advantage of
	// configuration and credential caching. See the session package for
	// more information.
	for key, value := range *pending {
		uid := uuid.NewV4()
		args := strings.Split(key, "/")
		version, stamp, ctg := args[0], args[1], args[2]
		stamp = stamp[:8] + "/" + stamp[8:12]
		key_name := "eden-wang/" + version + "/" + ctg + "-merged/" + stamp + "/" + uid.String() + ".gz"

		_, err := svc.PutObject(&s3.PutObjectInput{
			Bucket: aws.String("dev-wiwide"),
			Key:    aws.String(key_name),
			Body:   aws.ReadSeekCloser(strings.NewReader(strings.Join(value, "\n"))),
		})
		if err != nil {
			if aerr, ok := err.(awserr.Error); ok && aerr.Code() == request.CanceledErrorCode {
				// If the SDK can determine the request or retry delay was canceled
				// by a context the CanceledErrorCode error code will be returned.
				fmt.Fprintf(os.Stderr, "upload canceled due to timeout, %v\n", err)
			} else {
				fmt.Fprintf(os.Stderr, "failed to upload object, %v\n", err)
			}
			os.Exit(1)
		}

		fmt.Printf("successfully uploaded file to %s/%s\n", "dev-wiwide", key_name)

	}
}
