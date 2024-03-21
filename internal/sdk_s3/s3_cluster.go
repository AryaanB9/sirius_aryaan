package sdk_s3

import (
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3ConnectionOptions defines the Connection Options to AWS S3
type S3ConnectionOptions struct {
}

// S3ClusterConfig maintains the Config Settings for Connection to the AWS S3 Cluster
type S3ClusterConfig struct {
	AwsAccessKey        string              `json:"awsAccessKey,omitempty"`
	AwsSecretKey        string              `json:"awsSecretKey,omitempty"`
	AwsSessionToken     string              `json:"awsSessionToken,omitempty"`
	AwsRegion           string              `json:"awsRegion,omitempty"`
	S3ConnectionOptions S3ConnectionOptions `json:"s3ConnectionOptions,omitempty"`
}

type S3ClusterObject struct {
	S3ClusterClient *s3.Client `json:"-"`
}

func ValidateAwsClusterConfig(awsAccessKey, awsSecretKey, awsSessionToken, awsRegion string, awsS3ClusterConfig *S3ClusterConfig) error {
	if awsS3ClusterConfig == nil {
		awsS3ClusterConfig = &S3ClusterConfig{}
	}
	if awsAccessKey == "" {
		return fmt.Errorf("AWS Access Key is missing")
	}
	if awsSecretKey == "" {
		return fmt.Errorf("AWS Secret Key is missing")
	}
	if awsSessionToken == "" {
		return fmt.Errorf("AWS Session Token is missing")
	}
	if awsRegion == "" {
		return fmt.Errorf("AWS Region is missing")
	}
	return nil
}

// Close closes the AWS S3 Client connection.
func Close(s3ClusterObj *S3ClusterObject) error {
	s3ClusterObj.S3ClusterClient = nil
	return nil
}
