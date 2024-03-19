package sdk_s3

import (
	"context"
	"fmt"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3ConnectionManager contains different cluster information and connections to them.
type S3ConnectionManager struct {
	Clusters map[string]*S3ClusterObject
	lock     sync.Mutex
}

// ConfigS3ConnectionManager returns an instance of S3ConnectionManager.
func ConfigS3ConnectionManager() *S3ConnectionManager {

	return &S3ConnectionManager{
		Clusters: make(map[string]*S3ClusterObject),
		lock:     sync.Mutex{},
	}
}

// setS3ClusterObject maps the S3ClusterObject using connection string to *S3ConnectionManager
func (s3cm *S3ConnectionManager) setS3ClusterObject(clusterIdentifier string, c *S3ClusterObject) {
	s3cm.Clusters[clusterIdentifier] = c
}

// getS3ClusterObject returns S3ClusterObject if cluster is already setup.
// If not, then set up a S3ClusterObject using S3ClusterObject.
func (s3cm *S3ConnectionManager) getS3ClusterObject(awsAccessKey, awsSecretKey, awsSessionToken, awsRegion string,
	clusterConfig *S3ClusterConfig) (*S3ClusterObject, error) {

	clusterIdentifier := awsAccessKey

	_, ok := s3cm.Clusters[clusterIdentifier]
	if !ok {
		if err := ValidateAwsClusterConfig(awsAccessKey, awsSecretKey, awsSessionToken, awsRegion, clusterConfig); err != nil {
			return nil, err
		}
		cfg, err := config.LoadDefaultConfig(context.TODO(),
			config.WithCredentialsProvider(credentials.StaticCredentialsProvider{
				Value: aws.Credentials{
					AccessKeyID:     awsAccessKey,
					SecretAccessKey: awsSecretKey,
					//SessionToken:    awsSessionToken,
					Source: "Credentials received from user",
				},
			}),
			config.WithRegion(awsRegion),
		)
		if err != nil {
			fmt.Println("Error loading AWS SDK config:", err)
			return nil, err
		}
		s3Client := s3.NewFromConfig(cfg)

		c := &S3ClusterObject{
			S3ClusterClient: s3Client,
		}
		s3cm.setS3ClusterObject(clusterIdentifier, c)
	}
	return s3cm.Clusters[clusterIdentifier], nil
}

// Disconnect disconnects a particular AWS S3 Cluster
func (s3cm *S3ConnectionManager) Disconnect(connStr string) error {
	clusterIdentifier := connStr
	_, ok := s3cm.Clusters[clusterIdentifier]
	if ok {
		delete(s3cm.Clusters, clusterIdentifier)
	}
	return nil
}

// DisconnectAll disconnect all the AWS S3 Clusters used in a tasks.Request
func (s3cm *S3ConnectionManager) DisconnectAll() {
	defer s3cm.lock.Unlock()
	s3cm.lock.Lock()
	for cS, v := range s3cm.Clusters {
		if v.S3ClusterClient != nil {
			v.S3ClusterClient = nil
			delete(s3cm.Clusters, cS)
		}
		v = nil
	}
}

// GetS3Cluster return a *s3.Client which represents connection to a specific AWS S3 Cluster.
// It checks if there exists a S3ClusterObject then
func (s3cm *S3ConnectionManager) GetS3Cluster(awsAccessKey, awsSecretKey, awsSessionToken, awsRegion string, clusterConfig *S3ClusterConfig) (*s3.Client,
	error) {
	defer s3cm.lock.Unlock()
	s3cm.lock.Lock()
	cObj, err1 := s3cm.getS3ClusterObject(awsAccessKey, awsSecretKey, awsSessionToken, awsRegion, clusterConfig)
	if err1 != nil {
		return nil, err1
	}
	return cObj.S3ClusterClient, nil
}
