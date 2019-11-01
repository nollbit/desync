package desync

import (
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/minio/minio-go/pkg/credentials"
)

// S3Creds holds credentials or references to an S3 credentials file.
type S3Creds struct {
	AccessKey          string `json:"access-key,omitempty"`
	SecretKey          string `json:"secret-key,omitempty"`
	AwsCredentialsFile string `json:"aws-credentials-file,omitempty"`
	AwsProfile         string `json:"aws-profile,omitempty"`
	// Having an explicit aws region makes minio slightly faster because it avoids url parsing
	AwsRegion string `json:"aws-region,omitempty"`
}

// Config is used to hold the global tool configuration. It's used to customize
// store features and provide credentials where needed.
type Config struct {
	HTTPTimeout    time.Duration           `json:"http-timeout,omitempty"`
	HTTPErrorRetry int                     `json:"http-error-retry,omitempty"`
	S3Credentials  map[string]S3Creds      `json:"s3-credentials"`
	StoreOptions   map[string]StoreOptions `json:"store-options"`
}

// GetS3CredentialsFor attempts to find creds and region for an S3 location in the
// config and the environment (which takes precedence). Returns a minio credentials
// struct and region string. If not found, the creds struct will return "" when invoked.
// Uses the scheme, host and port which need to match what's in the config file.
func (c Config) GetS3CredentialsFor(u *url.URL) (*credentials.Credentials, string) {
	// See if creds are defined in the ENV, if so, they take precedence
	accessKey := os.Getenv("S3_ACCESS_KEY")
	region := os.Getenv("S3_REGION")
	secretKey := os.Getenv("S3_SECRET_KEY")
	if accessKey != "" || secretKey != "" {
		return NewStaticCredentials(accessKey, secretKey), region
	}

	// Look in the config to find a match for scheme+host
	key := &url.URL{
		Scheme: strings.TrimPrefix(u.Scheme, "s3+"),
		Host:   u.Host,
	}
	credsConfig := c.S3Credentials[key.String()]
	creds := NewStaticCredentials("", "")
	region = credsConfig.AwsRegion

	// if access access-key is present, it takes precedence
	if credsConfig.AccessKey != "" {
		creds = NewStaticCredentials(credsConfig.AccessKey, credsConfig.SecretKey)
	} else if credsConfig.AwsCredentialsFile != "" {
		creds = NewRefreshableSharedCredentials(credsConfig.AwsCredentialsFile, credsConfig.AwsProfile, time.Now)
	}
	return creds, region
}

// GetStoreOptionsFor returns optional config options for a specific store. Note that
// the location string in the config file needs to match exactly (watch for trailing /).
func (c Config) GetStoreOptionsFor(location string) StoreOptions {
	for k, v := range c.StoreOptions {
		if locationMatch(k, location) {
			return v
		}
	}
	return StoreOptions{}
}

var Cfg Config
