package secrets

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
)

// DBSecret mirrors the JSON structure stored in AWS Secrets Manager.
// Field names match the convention used by nxt-msa-commons GenericDataSourceConfig.
type DBSecret struct {
	Username string `json:"username"`
	Password string `json:"password"`
	Host     string `json:"host"`
	HostRO   string `json:"host_ro"`
	DBName   string `json:"dbname"`
	Port     int    `json:"port"`
}

// Fetcher retrieves and parses database credentials from AWS Secrets Manager.
type Fetcher struct {
	client     *secretsmanager.Client
	secretName string
}

// NewFetcher creates a Fetcher using the default AWS credential chain
// (IAM role → environment variables → shared credentials file).
// This matches the DefaultCredentialsProvider pattern used in GenericDataSourceConfig.
func NewFetcher(ctx context.Context, secretName string) (*Fetcher, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	return &Fetcher{
		client:     secretsmanager.NewFromConfig(cfg),
		secretName: secretName,
	}, nil
}

// Fetch retrieves and deserializes the DB credentials secret.
func (f *Fetcher) Fetch(ctx context.Context) (*DBSecret, error) {
	output, err := f.client.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{
		SecretId: aws.String(f.secretName),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get secret %q: %w", f.secretName, err)
	}

	var secret DBSecret
	if err := json.Unmarshal([]byte(*output.SecretString), &secret); err != nil {
		return nil, fmt.Errorf("failed to parse secret JSON: %w", err)
	}

	return &secret, nil
}
