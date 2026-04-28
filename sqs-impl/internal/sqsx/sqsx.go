// Package sqsx は SQS client の生成と queue URL 取得をまとめる。
package sqsx

import (
	"context"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

type ClientOpts struct {
	Endpoint  string // elasticmq の場合 http://localhost:9324
	Region    string
	AccessKey string
	SecretKey string
}

func DefaultOpts() ClientOpts {
	return ClientOpts{
		Endpoint:  envOr("SQS_ENDPOINT", "http://localhost:9324"),
		Region:    envOr("AWS_REGION", "us-east-1"),
		AccessKey: envOr("AWS_ACCESS_KEY_ID", "dummy"),
		SecretKey: envOr("AWS_SECRET_ACCESS_KEY", "dummy"),
	}
}

func New(ctx context.Context, opts ClientOpts) (*sqs.Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(opts.Region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(opts.AccessKey, opts.SecretKey, "")),
	)
	if err != nil {
		return nil, fmt.Errorf("aws config: %w", err)
	}
	return sqs.NewFromConfig(cfg, func(o *sqs.Options) {
		o.BaseEndpoint = aws.String(opts.Endpoint)
	}), nil
}

// QueueURL は queue 名から URL を解決する。
func QueueURL(ctx context.Context, c *sqs.Client, name string) (string, error) {
	out, err := c.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(name)})
	if err != nil {
		return "", fmt.Errorf("get queue url %s: %w", name, err)
	}
	return *out.QueueUrl, nil
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
