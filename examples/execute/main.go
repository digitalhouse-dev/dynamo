package main

import (
	"github.com/aws/aws-sdk-go/aws/credentials"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func main() {

	dyn := CreateDynamoDBClient("[Your region]")

}

func CreateDynamoDBClient(region string) (*dynamodb.DynamoDB, error) {
	conf := aws.NewConfig().WithRegion(region)

	dynamoAccessKey := "[Your ACCESS_KEY_ID]"
	dynamoSecretKey := "[Your SECRET_ACCESS_KEY]"
	if dynamoAccessKey != "" && dynamoSecretKey != "" {
		cred := credentials.NewStaticCredentials(dynamoAccessKey, dynamoSecretKey, "")
		conf = conf.WithCredentials(cred)
	}
	dynamoEndpoint := "[Your Dynamo Endpoint]"
	if dynamoEndpoint != "" {
		conf = conf.WithEndpoint(dynamoEndpoint)
	}

	sess, err := session.NewSession()
	if err != nil {
		return nil, err
	}
	svc := dynamodb.New(sess, conf)

	return svc, nil
}
