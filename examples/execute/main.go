package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/digitalhouse-dev/dynamo/dynamo"
	"os"
	"time"
)

const dynamoAccessKey = "[Your access key]"
const dynamoSecretKey = "[Your secret key]"
const dynamoToken = "[Your token key]"
const dynamoEndpoint = "[Your dynamo endpoint]"
const awsRegion = "[Your aws region]"
const table = "[Your dynamo table]"

func main() {

	dyn, err := CreateDynamoDBClient(awsRegion)
	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}

	// Get a value
	{
		query := dynamo.Query(table, dynamo.QueryType, "", dyn)
		query = query.Criterial("resource_id", dynamo.Equals, "myResourceTest").Order(dynamo.ASC)
		res, key, err := query.Execute()
		fmt.Println(res, key, err)
	}

	// Save a value
	{
		t := time.Now()
		myDomain := struct {
			ResourceID string     `json:"resource_id"`
			CreatedAt  *time.Time `json:"created_at"`
			ID         string     `json:"id"`
			Note       string     `json:"note"`
			WrittenBy  string     `json:"written_by"`
		}{
			"myResourceTest", &t, "123", "Test", "ncostamagna@digitalhouse.com",
		}
		err := dynamo.Save(table, dyn).Entity(myDomain).Execute()
		fmt.Println(err)
	}
}

func CreateDynamoDBClient(region string) (*dynamodb.DynamoDB, error) {
	conf := aws.NewConfig().WithRegion(region)

	if dynamoAccessKey != "" && dynamoSecretKey != "" {
		cred := credentials.NewStaticCredentials(dynamoAccessKey, dynamoSecretKey, dynamoToken)
		conf = conf.WithCredentials(cred)
	}

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
