package dynamo

import (
	"fmt"
	"reflect"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

//TODO: move to a separate module?

var m *mock

type mock struct {
	result []map[string]*dynamodb.AttributeValue
	err    error
}

func AddMock(result []map[string]*dynamodb.AttributeValue, err error) {
	m = &mock{
		result: result,
		err:    err,
	}
}

func FlushMock() {
	m = nil
}

type Input interface {
	Criterial(key string, criterial CriterialType, value string) Input
	AddFilter(key string, criterial CriterialType, value string) Input
	Limit(limit *int) Input
	Order(order OrderType) Input
	ExclusiveStartKey(startKey interface{}) Input
	Execute() ([]map[string]*dynamodb.AttributeValue, map[string]*dynamodb.AttributeValue, error)
}

type InputType int

const (
	ScanType InputType = iota
	QueryType
	IndexType
)

type OrderType int

const (
	ASC OrderType = iota
	DESC
)

func Query(tableName string, input InputType, indexName string, db *dynamodb.DynamoDB) Input {
	switch input {
	case ScanType:
		return &scanInput{input: &dynamodb.ScanInput{
			TableName: aws.String(tableName),
		},
			db: db}
	case IndexType:
		return &queryInput{input: &dynamodb.QueryInput{
			TableName: aws.String(tableName),
			IndexName: aws.String(indexName),
		},
			db: db}
	case QueryType:
		return &queryInput{input: &dynamodb.QueryInput{
			TableName: aws.String(tableName),
		},
			db: db}
	}

	return nil
}

type CriterialType int

const (
	Equals CriterialType = iota
	Upper
	UpperEquals
	Lower
	LowerEquals
)

func (ct *CriterialType) criterial() string {
	switch *ct {
	case Equals:
		return "="
	case Upper:
		return ">"
	case UpperEquals:
		return ">="
	case Lower:
		return "<"
	case LowerEquals:
		return "<="
	}
	return ""
}

type queryInput struct {
	input *dynamodb.QueryInput
	db    *dynamodb.DynamoDB
}

func (qi *queryInput) Criterial(key string, c CriterialType, value string) Input {
	cVar := fmt.Sprintf(":%s", key)

	qi.input.ExpressionAttributeValues = map[string]*dynamodb.AttributeValue{
		cVar: {S: aws.String(value)},
	}
	qi.input.KeyConditionExpression = aws.String(fmt.Sprintf("%s %s %s", key, c.criterial(), cVar))

	return qi
}

func (qi *queryInput) AddFilter(key string, c CriterialType, value string) Input {

	// unique cVar
	cVar := fmt.Sprintf(":%s%d", key, len(qi.input.ExpressionAttributeValues))

	if len(qi.input.ExpressionAttributeValues) == 0 {
		qi.input.ExpressionAttributeValues = map[string]*dynamodb.AttributeValue{
			cVar: {S: aws.String(value)},
		}
	} else {
		qi.input.ExpressionAttributeValues[cVar] = &dynamodb.AttributeValue{S: aws.String(value)}
	}

	if qi.input.FilterExpression == nil {
		qi.input.FilterExpression = aws.String(fmt.Sprintf("%s %s %s", key, c.criterial(), cVar))
	} else {
		*qi.input.FilterExpression = *qi.input.FilterExpression + " AND " + *aws.String(fmt.Sprintf("%s %s %s", key, c.criterial(), cVar))
	}

	return qi
}

func (qi *queryInput) Limit(limit *int) Input {
	var limit64 int64
	if limit != nil {
		limit64 = int64(*limit)
	}
	qi.input.Limit = &limit64

	return qi
}

func (qi *queryInput) Order(order OrderType) Input {
	scanForward := order == ASC

	qi.input.ScanIndexForward = &scanForward
	return qi
}

func (qi *queryInput) ExclusiveStartKey(startKey interface{}) Input {
	startKeyMap, _ := dynamodbattribute.MarshalMap(startKey)

	qi.input.ExclusiveStartKey = startKeyMap
	return qi
}

func (qi *queryInput) Execute() ([]map[string]*dynamodb.AttributeValue, map[string]*dynamodb.AttributeValue, error) {
	if m != nil {
		if m.err != nil {
			return nil, nil, m.err
		}
		return m.result, nil, nil
	}

	result, err := qi.db.Query(qi.input)

	if err != nil {
		return nil, nil, err
	}

	return result.Items, result.LastEvaluatedKey, nil
}

type scanInput struct {
	input *dynamodb.ScanInput
	db    *dynamodb.DynamoDB
}

func (si *scanInput) Criterial(key string, c CriterialType, value string) Input {

	// unique cVar
	cVar := fmt.Sprintf(":%s%d", key, len(si.input.ExpressionAttributeValues))

	if len(si.input.ExpressionAttributeValues) == 0 {
		si.input.ExpressionAttributeValues = map[string]*dynamodb.AttributeValue{
			cVar: {S: aws.String(value)},
		}
	} else {
		si.input.ExpressionAttributeValues[cVar] = &dynamodb.AttributeValue{S: aws.String(value)}
	}

	if si.input.FilterExpression == nil {
		si.input.FilterExpression = aws.String(fmt.Sprintf("%s %s %s", key, c.criterial(), cVar))
	} else {
		*si.input.FilterExpression = *si.input.FilterExpression + " AND " + *aws.String(fmt.Sprintf("%s %s %s", key, c.criterial(), cVar))
	}

	return si
}

func (si *scanInput) AddFilter(key string, c CriterialType, value string) Input {
	return si.Criterial(key, c, value)
}

func (si *scanInput) Limit(limit *int) Input {
	var limit64 int64
	if limit != nil {
		limit64 = int64(*limit)
	}
	si.input.Limit = &limit64

	return si
}

func (si *scanInput) Order(order OrderType) Input {
	//TODO: Inform the interpreter user that ordering does not work in scans
	return si
}

func (si *scanInput) ExclusiveStartKey(startKey interface{}) Input {
	startKeyMap, _ := dynamodbattribute.MarshalMap(startKey)

	si.input.ExclusiveStartKey = startKeyMap
	return si
}

func (si *scanInput) Execute() ([]map[string]*dynamodb.AttributeValue, map[string]*dynamodb.AttributeValue, error) {

	if m != nil {
		if m.err != nil {
			return nil, nil, m.err
		}
		return m.result, nil, nil
	}

	result, err := si.db.Scan(si.input)

	if err != nil {
		return nil, nil, err
	}

	return result.Items, result.LastEvaluatedKey, nil
}

type SaveInput struct {
	input *dynamodb.PutItemInput
	db    *dynamodb.DynamoDB
}

func Save(tableName string, db *dynamodb.DynamoDB) *SaveInput {
	return &SaveInput{
		input: &dynamodb.PutItemInput{TableName: aws.String(tableName)},
		db:    db,
	}
}

func (si *SaveInput) Entity(entity interface{}) *SaveInput {

	// error no controlado
	entityAV, _ := dynamodbattribute.MarshalMap(entity)

	si.input.Item = entityAV

	return si
}

func (si *SaveInput) Execute() error {
	if m != nil {
		if m.err != nil {
			return m.err
		}
		return nil
	}

	_, err := si.db.PutItem(si.input)

	if err != nil {
		return err
	}
	return nil
}

type UpdateInput struct {
	input *dynamodb.UpdateItemInput
	db    *dynamodb.DynamoDB
}

func Update(tableName string, db *dynamodb.DynamoDB) *UpdateInput {

	return &UpdateInput{
		input: &dynamodb.UpdateItemInput{
			TableName:    aws.String(tableName),
			ReturnValues: aws.String("UPDATED_NEW")},
		db: db,
	}
}

func (ui *UpdateInput) Criterial(key string, value string) *UpdateInput {

	ui.input.Key = map[string]*dynamodb.AttributeValue{
		key: {S: aws.String(value)},
	}

	return ui
}

func (ui *UpdateInput) Value(key string, value interface{}) *UpdateInput {

	t := reflect.TypeOf(value)
	cVar := fmt.Sprintf(":%s", key)
	switch t.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		break
	case reflect.String:
		ui.input.ExpressionAttributeValues = map[string]*dynamodb.AttributeValue{cVar: {S: aws.String(fmt.Sprintf("%v", value))}}
	default:
		// error no controlado
		q, _ := dynamodbattribute.MarshalList(value)
		ui.input.ExpressionAttributeValues = map[string]*dynamodb.AttributeValue{cVar: {L: q}}

	}

	ui.input.UpdateExpression = aws.String(fmt.Sprintf("set %s = %s", key, cVar))

	return ui
}

func (ui *UpdateInput) NumberValue(key string, value int) *UpdateInput {
	cVar := fmt.Sprintf(":%s", key)

	ui.input.ExpressionAttributeValues = map[string]*dynamodb.AttributeValue{cVar: {N: aws.String(strconv.Itoa(value))}}

	ui.input.UpdateExpression = aws.String(fmt.Sprintf("set %s = %s", key, cVar))

	return ui
}

func (ui *UpdateInput) Execute() error {

	if m != nil {
		if m.err != nil {
			return m.err
		}
		return nil
	}

	_, err := ui.db.UpdateItem(ui.input)

	if err != nil {
		return err
	}
	return nil
}

type RemoveInput struct {
	input *dynamodb.DeleteItemInput
	db    *dynamodb.DynamoDB
}

func Remove(tableName string, db *dynamodb.DynamoDB) *RemoveInput {

	return &RemoveInput{
		input: &dynamodb.DeleteItemInput{
			TableName: aws.String(tableName),
		},
		db: db,
	}
}

func (ri *RemoveInput) Criterial(key string, value string) *RemoveInput {

	ri.input.Key = map[string]*dynamodb.AttributeValue{
		key: {S: aws.String(value)},
	}

	return ri
}

func (ri *RemoveInput) Execute() error {

	if m != nil {
		return m.err
	}

	if _, err := ri.db.DeleteItem(ri.input); err != nil {
		return err
	}
	return nil
}
