package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"strconv"
)

// DynamoStore is regional; a table has string hash key 'pk'. The bounded
// aggregate is for the proof only. Production stores steps/events separately.
type DynamoStore struct {
	Client *dynamodb.Client
	Table  string
}

func (s DynamoStore) Load(ctx context.Context, key string) (Session, error) {
	out, err := s.Client.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String(s.Table), Key: map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: key}}, ConsistentRead: aws.Bool(true)})
	if err != nil {
		return Session{}, err
	}
	if len(out.Item) == 0 {
		return Session{}, ErrNotFound
	}
	payload, ok := out.Item["payload"].(*types.AttributeValueMemberB)
	if !ok {
		return Session{}, fmt.Errorf("invalid stored session")
	}
	var session Session
	err = json.Unmarshal(payload.Value, &session)
	return session, err
}
func (s DynamoStore) CAS(ctx context.Context, key string, expected uint64, next Session) error {
	if next.Revision != expected+1 || key != next.Scope.Key() {
		return ErrConflict
	}
	payload, err := json.Marshal(next)
	if err != nil {
		return err
	}
	if len(payload) > 128*1024 {
		return fmt.Errorf("prototype record limit exceeded")
	}
	input := &dynamodb.PutItemInput{TableName: aws.String(s.Table), Item: map[string]types.AttributeValue{
		"pk": &types.AttributeValueMemberS{Value: key}, "revision": &types.AttributeValueMemberN{Value: strconv.FormatUint(next.Revision, 10)}, "payload": &types.AttributeValueMemberB{Value: payload},
	}}
	if expected == 0 {
		input.ConditionExpression = aws.String("attribute_not_exists(pk)")
	} else {
		input.ConditionExpression = aws.String("revision = :expected")
		input.ExpressionAttributeValues = map[string]types.AttributeValue{":expected": &types.AttributeValueMemberN{Value: strconv.FormatUint(expected, 10)}}
	}
	_, err = s.Client.PutItem(ctx, input)
	var conditional *types.ConditionalCheckFailedException
	if errors.As(err, &conditional) {
		return ErrConflict
	}
	return err
}
