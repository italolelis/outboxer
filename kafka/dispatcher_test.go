package kafka

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/golang/mock/gomock"
	"github.com/italolelis/outboxer"
)

const (
	testTopic   = "test_topic"
	testPayload = "test payload"
)

func TestSyncKafka_buildProducerMessage(t *testing.T) {
	type args struct {
		msg outboxer.OutboxMessage
	}

	tests := []struct {
		name    string
		args    args
		want    sarama.ProducerMessage
		wantErr bool
		ErrType error
	}{
		{
			name: "topic must be specified and should be non empty",
			args: struct{ msg outboxer.OutboxMessage }{msg: outboxer.OutboxMessage{
				ID:      1,
				Payload: []byte(testPayload),
			}},
			want:    sarama.ProducerMessage{},
			wantErr: true,
			ErrType: errKafkaOptionMandatory,
		},
		{
			name: "if partition specified then it should be used",
			args: struct{ msg outboxer.OutboxMessage }{msg: outboxer.OutboxMessage{
				ID:      1,
				Payload: []byte(testPayload),
				Options: outboxer.DynamicValues{
					Partition: int32(1),
					Topic:     testTopic,
				},
			}},
			want: sarama.ProducerMessage{
				Topic:     testTopic,
				Partition: int32(1),
				Value:     sarama.ByteEncoder(testPayload),
				Headers:   []sarama.RecordHeader{},
			},
			wantErr: false,
		},
		{
			name: "header values should be assigned correctly",
			args: struct{ msg outboxer.OutboxMessage }{msg: outboxer.OutboxMessage{
				ID:      1,
				Payload: []byte(testPayload),
				Options: outboxer.DynamicValues{
					Partition: int32(1),
					Topic:     testTopic,
				},
				Headers: map[string]interface{}{
					"key1": []byte("val1"),
				},
			}},
			want: sarama.ProducerMessage{
				Topic:     testTopic,
				Partition: int32(1),
				Value:     sarama.ByteEncoder(testPayload),
				Headers: []sarama.RecordHeader{
					{
						Key:   []byte("key1"),
						Value: []byte("val1"),
					},
				},
			},
			wantErr: false,
		},
		{
			name: "if partition not specified then ID should be used as key",
			args: struct{ msg outboxer.OutboxMessage }{msg: outboxer.OutboxMessage{
				ID:      int64(1),
				Payload: []byte(testPayload),
				Options: outboxer.DynamicValues{
					Topic: testTopic,
				},
			}},
			want: sarama.ProducerMessage{
				Topic:   testTopic,
				Key:     sarama.StringEncoder(strconv.FormatInt(int64(1), 10)),
				Value:   sarama.ByteEncoder(testPayload),
				Headers: []sarama.RecordHeader{},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			got, err := buildSaramaProducerMessage(tt.args.msg)
			if (err != nil) != tt.wantErr {
				t.Errorf("buildSaramaProducerMessage() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if tt.wantErr && !errors.Is(err, tt.ErrType) {
				t.Errorf("buildSaramaProducerMessage() error = %v, want %v", err, tt.ErrType)
			}

			if tt.wantErr {
				fmt.Println(err)
				return
			}

			tt.want.Timestamp = got.Timestamp
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("buildSaramaProducerMessage() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_checkProducerConfig(t *testing.T) {
	type args struct {
		cfg *sarama.Config
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name:    "should config for acks",
			args:    struct{ cfg *sarama.Config }{cfg: sarama.NewConfig()},
			wantErr: true,
		},
		{
			name: "should not return error for correct config",
			args: struct{ cfg *sarama.Config }{cfg: func() *sarama.Config {
				c := sarama.NewConfig()
				c.Producer.RequiredAcks = sarama.WaitForAll
				c.Producer.Return.Errors = true
				c.Producer.Return.Successes = true

				return c
			}()},
			wantErr: false,
		},
		{
			name: "should configure producer to return Error and Success",
			args: struct{ cfg *sarama.Config }{cfg: func() *sarama.Config {
				c := sarama.NewConfig()
				c.Producer.RequiredAcks = sarama.WaitForAll
				c.Producer.Return.Successes = false

				return c
			}()},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := checkProducerConfig(tt.args.cfg); (err != nil) != tt.wantErr {
				t.Errorf("checkProducerConfig() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
