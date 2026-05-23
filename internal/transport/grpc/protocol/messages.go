package protocol

import (
	"encoding/json"
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/transmogr/transmogr/internal/models"
	peerv1 "github.com/transmogr/transmogr/pkg/proto/transmogr/peerv1"
)

// ToProtoMessage converts an internal outbound stream message to a server-streamed
// protobuf envelope. Client-originated control messages are not encoded here.
func ToProtoMessage(message models.StreamMessage) *peerv1.ConsumeResponse {
	switch {
	case message.Handshake != nil:
		return &peerv1.ConsumeResponse{
			Msg: &peerv1.ConsumeResponse_Handshake{
				Handshake: &peerv1.Handshake{
					Region:        message.Handshake.Region,
					Version:       message.Handshake.Version,
					Configuration: message.Handshake.Configuration,
				},
			},
		}
	case message.SnapshotChunk != nil:
		var watermark *timestamppb.Timestamp
		if !message.SnapshotChunk.Watermark.IsZero() {
			watermark = timestamppb.New(message.SnapshotChunk.Watermark)
		}
		return &peerv1.ConsumeResponse{
			Msg: &peerv1.ConsumeResponse_SnapshotChunk{
				SnapshotChunk: &peerv1.SnapshotChunk{
					Table:     message.SnapshotChunk.Table,
					Rows:      toProtoSnapshotRows(message.SnapshotChunk.Rows),
					LastChunk: message.SnapshotChunk.Last,
					Watermark: watermark,
				},
			},
		}
	case message.SnapshotComplete != nil:
		return &peerv1.ConsumeResponse{
			Msg: &peerv1.ConsumeResponse_SnapshotComplete{
				SnapshotComplete: &peerv1.SnapshotComplete{
					Table: message.SnapshotComplete.Table,
				},
			},
		}
	case message.EventBatch != nil:
		return &peerv1.ConsumeResponse{
			Msg: &peerv1.ConsumeResponse_EventBatch{
				EventBatch: &peerv1.EventBatch{
					Sequence: message.EventBatch.Sequence,
					Events:   toProtoEvents(message.EventBatch.Events),
				},
			},
		}
	case message.TransactionBatch != nil:
		return &peerv1.ConsumeResponse{
			Msg: &peerv1.ConsumeResponse_TransactionBatch{
				TransactionBatch: &peerv1.TransactionBatch{
					TxId:     message.TransactionBatch.TxID,
					Sequence: message.TransactionBatch.Sequence,
					Events:   toProtoEvents(message.TransactionBatch.Events),
				},
			},
		}
	case message.Ping != nil:
		return NewHeartbeatMessage(message.Ping.SentAt)
	default:
		return nil
	}
}

// NewHandshakeMessage wraps a handshake in a protobuf replication envelope.
func NewHandshakeMessage(handshake models.HandshakeMessage) *peerv1.ConsumeResponse {
	return ToProtoMessage(models.StreamMessage{Handshake: &handshake})
}

// NewConsumeRequest builds the consume request for one remote peer session.
func NewConsumeRequest(
	handshake models.HandshakeMessage,
	snapshotRequests []models.SnapshotRequest,
) *peerv1.ConsumeRequest {
	protoRequests := make([]*peerv1.SnapshotRequest, 0, len(snapshotRequests))
	for _, request := range snapshotRequests {
		protoRequests = append(protoRequests, &peerv1.SnapshotRequest{
			Table:           request.Table,
			AfterPrimaryKey: toProtoPrimaryKey(request.AfterPrimaryKey),
		})
	}

	return &peerv1.ConsumeRequest{
		Region:           handshake.Region,
		Version:          handshake.Version,
		Configuration:    handshake.Configuration,
		SnapshotRequests: protoRequests,
	}
}

// SnapshotRequestsFromConsume extracts per-table snapshot requests from a consume request.
func SnapshotRequestsFromConsume(req *peerv1.ConsumeRequest) []models.SnapshotRequest {
	if req == nil {
		return nil
	}

	result := make([]models.SnapshotRequest, 0, len(req.GetSnapshotRequests()))
	for _, request := range req.GetSnapshotRequests() {
		afterPrimaryKey, err := fromProtoPrimaryKey(request.GetAfterPrimaryKey())
		if err != nil {
			afterPrimaryKey = nil
		}
		result = append(result, models.SnapshotRequest{
			Table:           request.GetTable(),
			AfterPrimaryKey: afterPrimaryKey,
		})
	}
	return result
}

// NewHeartbeatMessage creates a protobuf heartbeat message for the given time.
func NewHeartbeatMessage(now time.Time) *peerv1.ConsumeResponse {
	return &peerv1.ConsumeResponse{
		Msg: &peerv1.ConsumeResponse_Heartbeat{
			Heartbeat: &peerv1.Heartbeat{
				SentAt: timestamppb.New(now),
			},
		},
	}
}

// NewAckRequest creates a unary ack request for the given table and event ID.
func NewAckRequest(region, table, lastEventID string) *peerv1.AckRequest {
	return &peerv1.AckRequest{
		Region:      region,
		Table:       table,
		LastEventId: lastEventID,
	}
}

// ToReplicationEvents converts protobuf events into internal replication events.
// The table name is read from each event's Table field.
func ToReplicationEvents(events []*peerv1.Event) ([]models.ReplicationEvent, error) {
	result := make([]models.ReplicationEvent, len(events))
	for i, event := range events {
		primaryKey, err := fromProtoPrimaryKey(event.GetPrimaryKey())
		if err != nil {
			return nil, fmt.Errorf("decode event primary key for %s event %q: %w", event.GetTable(), event.GetEventId(), err)
		}
		result[i] = models.ReplicationEvent{
			PeerRegion: event.GetOriginRegion(),
			Table:      event.GetTable(),
			EventID:    event.GetEventId(),
			PrimaryKey: primaryKey,
			Operation:  fromProtoOperation(event.GetOperation()),
			Version:    event.GetVersion(),
			Fields:     models.CloneFields(event.GetFields()),
			Metadata:   models.CloneFields(event.GetMetadata()),
		}
	}

	return result, nil
}

// ToSnapshotRows converts protobuf snapshot rows into internal snapshot rows.
func ToSnapshotRows(rows []*peerv1.SnapshotRow) ([]models.SnapshotRow, error) {
	result := make([]models.SnapshotRow, 0, len(rows))
	for i, row := range rows {
		payload, _ := models.FieldsToPayload(row.GetFields())
		primaryKey, err := fromProtoPrimaryKey(row.GetPrimaryKey())
		if err != nil {
			return nil, fmt.Errorf("decode snapshot row primary key at index %d: %w", i, err)
		}
		result = append(result, models.SnapshotRow{
			PrimaryKey: primaryKey,
			Payload:    payload,
			Metadata:   models.CloneFields(row.GetMetadata()),
			Version:    0,
		})
	}

	return result, nil
}

func toProtoSnapshotRows(rows []models.SnapshotRow) []*peerv1.SnapshotRow {
	result := make([]*peerv1.SnapshotRow, 0, len(rows))
	for _, row := range rows {
		fields, _ := models.PayloadToFields(row.Payload)
		result = append(result, &peerv1.SnapshotRow{
			PrimaryKey: toProtoPrimaryKey(row.PrimaryKey),
			Fields:     fields,
			Metadata:   row.Metadata,
		})
	}

	return result
}

func toProtoEvents(events []models.ReplicationEvent) []*peerv1.Event {
	result := make([]*peerv1.Event, 0, len(events))
	for _, event := range events {
		result = append(result, &peerv1.Event{
			EventId:      event.EventID,
			OriginRegion: event.PeerRegion,
			Table:        event.Table,
			PrimaryKey:   toProtoPrimaryKey(event.PrimaryKey),
			Fields:       event.Fields,
			Metadata:     event.Metadata,
			Version:      event.Version,
			Operation:    toProtoOperation(event.Operation),
		})
	}

	return result
}

func toProtoOperation(operation models.ReplicationOperation) peerv1.Event_Operation {
	switch operation {
	case models.ReplicationOperationInsert:
		return peerv1.Event_OPERATION_INSERT
	case models.ReplicationOperationUpdate:
		return peerv1.Event_OPERATION_UPDATE
	case models.ReplicationOperationDelete:
		return peerv1.Event_OPERATION_DELETE
	default:
		return peerv1.Event_OPERATION_UNSPECIFIED
	}
}

func fromProtoOperation(operation peerv1.Event_Operation) models.ReplicationOperation {
	switch operation {
	case peerv1.Event_OPERATION_INSERT:
		return models.ReplicationOperationInsert
	case peerv1.Event_OPERATION_UPDATE:
		return models.ReplicationOperationUpdate
	case peerv1.Event_OPERATION_DELETE:
		return models.ReplicationOperationDelete
	default:
		return models.ReplicationOperationUpsert
	}
}

func toProtoPrimaryKey(parts []models.PrimaryKeyPart) []*peerv1.PrimaryKeyPart {
	result := make([]*peerv1.PrimaryKeyPart, 0, len(parts))
	for _, part := range parts {
		protoPart := &peerv1.PrimaryKeyPart{
			Column: part.Column,
		}
		assignProtoPrimaryKeyValue(protoPart, part)
		result = append(result, protoPart)
	}

	return result
}

func fromProtoPrimaryKey(parts []*peerv1.PrimaryKeyPart) ([]models.PrimaryKeyPart, error) {
	result := make([]models.PrimaryKeyPart, 0, len(parts))
	for _, part := range parts {
		modelPart := models.PrimaryKeyPart{
			Column: part.GetColumn(),
			Type:   inferPrimaryKeyType(part),
		}
		value, err := fromProtoPrimaryKeyValue(part)
		if err != nil {
			return nil, err
		}
		modelPart.Value = value
		result = append(result, modelPart)
	}

	return result, nil
}

func assignProtoPrimaryKeyValue(dst *peerv1.PrimaryKeyPart, part models.PrimaryKeyPart) {
	switch part.Type {
	case models.PrimaryKeyTypeInteger:
		var value int64
		if err := json.Unmarshal(part.Value, &value); err == nil {
			dst.Value = &peerv1.PrimaryKeyPart_IntegerValue{IntegerValue: value}
			return
		}
	case models.PrimaryKeyTypeBoolean:
		var value bool
		if err := json.Unmarshal(part.Value, &value); err == nil {
			dst.Value = &peerv1.PrimaryKeyPart_BooleanValue{BooleanValue: value}
			return
		}
	default:
		var value string
		if err := json.Unmarshal(part.Value, &value); err == nil {
			dst.Value = &peerv1.PrimaryKeyPart_StringValue{StringValue: value}
			return
		}
	}
	dst.Value = &peerv1.PrimaryKeyPart_StringValue{StringValue: string(part.Value)}
}

func fromProtoPrimaryKeyValue(part *peerv1.PrimaryKeyPart) ([]byte, error) {
	switch value := part.GetValue().(type) {
	case *peerv1.PrimaryKeyPart_IntegerValue:
		raw, _ := json.Marshal(value.IntegerValue)
		return raw, nil
	case *peerv1.PrimaryKeyPart_BooleanValue:
		raw, _ := json.Marshal(value.BooleanValue)
		return raw, nil
	case *peerv1.PrimaryKeyPart_StringValue:
		raw, _ := json.Marshal(value.StringValue)
		return raw, nil
	default:
		return nil, fmt.Errorf("primary key part %q missing typed value", part.GetColumn())
	}
}

func inferPrimaryKeyType(part *peerv1.PrimaryKeyPart) models.PrimaryKeyType {
	switch part.GetValue().(type) {
	case *peerv1.PrimaryKeyPart_IntegerValue:
		return models.PrimaryKeyTypeInteger
	case *peerv1.PrimaryKeyPart_BooleanValue:
		return models.PrimaryKeyTypeBoolean
	case *peerv1.PrimaryKeyPart_StringValue:
		return models.PrimaryKeyTypeString
	default:
		return models.PrimaryKeyTypeUnspecified
	}
}
