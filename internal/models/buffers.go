package models

const (
	// LiveReplicationBufferSize is the shared in-memory buffer size for handing
	// off live outbox batches to the gRPC stream sender. The send queue must stay
	// at least this large so the server does not add tighter backpressure than
	// the subscriber channel it drains.
	LiveReplicationBufferSize = 128
)
