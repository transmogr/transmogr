// Package protocol converts between internal replication models and gRPC messages.
package protocol

import (
	"fmt"
	"slices"

	"github.com/transmogr/transmogr/internal/models"
	peerv1 "github.com/transmogr/transmogr/pkg/proto/transmogr/peerv1"
)

// CheckHandshakeCompatibility verifies that the remote peer is compatible with
// the local node: the replicated table sets must be identical and the protocol
// versions must match. Returns a plain error; callers wrap it into the
// appropriate type (e.g. a gRPC status or fmt.Errorf).
func CheckHandshakeCompatibility(
	localHandshake models.HandshakeMessage,
	hs *peerv1.Handshake,
) error {
	localCfg, _ := models.ParseHandshakeConfiguration(localHandshake.Configuration)
	remoteCfg, err := models.ParseHandshakeConfiguration(hs.GetConfiguration())
	if err != nil {
		return fmt.Errorf("invalid peer configuration: %w", err)
	}

	localTables := handshakeTableNames(localCfg)
	remoteTables := handshakeTableNames(remoteCfg)
	if !sameTables(localTables, remoteTables) {
		onlyLocal, onlyRemote := tableDiff(localTables, remoteTables)
		return fmt.Errorf("peer table set does not match: local_only=%v remote_only=%v", onlyLocal, onlyRemote)
	}

	if remoteVersion := hs.GetVersion(); remoteVersion != localHandshake.Version {
		return fmt.Errorf("peer version mismatch: local=%s remote=%s", localHandshake.Version, remoteVersion)
	}

	return nil
}

// handshakeTableNames returns the table names from a handshake configuration.
func handshakeTableNames(cfg models.HandshakeConfig) []string {
	names := make([]string, 0, len(cfg.Tables))
	for _, t := range cfg.Tables {
		names = append(names, t.Name)
	}
	return names
}

// sameTables reports whether two table name lists contain the same set of names.
func sameTables(left, right []string) bool {
	return slices.Equal(sortedCopy(left), sortedCopy(right))
}

// tableDiff returns names present only in local and names present only in remote.
func tableDiff(local, remote []string) (onlyLocal, onlyRemote []string) {
	localSet := make(map[string]struct{}, len(local))
	remoteSet := make(map[string]struct{}, len(remote))
	for _, t := range local {
		localSet[t] = struct{}{}
	}
	for _, t := range remote {
		remoteSet[t] = struct{}{}
	}
	for _, t := range local {
		if _, ok := remoteSet[t]; !ok {
			onlyLocal = append(onlyLocal, t)
		}
	}
	for _, t := range remote {
		if _, ok := localSet[t]; !ok {
			onlyRemote = append(onlyRemote, t)
		}
	}
	return
}

// LastProtoEventID returns the EventId of the last element in the slice, or "" if empty.
func LastProtoEventID[T interface{ GetEventId() string }](events []T) string {
	if len(events) == 0 {
		return ""
	}
	return events[len(events)-1].GetEventId()
}

func sortedCopy(src []string) []string {
	if src == nil {
		return nil
	}
	dst := make([]string, len(src))
	copy(dst, src)
	slices.Sort(dst)
	return dst
}
