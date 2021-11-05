package raftds

import (
	"context"

	"github.com/daotl/go-datastore"
	dskey "github.com/daotl/go-datastore/key"
	dsq "github.com/daotl/go-datastore/query"
	"github.com/hashicorp/raft"
)

var logPrefixKey = dskey.NewBytesKey([]byte("l"))
var stablePrefixKey = dskey.NewBytesKey([]byte("s"))

// Store can be used as a LogStore and StableStore for Raft.
type Store struct {
	ds datastore.Datastore
}

func NewStore(ds datastore.Datastore) *Store {
	return &Store{ds: ds}
}

func getStableKey(k []byte) dskey.Key {
	return stablePrefixKey.Child(dskey.NewBytesKey(k))
}

func getLogKey(idx uint64) dskey.Key {
	return logPrefixKey.Child(dskey.NewBytesKey(uint64ToBytes(idx)))
}

// --------------------- StableStore------------------------

func (s *Store) Set(key []byte, val []byte) error {
	return s.ds.Put(context.Background(), getStableKey(key), val)
}

func (s *Store) Get(key []byte) ([]byte, error) {
	return s.ds.Get(context.Background(), getStableKey(key))
}

func (s *Store) SetUint64(key []byte, val uint64) error {
	return s.Set(key, uint64ToBytes(val))
}

func (s *Store) GetUint64(key []byte) (uint64, error) {
	val, err := s.Get(key)
	if err != nil {
		return 0, err
	}
	return bytesToUint64(val), nil
}

// --------------------- StableStore over ------------------------

// --------------------- LogStore ------------------------

// FirstIndex returns the first index written. 0 for no entries.
func (s *Store) FirstIndex() (uint64, error) {
	results, err := s.ds.Query(context.Background(), dsq.Query{
		Prefix:   logPrefixKey,
		KeysOnly: true,
	})
	defer results.Close()
	if err != nil {
		return 0, err
	}
	res, ok := results.NextSync()
	if !ok {
		// no entries
		return 0, nil
	}
	if res.Error != nil {
		return 0, res.Error
	}
	return bytesToUint64(res.Key.TrimPrefix(logPrefixKey).Bytes()), nil
}

type reverseOrder struct {
}

func (o reverseOrder) Compare(entry dsq.Entry, entry2 dsq.Entry) int {
	if entry.Key.Less(entry2.Key) {
		return 1
	} else if entry2.Key.Less(entry.Key) {
		return -1
	}
	return 0
}

// LastIndex returns the last index written. 0 for no entries.
func (s *Store) LastIndex() (uint64, error) {
	results, err := s.ds.Query(context.Background(), dsq.Query{
		Prefix:   logPrefixKey,
		KeysOnly: true,
		Orders:   []dsq.Order{reverseOrder{}},
	})
	defer results.Close()
	if err != nil {
		return 0, err
	}
	res, ok := results.NextSync()
	if !ok {
		// no entries
		return 0, nil
	}
	if res.Error != nil {
		return 0, res.Error
	}
	return bytesToUint64(res.Key.TrimPrefix(logPrefixKey).Bytes()), nil
}

func (s *Store) GetLog(index uint64, log *raft.Log) error {
	val, err := s.ds.Get(context.Background(), getLogKey(index))
	if err != nil {
		return err
	}
	return decodeMsgPack(val, log)
}

func (s *Store) StoreLog(log *raft.Log) error {
	return s.StoreLogs([]*raft.Log{log})
}

func (s *Store) StoreLogs(logs []*raft.Log) error {
	for _, log := range logs {
		key := getLogKey(log.Index)
		val, err := encodeMsgPack(log)
		if err != nil {
			return err
		}

		if err := s.ds.Put(context.Background(), key, val.Bytes()); err != nil {
			return err
		}
	}
	return nil
}

// DeleteRange deletes a range of log entries. The range is inclusive.
func (s *Store) DeleteRange(min, max uint64) error {
	results, err := s.ds.Query(context.Background(), dsq.Query{
		Prefix:   logPrefixKey,
		KeysOnly: true,
		Range: dsq.Range{
			Start: getLogKey(min),
			End:   getLogKey(max + 1),
		},
	})
	defer results.Close()
	if err != nil {
		return err
	}
	for result := range results.Next() {
		if result.Error != nil {
			return err
		}
		s.ds.Delete(context.Background(), result.Key)
	}
	return nil
}

// --------------------- LogStore over ------------------------

// Close is used to gracefully close the datastore.
func (s *Store) Close() error {
	return s.ds.Close()
}
