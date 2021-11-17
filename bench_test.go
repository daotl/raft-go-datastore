package raftds

import (
	"testing"

	raftbench "github.com/hashicorp/raft/bench"
)

func BenchmarkDataStore_FirstIndex(b *testing.B) {
	store := testStore(b)
	defer store.Close()

	raftbench.FirstIndex(b, store)
}

func BenchmarkDataStore_LastIndex(b *testing.B) {
	store := testStore(b)
	defer store.Close()

	raftbench.LastIndex(b, store)
}

func BenchmarkDataStore_GetLog(b *testing.B) {
	store := testStore(b)
	defer store.Close()

	raftbench.GetLog(b, store)
}

func BenchmarkDataStore_StoreLog(b *testing.B) {
	store := testStore(b)
	defer store.Close()

	raftbench.StoreLog(b, store)
}

func BenchmarkDataStore_StoreLogs(b *testing.B) {
	store := testStore(b)
	defer store.Close()

	raftbench.StoreLogs(b, store)
}

func BenchmarkDataStore_DeleteRange(b *testing.B) {
	store := testStore(b)
	defer store.Close()

	raftbench.DeleteRange(b, store)
}

func BenchmarkDataStore_Set(b *testing.B) {
	store := testStore(b)
	defer store.Close()

	raftbench.Set(b, store)
}

func BenchmarkDataStore_Get(b *testing.B) {
	store := testStore(b)
	defer store.Close()

	raftbench.Get(b, store)
}

func BenchmarkDataStore_SetUint64(b *testing.B) {
	store := testStore(b)
	defer store.Close()

	raftbench.SetUint64(b, store)
}

func BenchmarkDataStore_GetUint64(b *testing.B) {
	store := testStore(b)
	defer store.Close()

	raftbench.GetUint64(b, store)
}
