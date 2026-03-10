//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package export

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"runtime"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/parquet-go/parquet-go"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
)

type testShard struct {
	store *lsmkv.Store
	name  string
}

func (s *testShard) Store() *lsmkv.Store { return s.store }
func (s *testShard) Name() string        { return s.name }

func createTestStore(t *testing.T, numObjects int) (*lsmkv.Store, int) {
	t.Helper()
	dir := t.TempDir()
	ctx := context.Background()
	logger, _ := test.NewNullLogger()

	store, err := lsmkv.New(dir, dir, logger, nil, nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop())
	require.NoError(t, err)

	err = store.CreateOrLoadBucket(ctx, helpers.ObjectsBucketLSM,
		lsmkv.WithStrategy(lsmkv.StrategyReplace))
	require.NoError(t, err)

	bucket := store.Bucket(helpers.ObjectsBucketLSM)
	require.NotNil(t, bucket)

	inserted := 0
	// Insert objects in batches across multiple segments.
	batchSize := numObjects / 3
	if batchSize == 0 {
		batchSize = numObjects
	}

	for i := 0; i < numObjects; i++ {
		obj := createTestStorObj(t, uint64(i), "TestClass")
		data, err := obj.MarshalBinary()
		require.NoError(t, err)

		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(i))
		require.NoError(t, bucket.Put(key, data))
		inserted++

		if inserted%batchSize == 0 && inserted < numObjects {
			require.NoError(t, bucket.FlushAndSwitch())
		}
	}

	if inserted > 0 {
		require.NoError(t, bucket.FlushAndSwitch())
	}

	return store, inserted
}

func newTestErrGroup(logger logrus.FieldLogger) *enterrors.ErrorGroupWrapper {
	eg := enterrors.NewErrorGroupWrapper(logger)
	eg.SetLimit(runtime.GOMAXPROCS(0))
	return eg
}

func createTestStorObj(t *testing.T, id uint64, className string) *storobj.Object {
	t.Helper()
	uid := strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-%012d", id))
	obj := &models.Object{
		ID:    uid,
		Class: className,
	}
	return storobj.FromObject(obj, nil, nil, nil)
}

// TestExportShardDataParallel verifies correctness and uniqueness across
// different dataset sizes: empty, single object, small, and exceeding the
// internal scanBatchSize (1000).
func TestExportShardDataParallel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		numObjects int
	}{
		{name: "empty bucket", numObjects: 0},
		{name: "single object", numObjects: 1},
		{name: "small dataset", numObjects: 200},
		{name: "exceeds scan batch size", numObjects: 2500},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			logger, _ := test.NewNullLogger()

			store, inserted := createTestStore(t, tc.numObjects)
			defer store.Shutdown(context.Background())
			require.Equal(t, tc.numObjects, inserted)

			shard := &testShard{store: store, name: "test-shard"}

			var buf bytes.Buffer
			writer, err := NewParquetWriter(&buf)
			require.NoError(t, err)

			err = exportShardDataParallel(context.Background(), newTestErrGroup(logger), shard, writer, "TestClass", logger)
			require.NoError(t, err)
			require.NoError(t, writer.Close())

			assert.Equal(t, int64(tc.numObjects), writer.ObjectsWritten())

			if tc.numObjects > 0 {
				readBack := readParquetRows(t, buf.Bytes())
				assert.Len(t, readBack, tc.numObjects)
				assertUniqueIDs(t, readBack)
			}
		})
	}
}

func TestExportShardDataParallel_NilStore(t *testing.T) {
	t.Parallel()
	logger, _ := test.NewNullLogger()

	shard := &testShard{store: nil, name: "nil-store-shard"}

	var buf bytes.Buffer
	writer, err := NewParquetWriter(&buf)
	require.NoError(t, err)

	err = exportShardDataParallel(context.Background(), newTestErrGroup(logger), shard, writer, "TestClass", logger)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "store not found")
}

func TestExportShardDataParallel_NilBucket(t *testing.T) {
	t.Parallel()
	logger, _ := test.NewNullLogger()

	// Create a store without the objects bucket.
	dir := t.TempDir()
	store, err := lsmkv.New(dir, dir, logger, nil, nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop())
	require.NoError(t, err)
	defer store.Shutdown(context.Background())

	shard := &testShard{store: store, name: "no-bucket-shard"}

	var buf bytes.Buffer
	writer, err := NewParquetWriter(&buf)
	require.NoError(t, err)

	err = exportShardDataParallel(context.Background(), newTestErrGroup(logger), shard, writer, "TestClass", logger)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "objects bucket not found")
}

func TestExportShardDataParallel_ContextCanceled(t *testing.T) {
	t.Parallel()
	logger, _ := test.NewNullLogger()

	store, _ := createTestStore(t, 200)
	defer store.Shutdown(context.Background())

	shard := &testShard{store: store, name: "test-shard"}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately.

	var buf bytes.Buffer
	writer, err := NewParquetWriter(&buf)
	require.NoError(t, err)

	err = exportShardDataParallel(ctx, newTestErrGroup(logger), shard, writer, "TestClass", logger)
	// Context cancellation is racy: the export may complete before
	// workers notice the cancellation.
	if err != nil {
		assert.ErrorIs(t, err, context.Canceled)
	}
}

// TestExportShardDataParallel_TryGoFallback occupies the only error group slot
// so all TryGo calls return false and ranges execute inline, exercising the
// synchronous fallback path.
func TestExportShardDataParallel_TryGoFallback(t *testing.T) {
	t.Parallel()
	logger, _ := test.NewNullLogger()

	numObjects := 500
	store, inserted := createTestStore(t, numObjects)
	defer store.Shutdown(context.Background())
	require.Equal(t, numObjects, inserted)

	shard := &testShard{store: store, name: "test-shard"}

	eg := enterrors.NewErrorGroupWrapper(logger)
	eg.SetLimit(1)

	blocked := make(chan struct{})
	eg.Go(func() error {
		<-blocked
		return nil
	})

	var buf bytes.Buffer
	writer, err := NewParquetWriter(&buf)
	require.NoError(t, err)

	err = exportShardDataParallel(context.Background(), eg, shard, writer, "TestClass", logger)
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	assert.Equal(t, int64(numObjects), writer.ObjectsWritten())

	readBack := readParquetRows(t, buf.Bytes())
	assert.Len(t, readBack, numObjects)
	assertUniqueIDs(t, readBack)

	// Unblock the occupied slot so eg.Wait() can return.
	close(blocked)
	require.NoError(t, eg.Wait())
}

// readParquetRows reads all ParquetRow entries from a parquet file in memory.
func readParquetRows(t *testing.T, data []byte) []ParquetRow {
	t.Helper()

	reader := parquet.NewGenericReader[ParquetRow](bytes.NewReader(data))
	defer reader.Close()

	rows := make([]ParquetRow, reader.NumRows())
	n, err := reader.Read(rows)
	if err != nil && !errors.Is(err, io.EOF) {
		require.NoError(t, err, "failed to read parquet rows")
	}
	require.Equal(t, int(reader.NumRows()), n, "did not read all parquet rows")
	return rows[:n]
}

// assertUniqueIDs verifies that all ParquetRow IDs are unique.
func assertUniqueIDs(t *testing.T, rows []ParquetRow) {
	t.Helper()
	seen := make(map[string]struct{}, len(rows))
	for _, r := range rows {
		if _, exists := seen[r.ID]; exists {
			t.Errorf("duplicate ID found: %s", r.ID)
		}
		seen[r.ID] = struct{}{}
	}
}
