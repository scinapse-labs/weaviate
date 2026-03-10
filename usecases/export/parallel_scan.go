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
	"fmt"
	"runtime"
	"sync"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/storobj"
)

const scanBatchSize = 1000

// exportShardDataParallel exports all objects from a shard using parallel
// range scanning. It splits the key space using QuantileKeys and assigns
// each range to a worker goroutine via the shared error group.
func exportShardDataParallel(ctx context.Context, eg *enterrors.ErrorGroupWrapper, shard ShardLike, writer *ParquetWriter, className string, logger logrus.FieldLogger) error {
	store := shard.Store()
	if store == nil {
		return fmt.Errorf("store not found for shard %s", shard.Name())
	}

	bucket := store.Bucket(helpers.ObjectsBucketLSM)
	if bucket == nil {
		return fmt.Errorf("objects bucket not found for shard %s", shard.Name())
	}

	parallelism := runtime.GOMAXPROCS(0) * 2
	if parallelism < 2 {
		parallelism = 2
	}

	quantileKeys := bucket.QuantileKeys(parallelism - 1)

	type keyRange struct {
		start, end []byte
	}

	var ranges []keyRange
	if len(quantileKeys) == 0 {
		// No split points (e.g. tiny dataset) — single range covers everything.
		ranges = []keyRange{{start: nil, end: nil}}
	} else {
		ranges = make([]keyRange, 0, len(quantileKeys)+1)
		ranges = append(ranges, keyRange{start: nil, end: quantileKeys[0]})
		for i := 1; i < len(quantileKeys); i++ {
			ranges = append(ranges, keyRange{start: quantileKeys[i-1], end: quantileKeys[i]})
		}
		ranges = append(ranges, keyRange{start: quantileKeys[len(quantileKeys)-1], end: nil})
	}

	rowsCh := make(chan []ParquetRow, len(ranges)*2)

	// scanCtx is canceled when the writer hits an error, so scan workers
	// stop early instead of blocking on channel sends.
	scanCtx, scanCancel := context.WithCancel(ctx)
	defer scanCancel()

	// Writer goroutine: single consumer drains rowsCh into ParquetWriter.
	var writerErr error
	writerDone := make(chan struct{})
	enterrors.GoWrapper(func() {
		defer close(writerDone)
		for batch := range rowsCh {
			for i := range batch {
				if err := writer.WriteRow(batch[i]); err != nil {
					writerErr = fmt.Errorf("write row to parquet: %w", err)
					scanCancel()
					// Drain rowsCh so scan workers don't block on sends.
					// After scanCancel(), workers will exit via ctx.Done()
					// in their select-sends, but a worker that already
					// passed the ctx check may still be mid-send. The drain
					// ensures those in-flight sends complete.
					for range rowsCh {
					}
					return
				}
			}
		}
	}, logger)

	var scanWg sync.WaitGroup
	var scanMu sync.Mutex
	var scanErr error

	setScanErr := func(err error) {
		scanMu.Lock()
		if scanErr == nil {
			scanErr = err
		}
		scanMu.Unlock()
	}

	for _, r := range ranges {
		scanWg.Add(1)
		scanFn := func() error {
			defer scanWg.Done()
			if err := scanRange(scanCtx, bucket, r.start, r.end, rowsCh); err != nil {
				setScanErr(err)
				return err
			}
			return nil
		}
		// Use TryGo to avoid deadlock: the caller already holds a slot in eg,
		// so blocking on eg.Go could deadlock if all slots are taken by shard
		// exporters. Ranges that don't get a slot run inline.
		if !eg.TryGo(scanFn) {
			if err := scanFn(); err != nil {
				break
			}
		}
	}

	scanWg.Wait()
	close(rowsCh)

	// Wait for writer to finish.
	<-writerDone

	// If the writer failed, it canceled scanCtx which caused scan workers to
	// return context.Canceled. The writer error is the root cause.
	if writerErr != nil {
		return writerErr
	}

	if scanErr != nil {
		return scanErr
	}

	logger.WithField("shard", shard.Name()).
		WithField("class", className).
		WithField("object_count", writer.ObjectsWritten()).
		Info("completed parallel shard iteration")

	return nil
}

// scanRange scans [startKey, endKey) using a Cursor and sends batches
// of ParquetRows to rowsCh. If endKey is nil, scans to the end.
func scanRange(
	ctx context.Context,
	bucket *lsmkv.Bucket,
	startKey, endKey []byte,
	rowsCh chan<- []ParquetRow,
) error {
	cursor := bucket.Cursor()
	defer cursor.Close()

	var key, val []byte
	if startKey == nil {
		key, val = cursor.First()
	} else {
		key, val = cursor.Seek(startKey)
	}

	batch := make([]ParquetRow, 0, scanBatchSize)

	for key != nil {
		// Boundary check: stop if we've reached endKey.
		if endKey != nil && bytes.Compare(key, endKey) >= 0 {
			break
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		obj, err := storobj.FromBinary(val)
		if err != nil {
			return fmt.Errorf("deserialize object: %w", err)
		}

		row, err := convertToParquetRow(obj)
		if err != nil {
			return fmt.Errorf("convert object to parquet row: %w", err)
		}

		batch = append(batch, row)

		if len(batch) >= scanBatchSize {
			select {
			case rowsCh <- batch:
			case <-ctx.Done():
				return ctx.Err()
			}
			// Fresh allocation required: the writer goroutine still
			// holds a reference to the previous batch slice.
			batch = make([]ParquetRow, 0, scanBatchSize)
		}

		key, val = cursor.Next()
	}

	// Send remaining rows.
	if len(batch) > 0 {
		select {
		case rowsCh <- batch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}
