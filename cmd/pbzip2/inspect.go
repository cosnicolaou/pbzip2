// Copyright 2021 Cosmos Nicolaou. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"

	"cloudeng.io/cmdutil"
	"cloudeng.io/errors"
	"github.com/cosnicolaou/pbzip2"
	"github.com/cosnicolaou/pbzip2/internal/bzip2"
)

func scanFile(ctx context.Context, name string) error {
	rd, _, readerCleanup, err := openFileOrURL(ctx, name)
	if err != nil {
		return err
	}
	defer readerCleanup(ctx)
	sc := pbzip2.NewScanner(rd)
	for sc.Scan(ctx) {
		block := sc.Block()
		fmt.Println(name, block.String())
	}
	return sc.Err()
}

func scan(ctx context.Context, values interface{}, args []string) error {
	ctx, cancel := context.WithCancel(ctx)
	cmdutil.HandleSignals(cancel, os.Interrupt)
	errs := errors.M{}
	for _, arg := range args {
		errs.Append(scanFile(ctx, arg))
	}
	return errs.Err()
}

func bz2StatsFile(ctx context.Context, name string) error {
	rd, _, readerCleanup, err := openFileOrURL(ctx, name)
	if err != nil {
		return err
	}
	defer readerCleanup(ctx)

	ctx, cancel := context.WithCancel(ctx)
	cmdutil.HandleSignals(func() {
		readerCleanup(ctx)
		cancel()
	}, os.Interrupt)

	bz2rd := bzip2.NewReaderWithStats(rd)
	if _, err = io.Copy(ioutil.Discard, bz2rd); err != nil {
		return fmt.Errorf("failed to read: %v: %v", name, err)
	}
	stats := bzip2.StreamStats(bz2rd)
	fmt.Printf("=== %v ===\n", name)
	fmt.Printf("Block, CRC, Size\n")
	if len(stats.BlockStartOffsets) > 0 {
		offsets := make([]uint, len(stats.BlockStartOffsets)+1)
		for i := 0; i < len(offsets)-1; i++ {
			offsets[i] = stats.BlockStartOffsets[i]
		}
		offsets[len(offsets)-1] = stats.EndOfStreamOffset
		for i := 1; i < len(offsets); i++ {
			size := offsets[i] - offsets[i-1] - 48
			crc := stats.BlockCRCs[i]
			fmt.Printf("% 12d   : % 12d - % 12d\n", i, crc, size)
		}
	}
	fmt.Printf("Stream/File CRC      : %v\n", stats.StreamCRC)
	return nil
}

func bz2stats(ctx context.Context, values interface{}, args []string) error {
	ctx, cancel := context.WithCancel(ctx)
	cmdutil.HandleSignals(cancel, os.Interrupt)
	errs := errors.M{}
	for _, arg := range args {
		errs.Append(bz2StatsFile(ctx, arg))
	}
	return errs.Err()
}
