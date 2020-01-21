// Copyright 2020 Cosmos Nicolaou. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// +build ignore

package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"

	"github.com/cosnicolaou/pbzip2/bzip2"
	"github.com/grailbio/base/file"
	"github.com/grailbio/base/must"
	"v.io/x/lib/cmd/flagvar"
)

var commandline struct {
	InputFile string `cmd:"input,,'input file, s3 path, or url'"`
}

func init() {
	must.Nil(flagvar.RegisterFlagsInStruct(flag.CommandLine, "cmd", &commandline,
		nil, nil))
}

func main() {
	ctx := context.Background()
	flag.Parse()

	file, err := file.Open(ctx, commandline.InputFile)
	if err != nil {
		log.Fatalf("open: %v", err)
	}
	rd := bzip2.NewReaderWithStats(file.Reader(ctx))
	if _, err = io.Copy(ioutil.Discard, rd); err != nil {
		log.Fatalf("failed to read: %v: %v", commandline.InputFile, err)
	}
	stats := bzip2.StreamStats(rd)
	fmt.Printf("=== %v ===\n", commandline.InputFile)
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
}
