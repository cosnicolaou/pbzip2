// +build ignore

package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/cosnicolaou/pbzip2"
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
	sc := pbzip2.NewScanner(file.Reader(ctx))
	fmt.Printf("=== %v ===\n", commandline.InputFile)
	fmt.Printf("Block, CRC, Size\n")
	i := 1
	for sc.Scan(ctx) {
		if i == 1587429 {
			pbzip2.VerboseV = true
		}
		block, _, blockSize, blockCRC := sc.Block()
		if i >= 1587430 {
			fmt.Printf("% 12d   : % 12d - % 12d\n", i, blockCRC, blockSize)
		}
		if i == 1587431 {
			err = ioutil.WriteFile("badblock", block, 0600)
			fmt.Printf("ERR: %v\n", err)
			os.Exit(1)
		}
		i++
	}
	if err := sc.Err(); err != nil {
		log.Fatalf("scanner: %v", err)
	}
	fmt.Printf("Stream/File CRC      : %v\n", sc.StreamCRC())
}
