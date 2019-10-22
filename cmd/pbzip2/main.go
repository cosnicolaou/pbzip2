package main

import (
	"bytes"
	"compress/bzip2"
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"strings"

	"github.com/grailbio/base/file"
	"github.com/spf13/cobra"
	"neeva.co/cmdutil"
	"neeva.co/util/jsonutil"
	"v.io/x/lib/cmd/pflagvar"
)

var (
	inputFile string
)

func init() {
	flag.StringVar(&inputFile, "", "input file or URL")
}

func sharderCmd(cmd *cobra.Command, args []string) error {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	cmdutil.OnSignal(cancel, os.Interrupt)

	input := inputFile
	if u, err := url.Parse(input); err == nil && (u.Scheme != "") {
		// initiate multiple part download.
		// then start parallel bzip.
		return fmt.Errorf("download not implemented yet: %v", input)
	}
	if !strings.HasSuffix(input, ".bz2") {
		return fmt.Errorf("sharding a non bz2 file is not yet supported, sharding only .bz2 files for now.")
	}
	inputFile, err := file.Open(ctx, input)
	if err != nil {
		log.Fatalf("failed to open %v: %v", input, err)
	}
	reader := inputFile.Reader(ctx)
	sc := jsonutil.NewBZ2BlockScanner(reader)
	for sc.Scan() {
		trailer := []byte{0x17, 0x72, 0x45, 0x38, 0x50, 0x90}
		block := sc.Block()
		header := sc.StreamHeader()
		stream := make([]byte, len(block)+len(header)+len(trailer))
		copy(stream[:len(header)], header)
		copy(stream[len(header):], block)
		copy(stream[len(header)+len(block):], trailer)
		dec := bzip2.NewReader(bytes.NewBuffer(stream))
		buf, err := ioutil.ReadAll(dec)
		if err != nil {
			fmt.Printf("ERR: %v\n", err)
		}
		_ = buf
		fmt.Printf("STREAM %x + %x = %x\n", header, block[:10], stream[:12])
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	_, max := sc.Sizes()
	fmt.Printf("max bzip record size: %v\n", max)
	return sc.Err()
}
