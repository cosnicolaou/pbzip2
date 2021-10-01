// Copyright 2020 Cosmos Nicolaou. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"runtime"
	"strings"
	"sync"

	"cloudeng.io/cmdutil"
	"cloudeng.io/cmdutil/flags"
	"cloudeng.io/errors"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/cosnicolaou/pbzip2"
	"github.com/grailbio/base/file"
	"github.com/grailbio/base/file/s3file"
	"github.com/schollz/progressbar/v2"
	"golang.org/x/crypto/ssh/terminal"
)

type commandlineFlags struct {
	InputFile        string `subcmd:"input,,'input file, s3 path, or url'"`
	Concurrency      int    `subcmd:"concurrency,4,'concurrency for the decompression'"`
	ProgressBar      bool   `subcmd:"progress,true,display a progress bar"`
	OutputFile       string `subcmd:"output,,'output file or s3 path, omit for stdout'"`
	MaxBlockOverhead int    `subcmd:"max-block-overhead,,'the max size of the per block coding tables'"`
	Verbose          bool   `subcmd:"verbose,false,verbose debug/trace information"`
}

var commandline commandlineFlags

func init() {
	flags.RegisterFlagsInStruct(flag.CommandLine, "subcmd", &commandline,
		map[string]interface{}{
			"concurrency": runtime.GOMAXPROCS(-1),
		}, nil)
	file.RegisterImplementation("s3", func() file.Implementation {
		return s3file.NewImplementation(
			s3file.NewDefaultProvider(session.Options{}), s3file.Options{})
	})
}

func progressBar(ctx context.Context, progressBarWr io.Writer, ch chan pbzip2.Progress, size int64) {
	next := uint64(1)
	bar := progressbar.NewOptions64(size,
		progressbar.OptionSetBytes64(size),
		progressbar.OptionSetWriter(progressBarWr),
		progressbar.OptionSetPredictTime(true))
	bar.RenderBlank()
	for {
		select {
		case p := <-ch:
			if p.Block == 0 {
				fmt.Fprintf(progressBarWr, "\n")
				return
			}
			bar.Add(p.Compressed)
			if p.Block != next {
				log.Fatalf("out of sequence block %#v\n", p)
			}
			next++
		case <-ctx.Done():
			return
		}
	}
}

func openFileOrURL(ctx context.Context, name string) (io.Reader, int64, func(context.Context) error, error) {
	if strings.HasPrefix(name, "http") {
		resp, err := http.Get(name)
		if err != nil {
			return nil, 0, nil, err
		}
		return resp.Body,
			resp.ContentLength,
			func(context.Context) error {
				resp.Body.Close()
				return nil
			},

			err
	}
	info, err := file.Stat(ctx, name)
	if err != nil {
		return nil, 0, nil, err
	}
	file, err := file.Open(ctx, name)
	if err != nil {
		return nil, 0, nil, err
	}
	return file.Reader(ctx), info.Size(), file.Close, nil
}

func createFile(ctx context.Context, name string) (io.Writer, func(context.Context) error, error) {
	if len(name) == 0 {
		return os.Stdout,
			func(context.Context) error {
				return nil
			},
			nil
	}
	file, err := file.Create(ctx, name)
	if err != nil {
		return nil, nil, err
	}
	return file.Writer(ctx), file.Close, nil
}

func main() {
	flag.Parse()
	if err := runner(); err != nil {
		log.Fatal(err)
	}
}

func optsFromFlags(cl commandlineFlags) (
	bzOpts []pbzip2.DecompressorOption,
	scanOpts []pbzip2.ScannerOption,
	progressBarCh chan pbzip2.Progress,
	isTTY bool) {
	bzOpts = []pbzip2.DecompressorOption{
		pbzip2.BZConcurrency(commandline.Concurrency),
		pbzip2.BZVerbose(commandline.Verbose),
	}
	scanOpts = []pbzip2.ScannerOption{}

	if commandline.MaxBlockOverhead > 0 {
		scanOpts = append(scanOpts,
			pbzip2.ScanBlockOverhead(commandline.MaxBlockOverhead))
	}
	isTTY = terminal.IsTerminal(int(os.Stdout.Fd()))
	if commandline.ProgressBar && (len(commandline.OutputFile) > 0 || !isTTY) {
		ch := make(chan pbzip2.Progress, commandline.Concurrency)
		bzOpts = append(bzOpts, pbzip2.BZSendUpdates(ch))
		progressBarCh = ch
	}
	return
}

func runner() error {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	cmdutil.HandleSignals(cancel, os.Interrupt)

	bzOpts, scanOpts, progressBarCh, isTTY := optsFromFlags(commandline)
	if len(commandline.InputFile) == 0 {
		return fmt.Errorf("please specify an input file, s3 path or url")
	}

	rd, size, readerCleanup, err := openFileOrURL(ctx, commandline.InputFile)
	if err != nil {
		return err
	}
	defer readerCleanup(ctx)

	wr, writerCleanup, err := createFile(ctx, commandline.OutputFile)
	if err != nil {
		return err
	}

	// Kick off the progress bar, if requested and the output is not
	// being written to stdout.
	var (
		progressBarWg sync.WaitGroup
		progressBarWr = os.Stdout
	)

	if progressBarCh != nil {
		progressBarWg.Add(1)
		if !isTTY {
			progressBarWr = os.Stderr
		}
		go func() {
			progressBar(ctx, progressBarWr, progressBarCh, size)
			progressBarWg.Done()
		}()
	}

	dc := pbzip2.NewReader(ctx, rd,
		pbzip2.DecompressionOptions(bzOpts...),
		pbzip2.ScannerOptions(scanOpts...))

	errs := &errors.M{}
	_, err = io.Copy(wr, dc)
	errs.Append(err)
	err = writerCleanup(ctx)
	errs.Append(err)

	if progressBarCh != nil {
		close(progressBarCh)
		progressBarWg.Wait()
	}

	return errs.Err()
}
