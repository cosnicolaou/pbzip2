// Copyright 2020 Cosmos Nicolaou. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"strings"
	"sync"

	"cloudeng.io/cmdutil"
	"cloudeng.io/cmdutil/subcmd"
	"cloudeng.io/errors"
	"github.com/cosnicolaou/pbzip2"
	"github.com/schollz/progressbar/v2"
	"golang.org/x/crypto/ssh/terminal"
)

type CommonFlags struct {
	Concurrency      int  `subcmd:"concurrency,4,'concurrency for the decompression'"`
	MaxBlockOverhead int  `subcmd:"max-block-overhead,,'the max size of the per block coding tables'"`
	Verbose          bool `subcmd:"verbose,false,verbose debug/trace information"`
}

type catFlags struct {
	CommonFlags
}

type unzipFlags struct {
	CommonFlags
	ProgressBar bool   `subcmd:"progress,true,display a progress bar"`
	OutputFile  string `subcmd:"output,,'local output filepath, omit for stdout'"`
}

type noFlags struct{}

var cmdSet *subcmd.CommandSet

func init() {
	defaultConcurrency := map[string]interface{}{
		"concurrency": runtime.GOMAXPROCS(-1),
	}

	bzcatCmd := subcmd.NewCommand("cat",
		subcmd.MustRegisterFlagStruct(&catFlags{}, defaultConcurrency, nil),
		cat, subcmd.AtLeastNArguments(0))
	bzcatCmd.Document(`decompress bzip2 files or stdin. Files may be local, on S3 or a URL.`)

	unzipCmd := subcmd.NewCommand("unzip",
		subcmd.MustRegisterFlagStruct(&unzipFlags{}, defaultConcurrency, nil),
		unzip, subcmd.ExactlyNumArguments(1))
	unzipCmd.Document(`decompress a bzip2 file.`)

	scanCmd := subcmd.NewCommand("scan",
		subcmd.MustRegisterFlagStruct(&noFlags{}, nil, nil),
		scan, subcmd.AtLeastNArguments(1))
	scanCmd.Document(`scan a bzip2 file using the pbzip2 package's scanner.`)

	bz2Stats := subcmd.NewCommand("bz2-stats",
		subcmd.MustRegisterFlagStruct(&noFlags{}, nil, nil),
		bz2stats, subcmd.AtLeastNArguments(1))
	bz2Stats.Document(`scan a bzip2 file to obtain bz2 stats on each block, the scan is serial and is intended purely for debugging purposes.`)

	cmdSet = subcmd.NewCommandSet(bzcatCmd, unzipCmd, scanCmd, bz2Stats)
	cmdSet.Document(`decompress and inspect bzip2 files. Files may be local, on S3 or a URL.`)

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

func openFile(name string) (io.Reader, int64, func() error, error) {
	if strings.HasPrefix(name, "http") {
		return nil, 0, nil, fmt.Errorf("http urls not supported")
	}
	info, err := os.Stat(name)
	if err != nil {
		return nil, 0, nil, err
	}
	file, err := os.Open(name)
	if err != nil {
		return nil, 0, nil, err
	}
	return file, info.Size(), file.Close, nil
}

func createFile(name string) (io.Writer, func() error, error) {
	if len(name) == 0 {
		return os.Stdout,
			func() error {
				return nil
			},
			nil
	}
	file, err := os.Create(name)
	if err != nil {
		return nil, nil, err
	}
	return file, file.Close, nil
}

func main() {
	cmdSet.MustDispatch(context.Background())
}

func optsFromCommonFlags(cl *CommonFlags) (
	bzOpts []pbzip2.DecompressorOption, scanOpts []pbzip2.ScannerOption) {

	bzOpts = []pbzip2.DecompressorOption{
		pbzip2.BZConcurrency(cl.Concurrency),
		pbzip2.BZVerbose(cl.Verbose),
	}
	scanOpts = []pbzip2.ScannerOption{}

	if cl.MaxBlockOverhead > 0 {
		scanOpts = append(scanOpts,
			pbzip2.ScanBlockOverhead(cl.MaxBlockOverhead))
	}
	return
}

func cat(ctx context.Context, values interface{}, args []string) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	cl := values.(*catFlags)
	cmdutil.HandleSignals(cancel, os.Interrupt)

	bzOpts, scanOpts := optsFromCommonFlags(&cl.CommonFlags)

	if len(args) == 0 {
		rd := pbzip2.NewReader(ctx, os.Stdin,
			pbzip2.DecompressionOptions(bzOpts...),
			pbzip2.ScannerOptions(scanOpts...))
		_, err := io.Copy(os.Stdout, rd)
		return err
	}

	for _, inputFile := range args {
		rd, _, readerCleanup, err := openFile(inputFile)
		if err != nil {
			return err
		}
		defer readerCleanup()

		dc := pbzip2.NewReader(ctx, rd,
			pbzip2.DecompressionOptions(bzOpts...),
			pbzip2.ScannerOptions(scanOpts...))

		_, err = io.Copy(os.Stdout, dc)
		if err != nil {
			return err
		}
	}
	return nil
}

func optsFromUnzipFlags(cl *unzipFlags) (
	bzOpts []pbzip2.DecompressorOption,
	scanOpts []pbzip2.ScannerOption,
	progressBarCh chan pbzip2.Progress,
	isTTY bool) {

	bzOpts, scanOpts = optsFromCommonFlags(&cl.CommonFlags)

	isTTY = terminal.IsTerminal(int(os.Stdout.Fd()))
	if cl.ProgressBar && (len(cl.OutputFile) > 0 || !isTTY) {
		ch := make(chan pbzip2.Progress, cl.Concurrency)
		bzOpts = append(bzOpts, pbzip2.BZSendUpdates(ch))
		progressBarCh = ch
	}
	return
}

func unzip(ctx context.Context, values interface{}, args []string) error {
	ctx, cancel := context.WithCancel(ctx)
	cmdutil.HandleSignals(cancel, os.Interrupt)
	cl := values.(*unzipFlags)

	bzOpts, scanOpts, progressBarCh, isTTY := optsFromUnzipFlags(cl)

	rd, size, readerCleanup, err := openFile(args[0])
	if err != nil {
		return err
	}
	defer readerCleanup()

	wr, writerCleanup, err := createFile(cl.OutputFile)
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
	err = writerCleanup()
	errs.Append(err)

	if progressBarCh != nil {
		close(progressBarCh)
		progressBarWg.Wait()
	}

	return errs.Err()
}
