package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/cosnicolaou/pbzip2"
	"github.com/grailbio/base/file"
	"github.com/grailbio/base/file/s3file"
	"github.com/grailbio/base/must"
	"github.com/schollz/progressbar/v2"
	"golang.org/x/crypto/ssh/terminal"
	"v.io/x/lib/cmd/flagvar"
)

var commandline struct {
	InputFile        string `cmd:"input,,'input file, s3 path, or url'"`
	Concurrency      int    `cmd:"concurrency,4,'concurrency for the decompression'"`
	ProgressBar      bool   `cmd:"progress,true,display a progress bar"`
	OutputFile       string `cmd:"output,,'output file or s3 path, omit for stdout'"`
	MaxBlockOverhead int    `cmd:"max-block-overhead,,'the max size of the per block coding tables'"`
	Verbose          bool   `cmd:"verbose,false,verbose debug/trace information"`
}

func init() {
	must.Nil(flagvar.RegisterFlagsInStruct(flag.CommandLine, "cmd", &commandline,
		map[string]interface{}{
			"concurrency": runtime.GOMAXPROCS(-1),
		}, nil))
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

func OnSignal(fn func(), signals ...os.Signal) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, signals...)
	go func() {
		sig := <-sigCh
		fmt.Println("stopping on... ", sig)
		fn()
	}()
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

func runner() (returnErr error) {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	OnSignal(cancel, os.Interrupt)

	bzOpts := []pbzip2.DecompressorOption{
		pbzip2.BZConcurrency(commandline.Concurrency),
		pbzip2.BZVerbose(commandline.Verbose),
	}
	scanOpts := []pbzip2.ScannerOption{}

	if commandline.MaxBlockOverhead > 0 {
		scanOpts = append(scanOpts,
			pbzip2.ScanBlockOverhead(commandline.MaxBlockOverhead))
	}

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

	defer func() {
		if err := writerCleanup(ctx); err != nil {
			log.Printf("writer cleanup: %v", err)
			if returnErr == nil {
				returnErr = err
			}
		}
	}()

	// Kick off the progress bar, if requested and the output is not
	// being written to stdout.
	var (
		progressBarCh chan pbzip2.Progress
		progressBarWg sync.WaitGroup
		progressBarWr = os.Stdout
	)
	showProgressBar := len(commandline.OutputFile) > 0
	isTTY := terminal.IsTerminal(int(os.Stdout.Fd()))
	if commandline.ProgressBar && (showProgressBar || !isTTY) {
		progressBarCh = make(chan pbzip2.Progress, commandline.Concurrency)
		progressBarWg.Add(1)
		bzOpts = append(bzOpts, pbzip2.BZSendUpdates(progressBarCh))
		if !isTTY {
			progressBarWr = os.Stderr
		}
		go func() {
			progressBar(ctx, progressBarWr, progressBarCh, size)
			progressBarWg.Done()
		}()
	}

	sc := pbzip2.NewScanner(rd, scanOpts...)
	dc := pbzip2.NewDecompressor(ctx, bzOpts...)

	// Kick off the output routine.
	errCh := make(chan error, 1)

	go func() {
		defer close(errCh)
		_, err := io.Copy(wr, dc.Reader())
		if err != nil && err != io.EOF {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	bn := 0
	for sc.Scan(ctx) {
		block, bitOffset, size, blockCRC := sc.Block()
		if commandline.Verbose {
			fmt.Printf("block # %v: %v bits\n", bn, size)
		}
		bn++
		dc.Decompress(sc.BlockSize(), block, bitOffset, blockCRC)
	}
	if err := sc.Err(); err != nil {
		return fmt.Errorf("scanner: %v", err)
	}

	crc, err := dc.Finish()
	if err != nil {
		return fmt.Errorf("decompressor: %v", err)
	}
	if got, want := crc, sc.StreamCRC(); got != want {
		returnErr = fmt.Errorf("mismatched CRC: %v != %v", got, want)
	}

	returnErr = <-errCh
	if progressBarCh != nil {
		close(progressBarCh)
		progressBarWg.Wait()
	}
	return
}
