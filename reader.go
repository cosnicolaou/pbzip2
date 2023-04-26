// Copyright 2021 Cosmos Nicolaou. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package pbzip2

import (
	"context"
	"io"
	"sync"
)

type readerOpts struct {
	decOpts  []DecompressorOption
	scanOpts []ScannerOption
}

// ReaderOption represents an option to NewReader.
type ReaderOption func(o *readerOpts)

// ScannerOptions passes a ScannerOption to the underlying scanner created by
// NewReader.
func ScannerOptions(opts ...ScannerOption) ReaderOption {
	return func(o *readerOpts) {
		o.scanOpts = append(o.scanOpts, opts...)
	}
}

// DecompressionOptions passes a ScannerOption to the underlying decompressor
// created by NewReader.
func DecompressionOptions(opts ...DecompressorOption) ReaderOption {
	return func(o *readerOpts) {
		o.decOpts = append(o.decOpts, opts...)
	}
}

type reader struct {
	ctx   context.Context
	errCh chan error
	wg    *sync.WaitGroup
	dc    *Decompressor
}

// NewReader returns an io.Reader that uses a scanner and decompressor to decompress
// bzip2 data concurrently.
func NewReader(ctx context.Context, rd io.Reader, opts ...ReaderOption) io.Reader {
	rdOpts := &readerOpts{}
	for _, fn := range opts {
		fn(rdOpts)
	}
	sc := NewScanner(rd, rdOpts.scanOpts...)
	dc := NewDecompressor(ctx, rdOpts.decOpts...)

	errCh := make(chan error, 1)
	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func() {
		errCh <- decompress(ctx, sc, dc)
		close(errCh)
		wg.Done()
	}()
	return &reader{
		ctx:   ctx,
		errCh: errCh,
		dc:    dc,
		wg:    wg,
	}
}

// decompress guarantees that it Finish will have been called on the
// decompressor. Any non-nil error it returns should be returned by the
// final call to Read.
func decompress(ctx context.Context, sc *Scanner, dc *Decompressor) error {
	if err := scan(ctx, sc, dc); err != nil {
		dc.Cancel(err)
		dc.Finish()
		return err
	}
	return dc.Finish()
}

// scan runs the scanner against the input stream invoking the decompressor
// to add each block to the set to decompressed.
func scan(ctx context.Context, sc *Scanner, dc *Decompressor) error {
	for sc.Scan(ctx) {
		block := sc.Block()
		if err := dc.Append(block); err != nil {
			return err
		}
	}
	return sc.Err()
}

// handleErrorOrCancel returns an error returned by the decompression goroutine
// above or if the context is canceled.
func (rd *reader) handleErrorOrCancel() error {
	select {
	case err := <-rd.errCh:
		return err
	case <-rd.ctx.Done():
		return rd.ctx.Err()
	default:
		return nil
	}
}

// Read implements io.Reader.
func (rd *reader) Read(buf []byte) (int, error) {
	// test for any errors prior to calling Read which may block
	// if we don't handle context cancelation here and in particular
	// call Cancel on the decompressor.
	if err := rd.handleErrorOrCancel(); err != nil {
		rd.dc.Cancel(err)
		rd.wg.Wait() // wait for internal goroutine to finish.
		return 0, err
	}
	n, err := rd.dc.Read(buf)
	if err == nil {
		return n, nil
	}

	rd.wg.Wait() // wait for internal goroutine to finish.

	// make sure to catch errors sent after the decompressor is done
	// such as a CRC error.
	select {
	case cerr := <-rd.errCh:
		if err != io.EOF {
			return n, err
		}
		if cerr != nil {
			return n, cerr
		}
	default:
	}
	return n, err
}
