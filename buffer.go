// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2013 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"io"
	"net"
	"time"
)

const defaultBufSize = 4096
const tinyBufferSize = 64

// A buffer which is used for both reading and writing.
// This is possible since communication on each connection is synchronous.
// In other words, we can't write and read simultaneously on the same connection.
// The buffer is similar to bufio.Reader / Writer but zero-copy-ish
// Also highly optimized for this particular use case.
type buffer struct {
	buf     []byte // buf is a byte buffer who's length and capacity are equal.
	large   []byte
	safe    []byte
	nc      net.Conn
	idx     int
	length  int
	timeout time.Duration
}

// newBuffer allocates and returns a new buffer.
func newBuffer(nc net.Conn) buffer {
	buf := make([]byte, defaultBufSize)
	return buffer{
		buf:   buf,
		large: buf,
		safe:  make([]byte, tinyBufferSize),
		nc:    nc,
	}
}

var scratchBuffer [4096]byte

func (b *buffer) skip(need int) error {
	if b.length >= need {
		b.idx += need
		b.length -= need
		return nil
	}

	need -= b.length
	b.idx += b.length
	b.length = 0

	for need > 0 {
		r := need
		if r > len(scratchBuffer) {
			r = len(scratchBuffer)
		}
		nn, err := b.read(scratchBuffer[:r])
		need -= nn

		switch err {
		case nil:
			continue
		case io.EOF:
			if need == 0 {
				return nil
			}
			return io.ErrUnexpectedEOF
		default:
			return err
		}
	}
	return nil
}

func (b *buffer) read(out []byte) (int, error) {
	if b.timeout > 0 {
		if err := b.nc.SetReadDeadline(time.Now().Add(b.timeout)); err != nil {
			return 0, err
		}
	}
	return b.nc.Read(out)
}

// fill reads into the buffer until at least _need_ bytes are in it
func (b *buffer) fill(need int, safe bool) error {
	n := b.length

	if safe {
		copy(b.safe[0:n], b.buf[b.idx:])
		b.buf = b.safe
	} else {
		// move existing data to the beginning
		if n > 0 && b.idx > 0 {
			copy(b.large[0:n], b.buf[b.idx:])
		}
		b.buf = b.large
	}

	// grow buffer if necessary
	if need > len(b.buf) {
		// Round up to the next multiple of the default size
		newBuf := make([]byte, ((need/defaultBufSize)+1)*defaultBufSize)
		copy(newBuf, b.buf)
		b.buf = newBuf
	}

	b.idx = 0

	for {
		nn, err := b.read(b.buf[n:])
		n += nn

		switch err {
		case nil:
			if n < need {
				continue
			}
			b.length = n
			return nil

		case io.EOF:
			if n >= need {
				b.length = n
				return nil
			}
			return io.ErrUnexpectedEOF

		default:
			return err
		}
	}
}

func (b *buffer) peekByte(safe bool) (byte, error) {
	if b.length < 1 {
		// refill
		if err := b.fill(1, safe); err != nil {
			return 0, err
		}
	}
	return b.buf[b.idx], nil
}

// returns next N bytes from buffer.
// The returned slice is only guaranteed to be valid until the next read
func (b *buffer) readNext(need int, safe bool) ([]byte, error) {
	if b.length < need {
		// refill
		if err := b.fill(need, safe); err != nil {
			return nil, err
		}
	}

	offset := b.idx
	b.idx += need
	b.length -= need
	return b.buf[offset:b.idx], nil
}

// takeBuffer returns a buffer with the requested size.
// If possible, a slice from the existing buffer is returned.
// Otherwise a bigger buffer is made.
// Only one buffer (total) can be used at a time.
func (b *buffer) takeBuffer(length int) ([]byte, error) {
	if b.length > 0 {
		return nil, ErrBusyBuffer
	}

	// restore original buffer if it's been resized or replaced
	b.buf = b.large

	// test (cheap) general case first
	if length <= cap(b.buf) {
		return b.buf[:length], nil
	}

	if length < maxPacketSize {
		b.buf = make([]byte, length)
		return b.buf, nil
	}

	// buffer is larger than we want to store.
	return make([]byte, length), nil
}

// takeSmallBuffer is shortcut which can be used if length is
// known to be smaller than defaultBufSize.
// Only one buffer (total) can be used at a time.
func (b *buffer) takeSmallBuffer(length int) ([]byte, error) {
	if b.length > 0 {
		return nil, ErrBusyBuffer
	}
	// restore original buffer if it's been resized or replaced
	b.buf = b.large
	return b.buf[:length], nil
}

// takeCompleteBuffer returns the complete existing buffer.
// This can be used if the necessary buffer size is unknown.
// cap and len of the returned buffer will be equal.
// Only one buffer (total) can be used at a time.
func (b *buffer) takeCompleteBuffer() ([]byte, error) {
	if b.length > 0 {
		return nil, ErrBusyBuffer
	}
	// restore original buffer if it's been resized or replaced
	b.buf = b.large
	return b.buf, nil
}

// store stores buf, an updated buffer, if its suitable to do so.
func (b *buffer) store(buf []byte) error {
	if b.length > 0 {
		return ErrBusyBuffer
	} else if cap(buf) <= maxPacketSize && cap(buf) > cap(b.buf) {
		b.buf = buf[:cap(buf)]
	}
	return nil
}
