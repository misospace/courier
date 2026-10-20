package git

import (
	"bufio"
	"compress/zlib"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
)

// ObjectLimits bounds an untrusted object set. Every field must be positive:
// an unset bound is rejected rather than read as "unbounded".
type ObjectLimits struct {
	MaxObjects int
	// MaxTotalBytes bounds two things at once: the unpacked size of the
	// delivered object set, and the bytes the scan itself may inflate. The
	// two are close but not identical — a delta's declared size is the size
	// of its delta stream, not of the object the delta reconstructs — so a
	// bundle can trip the byte bound before its delivered total is over it.
	// The bound fails closed either way: the scan never inflates more than
	// the byte budget allows, and a bundle it refuses is never handed to
	// git.
	MaxTotalBytes  int64
	MaxObjectBytes int64
}

// LimitError reports which bound an untrusted object set exceeded. It carries
// a fixed limit name only; untrusted content never appears in it.
type LimitError struct{ Limit string }

// Error names the exceeded bound. The name is one of the Limit constants.
func (e *LimitError) Error() string {
	return "git object limit exceeded: " + e.Limit
}

// The bounds a LimitError can name.
const (
	LimitObjects     = "objects"      // object-count bound
	LimitBytes       = "bytes"        // total unpacked-byte bound
	LimitObjectBytes = "object-bytes" // single-object (blob) bound
)

// The pack object types this scan understands. Types 1-4 are whole objects;
// 6 and 7 are deltas against a base this scan never resolves.
const (
	packTypeCommit   = 1
	packTypeTree     = 2
	packTypeBlob     = 3
	packTypeTag      = 4
	packTypeOFSDelta = 6
	packTypeREFDelta = 7
)

// validObjectLimits fails closed: a bound that is not positive is never
// treated as "no bound".
func validObjectLimits(limits ObjectLimits) error {
	if limits.MaxObjects <= 0 || limits.MaxTotalBytes <= 0 || limits.MaxObjectBytes <= 0 {
		return errors.New("git object limits: every bound must be positive")
	}
	return nil
}

// scanBundlePack walks the packfile inside an untrusted bundle and enforces
// the object budget against the pack's own declarations. Nothing is inflated
// beyond what the bounds allow and no inflated byte is ever held: the scan
// exists so a bundle that is over budget is rejected before `git bundle
// unbundle` inflates it at all.
//
// A bound that trips is reported as *LimitError; a bundle whose pack does not
// parse is reported as an ordinary error.
func scanBundlePack(ctx context.Context, bundlePath string, limits ObjectLimits) error {
	if err := validObjectLimits(limits); err != nil {
		return err
	}
	file, err := os.Open(bundlePath)
	if err != nil {
		return fmt.Errorf("git bundle scan: %w", err)
	}
	defer file.Close()
	packStart, err := findPackStart(file)
	if err != nil {
		return err
	}
	if _, err := file.Seek(packStart, io.SeekStart); err != nil {
		return fmt.Errorf("git bundle scan: %w", err)
	}
	return scanPackfile(ctx, bufio.NewReader(file), limits)
}

// findPackStart returns the offset of the packfile that follows a git
// bundle's header block. The header is a text block terminated by a blank
// line and the packfile starts immediately after it.
func findPackStart(file *os.File) (int64, error) {
	buffer := make([]byte, 32*1024)
	var offset int64
	previous := byte(0)
	for {
		n, err := file.Read(buffer)
		for i := 0; i < n; i++ {
			if buffer[i] == '\n' && previous == '\n' {
				return offset + int64(i) + 1, nil
			}
			previous = buffer[i]
		}
		offset += int64(n)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return 0, errors.New("git bundle scan: malformed bundle: no packfile")
			}
			return 0, fmt.Errorf("git bundle scan: %w", err)
		}
	}
}

// scanPackfile walks every object of one packfile and enforces the budget.
// The walk stops at the first violation; nothing after a violation is read.
//
// The scanner reads the pack's own declarations and is deliberately not
// identical to what git accepts. It refuses a pack with trailing bytes after
// its trailer, which git's index-pack tolerates, and it never applies a
// delta, so it does not prove that a delta reconstructs its base — git does
// that when it unpacks, and the pack's own checksum is checked there too.
// Both differences fail closed: a bundle this scanner accepts is one git
// still verifies, and a bundle this scanner refuses never reaches git.
//
// The pack is SHA-1: the trailer and a reference delta's base object ID are
// SHA-1 sized, so a SHA-256 bundle is refused as malformed rather than
// misread.
func scanPackfile(ctx context.Context, reader *bufio.Reader, limits ObjectLimits) error {
	header := make([]byte, 12)
	if _, err := io.ReadFull(reader, header); err != nil {
		return errors.New("git bundle scan: malformed pack header")
	}
	if string(header[0:4]) != "PACK" {
		return errors.New("git bundle scan: malformed pack header")
	}
	switch binary.BigEndian.Uint32(header[4:8]) {
	case 2, 3:
	default:
		return errors.New("git bundle scan: unsupported pack version")
	}
	count := uint64(binary.BigEndian.Uint32(header[8:12]))
	// The pack header states the object count before a single object is
	// touched, so an over-large pack is refused without any inflation.
	if count > uint64(limits.MaxObjects) {
		return &LimitError{Limit: LimitObjects}
	}
	var unpackedTotal, payloadTotal uint64
	for index := uint64(0); index < count; index++ {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("git bundle scan: %w", err)
		}
		objectType, size, err := readPackObjectHeader(reader)
		if err != nil {
			return errors.New("git bundle scan: malformed object header")
		}
		delta := false
		switch objectType {
		case packTypeCommit, packTypeTree, packTypeBlob, packTypeTag:
		case packTypeOFSDelta:
			delta = true
			if err := skipPackOffset(reader); err != nil {
				return errors.New("git bundle scan: malformed delta offset")
			}
		case packTypeREFDelta:
			delta = true
			var base [20]byte
			if _, err := io.ReadFull(reader, base[:]); err != nil {
				return errors.New("git bundle scan: malformed delta base")
			}
		default:
			return errors.New("git bundle scan: unknown object type")
		}
		if !delta {
			// A whole object's declared size is its unpacked size, so it
			// is refused before a single byte of it is inflated.
			if size > uint64(limits.MaxObjectBytes) {
				return &LimitError{Limit: LimitObjectBytes}
			}
			if err := addScanTotal(&unpackedTotal, size, limits.MaxTotalBytes); err != nil {
				return err
			}
		}
		// The declared payload is what this scan has to inflate; bounding it
		// bounds the scan's own work.
		if err := addScanTotal(&payloadTotal, size, limits.MaxTotalBytes); err != nil {
			return err
		}
		unpacked, err := inflatePackObject(reader, size, delta)
		if err != nil {
			return err
		}
		if delta {
			// A delta's declared size is the size of the delta stream; the
			// stream's own header declares the size of the object it
			// produces, which is that object's unpacked size.
			if unpacked > uint64(limits.MaxObjectBytes) {
				return &LimitError{Limit: LimitObjectBytes}
			}
			if err := addScanTotal(&unpackedTotal, unpacked, limits.MaxTotalBytes); err != nil {
				return err
			}
		}
	}
	// The pack ends with a 20-byte checksum. Reading it and nothing more
	// proves the walk kept the pack's structure.
	trailer := make([]byte, 20)
	if _, err := io.ReadFull(reader, trailer); err != nil {
		return errors.New("git bundle scan: malformed pack trailer")
	}
	if _, err := reader.ReadByte(); err == nil {
		return errors.New("git bundle scan: trailing data after the pack")
	} else if !errors.Is(err, io.EOF) {
		return errors.New("git bundle scan: malformed pack trailer")
	}
	return nil
}

// addScanTotal adds one declared size to a running total and refuses the
// object set when the total passes the bound.
func addScanTotal(total *uint64, amount uint64, limit int64) error {
	if amount > uint64(limit)-*total {
		return &LimitError{Limit: LimitBytes}
	}
	*total += amount
	return nil
}

// readPackObjectHeader reads one variable-length pack object header and
// reports the object's type and its declared size.
func readPackObjectHeader(reader *bufio.Reader) (int, uint64, error) {
	current, err := reader.ReadByte()
	if err != nil {
		return 0, 0, err
	}
	objectType := int((current >> 4) & 0x07)
	size := uint64(current & 0x0f)
	shift := uint(4)
	for current&0x80 != 0 {
		current, err = reader.ReadByte()
		if err != nil {
			return 0, 0, err
		}
		if shift >= 64 {
			return 0, 0, errors.New("git bundle scan: object size overflow")
		}
		size |= uint64(current&0x7f) << shift
		shift += 7
	}
	return objectType, size, nil
}

// skipPackOffset consumes git's negative-offset encoding that precedes an
// offset delta's payload. The base object is never resolved.
func skipPackOffset(reader *bufio.Reader) error {
	current, err := reader.ReadByte()
	if err != nil {
		return err
	}
	for current&0x80 != 0 {
		current, err = reader.ReadByte()
		if err != nil {
			return err
		}
	}
	return nil
}

// inflatePackObject inflates one object's payload and reports the unpacked
// size of the object it produces: the declared size for a whole object, the
// delta target size for a delta. The inflated bytes are never held; they are
// counted and discarded as they stream by.
//
// The payload must inflate to exactly its declared size: a payload that
// yields more is refused rather than allowed to inflate without limit, and a
// payload that yields less is a malformed pack.
func inflatePackObject(reader *bufio.Reader, size uint64, delta bool) (uint64, error) {
	// The reader is a *bufio.Reader so the zlib reader reads the compressed
	// stream byte by byte and never reads past the end of this object's
	// stream; the next object's header would otherwise be misread.
	stream, err := zlib.NewReader(reader)
	if err != nil {
		return 0, errors.New("git bundle scan: malformed object payload")
	}
	defer stream.Close()
	var consumed uint64
	target := uint64(0)
	if delta {
		// The delta stream header declares the base size and the size of the
		// object the delta produces.
		_, first, err := readDeltaVarint(stream)
		if err != nil {
			return 0, err
		}
		second, read, err := readDeltaVarint(stream)
		if err != nil {
			return 0, err
		}
		consumed += uint64(first + read)
		target = second
	}
	if consumed > size {
		return 0, errors.New("git bundle scan: object payload exceeds its declared size")
	}
	remaining := size - consumed
	limit := int64(remaining)
	if limit < math.MaxInt64 {
		// One byte past the declared size, so an over-long payload is
		// detected instead of being silently accepted.
		limit++
	}
	copied, err := io.Copy(io.Discard, io.LimitReader(stream, limit))
	consumed += uint64(copied)
	if consumed > size {
		return 0, errors.New("git bundle scan: object payload exceeds its declared size")
	}
	if consumed < size {
		return 0, errors.New("git bundle scan: truncated object payload")
	}
	if err != nil {
		return 0, errors.New("git bundle scan: malformed object payload")
	}
	return target, nil
}

// readDeltaVarint reads one little-endian base-128 varint from a delta
// stream header and reports how many bytes it consumed.
func readDeltaVarint(stream io.Reader) (uint64, int, error) {
	var value uint64
	var shift uint
	for read := 1; read <= 10; read++ {
		var one [1]byte
		if _, err := io.ReadFull(stream, one[:]); err != nil {
			return 0, 0, errors.New("git bundle scan: malformed delta header")
		}
		value |= uint64(one[0]&0x7f) << shift
		if one[0]&0x80 == 0 {
			return value, read, nil
		}
		shift += 7
	}
	return 0, 0, errors.New("git bundle scan: malformed delta header")
}
