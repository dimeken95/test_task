package httpapi

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"mime"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"github.com/dimeken95/test_task/internal/config"
	"github.com/dimeken95/test_task/internal/domain"
)

type MediaKind string

const (
	KindDocument MediaKind = "document"
	KindImage    MediaKind = "image"
	KindVideo    MediaKind = "video"
)

// sniffLen is enough for every signature below; WebP needs 12 bytes and the
// ISO-BMFF ftyp box needs 12.
const sniffLen = 64

const (
	ctPDF  = "application/pdf"
	ctDOC  = "application/msword"
	ctDOCX = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
	ctXLSX = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
	ctPPTX = "application/vnd.openxmlformats-officedocument.presentationml.presentation"
)

// mediaRule pairs an accepted MIME type with the content check that must pass.
// Declared Content-Type alone is attacker-controlled, so every accepted type
// carries a magic-byte verifier — including video, where http.DetectContentType
// is useless.
type mediaRule struct {
	kind   MediaKind
	verify func(head []byte) bool
}

var allowed = map[string]mediaRule{
	ctPDF:  {KindDocument, hasPrefix("%PDF-")},
	ctDOC:  {KindDocument, hasPrefixBytes([]byte{0xD0, 0xCF, 0x11, 0xE0, 0xA1, 0xB1, 0x1A, 0xE1})}, // OLE2
	ctDOCX: {KindDocument, isZipContainer},
	ctXLSX: {KindDocument, isZipContainer},
	ctPPTX: {KindDocument, isZipContainer},

	"image/jpeg": {KindImage, hasPrefixBytes([]byte{0xFF, 0xD8, 0xFF})},
	"image/png":  {KindImage, hasPrefixBytes([]byte{0x89, 'P', 'N', 'G', 0x0D, 0x0A, 0x1A, 0x0A})},
	"image/gif":  {KindImage, anyOf(hasPrefix("GIF87a"), hasPrefix("GIF89a"))},
	"image/webp": {KindImage, isWebP},

	"video/mp4":       {KindVideo, isISOBMFF},
	"video/quicktime": {KindVideo, isISOBMFF},
	"video/webm":      {KindVideo, hasPrefixBytes([]byte{0x1A, 0x45, 0xDF, 0xA3})}, // EBML
}

var extToMIME = map[string]string{
	".pdf":  ctPDF,
	".doc":  ctDOC,
	".docx": ctDOCX,
	".xlsx": ctXLSX,
	".pptx": ctPPTX,
	".jpg":  "image/jpeg",
	".jpeg": "image/jpeg",
	".png":  "image/png",
	".webp": "image/webp",
	".gif":  "image/gif",
	".mp4":  "video/mp4",
	".webm": "video/webm",
	".mov":  "video/quicktime",
}

func hasPrefix(s string) func([]byte) bool {
	return hasPrefixBytes([]byte(s))
}

func hasPrefixBytes(magic []byte) func([]byte) bool {
	return func(head []byte) bool { return bytes.HasPrefix(head, magic) }
}

func anyOf(fns ...func([]byte) bool) func([]byte) bool {
	return func(head []byte) bool {
		for _, fn := range fns {
			if fn(head) {
				return true
			}
		}
		return false
	}
}

// isZipContainer covers the OOXML family: docx/xlsx/pptx are ZIP archives.
// Distinguishing them further would require reading the central directory,
// which defeats streaming; the ZIP check already rejects arbitrary binaries.
func isZipContainer(head []byte) bool {
	return bytes.HasPrefix(head, []byte("PK\x03\x04")) ||
		bytes.HasPrefix(head, []byte("PK\x05\x06"))
}

// isWebP: "RIFF" <4-byte size> "WEBP".
func isWebP(head []byte) bool {
	return len(head) >= 12 &&
		bytes.HasPrefix(head, []byte("RIFF")) &&
		bytes.Equal(head[8:12], []byte("WEBP"))
}

// isoBMFFBoxes are the top-level atom types a valid MP4/MOV may open with.
// Modern files lead with `ftyp`, but QuickTime predates it and can start with
// another top-level atom, so requiring `ftyp` alone rejects legitimate .mov.
var isoBMFFBoxes = [][]byte{
	[]byte("ftyp"), []byte("moov"), []byte("mdat"),
	[]byte("free"), []byte("skip"), []byte("wide"), []byte("pnot"),
}

// isISOBMFF matches the ISO base media file format container: a 4-byte
// big-endian box size followed by a 4-byte box type. Checking the size as well
// as the type keeps the widened type list from accepting arbitrary binaries.
func isISOBMFF(head []byte) bool {
	if len(head) < 12 {
		return false
	}
	// 0 means "extends to end of file" and 1 means "64-bit size follows";
	// any other value is a real length and cannot be smaller than the header.
	if size := binary.BigEndian.Uint32(head[:4]); size != 0 && size != 1 && size < 8 {
		return false
	}
	return slices.ContainsFunc(isoBMFFBoxes, func(box []byte) bool {
		return bytes.Equal(head[4:8], box)
	})
}

var sniffPool = sync.Pool{
	New: func() any {
		b := make([]byte, sniffLen)
		return &b
	},
}

func getSniffBuf() *[]byte {
	if buf, ok := sniffPool.Get().(*[]byte); ok && buf != nil {
		return buf
	}
	b := make([]byte, sniffLen)
	return &b
}

type ValidatedFile struct {
	ContentType string
	Kind        MediaKind
	MaxBytes    int64
	FileName    string
	// Reader replays the sniffed head followed by the rest of the body and
	// fails with ErrPayloadTooLarge once MaxBytes is exceeded.
	Reader io.Reader

	capped *cappedReader
}

// Exceeded reports whether the body ran past MaxBytes. The object store wraps
// reader errors in its own SDK types, so the handler asks the reader directly
// instead of relying on errors.Is surviving that chain.
func (v *ValidatedFile) Exceeded() bool { return v.capped != nil && v.capped.exceeded }

// ValidateFile resolves the effective content type, verifies it against the
// payload's magic bytes and returns a size-capped reader.
func ValidateFile(cfg config.Config, fileName, declaredCT string, r io.Reader) (*ValidatedFile, error) {
	ct := resolveContentType(fileName, declaredCT)

	rule, ok := allowed[ct]
	if !ok {
		return nil, fmt.Errorf("%w: %s", domain.ErrUnsupportedMedia, displayCT(ct))
	}

	bufPtr := getSniffBuf()
	defer sniffPool.Put(bufPtr)

	head := (*bufPtr)[:sniffLen]
	n, err := io.ReadFull(r, head)
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return nil, fmt.Errorf("%w: read file head: %w", domain.ErrInvalidInput, err)
	}
	head = head[:n]

	if !rule.verify(head) {
		return nil, fmt.Errorf("%w: content does not match declared type %s", domain.ErrUnsupportedMedia, ct)
	}

	// Copy out of the pooled buffer before it goes back.
	sniffed := make([]byte, n)
	copy(sniffed, head)

	max := maxForKind(cfg, rule.kind)
	capped := &cappedReader{r: io.MultiReader(bytes.NewReader(sniffed), r), max: max}

	return &ValidatedFile{
		ContentType: ct,
		Kind:        rule.kind,
		MaxBytes:    max,
		FileName:    fileName,
		Reader:      capped,
		capped:      capped,
	}, nil
}

// resolveContentType prefers the declared type and falls back to the extension
// when the client sends nothing useful (browsers and curl often send
// application/octet-stream).
func resolveContentType(fileName, declaredCT string) string {
	ct := normalizeCT(declaredCT)
	if ct == "" || ct == "application/octet-stream" {
		if m, ok := extToMIME[strings.ToLower(filepath.Ext(fileName))]; ok {
			return m
		}
	}
	return ct
}

func maxForKind(cfg config.Config, kind MediaKind) int64 {
	switch kind {
	case KindDocument:
		return cfg.MaxDocBytes
	case KindImage:
		return cfg.MaxImageBytes
	case KindVideo:
		return cfg.MaxVideoBytes
	default:
		return cfg.MaxDocBytes
	}
}

func normalizeCT(ct string) string {
	ct = strings.TrimSpace(strings.ToLower(ct))
	if ct == "" {
		return ""
	}
	media, _, err := mime.ParseMediaType(ct)
	if err != nil {
		return ct
	}
	return media
}

func displayCT(ct string) string {
	if ct == "" {
		return "(none)"
	}
	return ct
}

// cappedReader enforces the per-kind size limit while streaming, since
// multipart parts carry no length we could check up front.
type cappedReader struct {
	r        io.Reader
	max      int64
	n        int64
	exceeded bool
}

func (c *cappedReader) Read(p []byte) (int, error) {
	if c.exceeded {
		return 0, c.err()
	}
	// Read at most one byte past the limit: that extra byte is what proves the
	// payload is too large rather than exactly at the limit.
	if room := c.max + 1 - c.n; int64(len(p)) > room {
		p = p[:room]
	}
	n, err := c.r.Read(p)
	c.n += int64(n)
	if c.n > c.max {
		c.exceeded = true
		return 0, c.err()
	}
	return n, err
}

func (c *cappedReader) err() error {
	return fmt.Errorf("%w: limit is %d bytes", domain.ErrPayloadTooLarge, c.max)
}
