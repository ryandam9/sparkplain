package eventlog

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

const fixtures = "../../../testdata/eventlog"
const mainApp = "application_1790380000000_0042"

func readAllCodec(t *testing.T, name string, data []byte) ([]byte, string, error) {
	t.Helper()
	r, codec, note, done, err := decompressor(bytes.NewReader(data), name)
	if err != nil {
		return nil, codec, err
	}
	defer done()
	if note != "" {
		t.Logf("note: %s", note)
	}
	out, err := io.ReadAll(r)
	return out, codec, err
}

// The compressed fixtures were written by Spark's own CompressionCodec, so
// decoding them to the same bytes as the plain log proves the readers match
// the Java stream formats.
func TestCodecsMatchPlainFixture(t *testing.T) {
	t.Parallel()
	plain, err := os.ReadFile(filepath.Join(fixtures, mainApp))
	if err != nil {
		t.Fatal(err)
	}
	for _, codec := range []string{CodecLZ4, CodecZstd, CodecSnappy} {
		t.Run(codec, func(t *testing.T) {
			name := mainApp + "." + codec
			data, err := os.ReadFile(filepath.Join(fixtures, name))
			if err != nil {
				t.Fatal(err)
			}
			if got := codecFromMagic(data[:8]); got != codec {
				t.Fatalf("magic says %q, want %q", got, codec)
			}
			out, got, err := readAllCodec(t, name, data)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if got != codec {
				t.Fatalf("codec %q, want %q", got, codec)
			}
			if !bytes.Equal(out, plain) {
				t.Fatalf("decoded %d bytes, differ from %d plain bytes", len(out), len(plain))
			}
		})
	}
}

func TestTruncatedStreamsReportTruncation(t *testing.T) {
	t.Parallel()
	for _, codec := range []string{CodecLZ4, CodecZstd, CodecSnappy} {
		t.Run(codec, func(t *testing.T) {
			name := mainApp + "." + codec
			data, err := os.ReadFile(filepath.Join(fixtures, name))
			if err != nil {
				t.Fatal(err)
			}
			out, _, err := readAllCodec(t, name, data[:len(data)*2/3])
			if !errors.Is(err, errTruncated) && !errors.Is(err, errUnterminated) {
				t.Fatalf("want truncation error, got %v", err)
			}
			if len(out) == 0 {
				t.Fatal("expected the data before the cut to be returned")
			}
		})
	}
}

func TestExtensionMagicMismatchUsesMagic(t *testing.T) {
	t.Parallel()
	data, err := os.ReadFile(filepath.Join(fixtures, mainApp+".zstd"))
	if err != nil {
		t.Fatal(err)
	}
	_, codec, note, done, err := decompressor(bytes.NewReader(data), "renamed.lz4")
	if err != nil {
		t.Fatal(err)
	}
	done()
	if codec != CodecZstd || note == "" {
		t.Fatalf("codec %q note %q; want zstd with a note", codec, note)
	}
}

func TestCorruptLZ4ChecksumFails(t *testing.T) {
	t.Parallel()
	data, err := os.ReadFile(filepath.Join(fixtures, mainApp+".lz4"))
	if err != nil {
		t.Fatal(err)
	}
	bad := append([]byte(nil), data...)
	bad[17] ^= 0xff // first block's checksum
	_, _, err = readAllCodec(t, "x.lz4", bad)
	if err == nil || errors.Is(err, errTruncated) {
		t.Fatalf("want checksum error, got %v", err)
	}
}

func TestLZFRejected(t *testing.T) {
	t.Parallel()
	if _, _, _, _, err := decompressor(bytes.NewReader([]byte("ZV...")), "a.lzf"); err == nil {
		t.Fatal("lzf should be rejected")
	}
}

func TestXXH32KnownValues(t *testing.T) {
	t.Parallel()
	// Reference values from the xxHash specification test vectors.
	cases := []struct {
		in   string
		seed uint32
		want uint32
	}{
		{"", 0, 0x02cc5d05},
		{"a", 0, 0x550d7456},
		{"abc", 0, 0x32d153ff},
		{"Nobody inspects the spammish repetition", 0, 0xe2293b2f},
	}
	for _, c := range cases {
		if got := xxh32([]byte(c.in), c.seed); got != c.want {
			t.Errorf("xxh32(%q) = %08x, want %08x", c.in, got, c.want)
		}
	}
}

func fuzzSeeds(f *testing.F, codec string) {
	data, err := os.ReadFile(filepath.Join(fixtures, mainApp+"."+codec))
	if err != nil {
		f.Fatal(err)
	}
	f.Add(data[:min(len(data), 4096)])
	f.Add([]byte{})
}

func FuzzLZ4Block(f *testing.F) {
	fuzzSeeds(f, CodecLZ4)
	f.Fuzz(func(t *testing.T, b []byte) {
		_, _ = io.Copy(io.Discard, io.LimitReader(newLZ4BlockReader(bytes.NewReader(b)), 1<<26))
	})
}

func FuzzSnappyStream(f *testing.F) {
	fuzzSeeds(f, CodecSnappy)
	f.Fuzz(func(t *testing.T, b []byte) {
		s, err := newSnappyStreamReader(bytes.NewReader(b))
		if err != nil {
			return
		}
		_, _ = io.Copy(io.Discard, io.LimitReader(s, 1<<26))
	})
}
