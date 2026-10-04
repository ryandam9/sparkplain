package source

import "sync/atomic"

// Progress counts the work a long read has done, so the command line can
// show how far it is. Readers add to it as they go, and the totals grow as
// they find more to read; any goroutine may update or read it.
type Progress struct {
	Bytes, BytesTotal atomic.Int64 // bytes read, as stored
	Files, FilesTotal atomic.Int64 // files read
}

// Add counts a file to read of size bytes.
func (p *Progress) Add(size int64) {
	if p == nil {
		return
	}
	p.FilesTotal.Add(1)
	p.BytesTotal.Add(size)
}

// Read counts n bytes read.
func (p *Progress) Read(n int64) {
	if p != nil {
		p.Bytes.Add(n)
	}
}

// Done counts a file read.
func (p *Progress) Done() {
	if p != nil {
		p.Files.Add(1)
	}
}
