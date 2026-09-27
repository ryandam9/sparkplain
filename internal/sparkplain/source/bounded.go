package source

import (
	"fmt"
	"io"
	"sync"
)

// Bounded returns a reader that passes through at most limit bytes of r and
// then fails with a ClassTooLarge error if r holds more, instead of
// stopping quietly as io.LimitReader does: a log cut at the limit must not
// look complete. what names the limit in the error, for example
// "-max-unpacked". A reader that holds exactly limit bytes reads cleanly.
func Bounded(r io.Reader, limit int64, where, what string) io.Reader {
	return &bounded{r: r, left: limit, limit: limit, where: where, what: what}
}

type bounded struct {
	r           io.Reader
	left, limit int64
	where, what string
	err         error
}

func (b *bounded) Read(p []byte) (int, error) {
	if b.err != nil {
		return 0, b.err
	}
	if b.left <= 0 {
		// At the limit: one more byte means the content is larger.
		var one [1]byte
		for {
			n, err := b.r.Read(one[:])
			if n > 0 {
				b.err = &Error{Class: ClassTooLarge, Key: b.where, Err: fmt.Errorf("unpacks to more than %d bytes (%s)", b.limit, b.what)}
				return 0, b.err
			}
			if err != nil {
				return 0, err
			}
		}
	}
	if int64(len(p)) > b.left {
		p = p[:b.left]
	}
	n, err := b.r.Read(p)
	b.left -= int64(n)
	return n, err
}

// budget is a counting semaphore over bytes: Fetch holds a zip's size from
// it while the zip sits in memory, so several workers cannot each hold a
// large zip at once.
type budget struct {
	mu   sync.Mutex
	cond *sync.Cond
	free int64
}

func newBudget(n int64) *budget {
	b := &budget{free: n}
	b.cond = sync.NewCond(&b.mu)
	return b
}

func (b *budget) acquire(n int64) {
	b.mu.Lock()
	for b.free < n {
		b.cond.Wait()
	}
	b.free -= n
	b.mu.Unlock()
}

func (b *budget) release(n int64) {
	b.mu.Lock()
	b.free += n
	b.mu.Unlock()
	b.cond.Broadcast()
}
