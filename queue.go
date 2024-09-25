package goqueue

import (
	"context"
	"sync"
)

type Queue struct {
	mu        sync.Mutex
	q         []Data
	qStartPos int
	ctx       context.Context
}

// In adds a new Data element to the Queue. If the Queue's context has been canceled,
// it returns an error; otherwise, it locks the Queue, appends the data, and unlocks it.
func (q *Queue) In(data Data) (err error) {
	select {
	case <-q.ctx.Done():
		return q.ctx.Err()
	default:
		q.mu.Lock()
		q.q = append(q.q, data)
		q.mu.Unlock()
		return nil
	}
}

// Out retrieves the next element from the Queue as a pointer to QueueData.
// If the context has been canceled, it returns an error. If the Queue is empty,
// it returns nil without error.
func (q *Queue) Out() (data *QueueData, err error) {
	select {
	case <-q.ctx.Done():
		return nil, q.ctx.Err()
	default:
		q.mu.Lock()
		if len(q.q) > 0 {
			if q.qStartPos >= len(q.q) {
				q.qStartPos = 0
			}
			return &QueueData{
				q:   q,
				pos: q.qStartPos,
			}, nil
		}
		q.mu.Unlock()
		return nil, nil
	}
}

func (q *Queue) inMany(qData []Data) error {
	select {
	case <-q.ctx.Done():
		return q.ctx.Err()
	default:
		q.mu.Lock()
		q.q = append(q.q, qData...)
		q.mu.Unlock()
		return nil
	}
}

// New initializes and returns a new Queue and a context.CancelFunc.
// The Queue starts with an empty slice of Data, and a context to manage cancellation.
// The cancel function can be used to terminate any operations associated with the Queue's context.
func New() (*Queue, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())

	q := Queue{
		q:         make([]Data, 0),
		qStartPos: 0,
		ctx:       ctx,
	}

	return &q, cancel
}
