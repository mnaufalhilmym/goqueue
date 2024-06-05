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

func (q *Queue) Out() (data *QueueData, err error) {
	select {
	case <-q.ctx.Done():
		return nil, q.ctx.Err()
	default:
		q.mu.Lock()
		if len(q.q) > 0 {
			if q.qStartPos < len(q.q)-1 {
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

func New() (*Queue, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())

	q := Queue{
		q:         make([]Data, 0),
		qStartPos: 0,
		ctx:       ctx,
	}

	return &q, cancel
}
