package goqueue

import (
	"context"
	"sync"
)

type QueueWithStorage struct {
	mu sync.Mutex
	q  *Queue
	s  Storage
}

func (q *QueueWithStorage) In(data Data) (err error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	id, err := q.s.Insert(data)
	if err != nil {
		return err
	}

	data.ID = id

	if err := q.q.In(data); err != nil {
		return err
	}

	return nil
}

func (q *QueueWithStorage) Out() (qData *QueueStorageData, err error) {
	q.mu.Lock()

	data, err := q.q.Out()
	if err != nil {
		q.mu.Unlock()
		return nil, err
	}
	if data == nil {
		q.mu.Unlock()
		return nil, nil
	}

	return &QueueStorageData{
		q:  data,
		qs: q,
	}, nil
}

func NewWithStorage(s Storage) (*QueueWithStorage, context.CancelFunc, error) {
	q, cancel := New()

	data, err := s.GetAll()
	if err != nil {
		cancel()
		return nil, nil, err
	}

	if err := q.inMany(data); err != nil {
		cancel()
		return nil, nil, err
	}

	return &QueueWithStorage{
		q: q,
		s: s,
	}, cancel, nil
}
