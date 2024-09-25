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

// `In` inserts the given `Data` into both the underlying `Storage` and `Queue`.
// It first stores the `Data` in `Storage`, then assigns the generated ID to the `Data`
// before adding it to the in-memory `Queue`. Returns an error if insertion fails.
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

// `Out` retrieves the next element from the in-memory `Queue` as a `QueueStorageData`.
// It locks the `Queue`, fetches the next item, and if successful, returns it wrapped
// in a `QueueStorageData` struct. If no data is available or an `error` occurs, it returns `nil`.
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

// `NewWithStorage` initializes and returns a `QueueWithStorage` and a `context.CancelFunc`.
// It populates the queue with data retrieved from the provided `Storage`.
// If data retrieval or insertion fails, it returns an `error` and cancels the context.
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
