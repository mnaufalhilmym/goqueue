package goqueue

type Data struct {
	ID    int64
	Bytes []byte
}

type QueueData struct {
	q   *Queue
	pos int
}

// `Cancel` releases the lock on the `Queue`. It is typically called when an operation
// is abandoned, ensuring the `Queue` is not left in a locked state.
func (q *QueueData) Cancel() {
	q.q.mu.Unlock()
}

// `Skip` advances the current position in the `Queue` to the next item.
// If at the end of the `Queue`, it wraps around to the beginning.
func (q *QueueData) Skip() {
	defer q.q.mu.Unlock()

	if q.q.qStartPos < len(q.q.q)-1 {
		q.q.qStartPos++
	} else {
		q.q.qStartPos = 0
	}
}

// `Remove` deletes the current item from the `Queue`. It shifts the elements
// and removes the item at the current position, adjusting the `Queue` accordingly.
func (q *QueueData) Remove() {
	defer q.q.mu.Unlock()

	q.q.q = append(q.q.q[:q.pos], q.q.q[q.pos+1:]...)
}

// `Data` returns the `Data` item at the current position in the `Queue`.
func (q *QueueData) Data() Data {
	return q.q.q[q.pos]
}

type QueueStorageData struct {
	q  *QueueData
	qs *QueueWithStorage
}

// `Cancel` releases the lock on the `QueueWithStorage` and cancels the current operation in the in-memory `Queue`.
// It ensures that the lock is properly released even if the operation is abandoned.
func (q *QueueStorageData) Cancel() {
	defer q.qs.mu.Unlock()
	q.q.Cancel()
}

// `Skip` advances the current position in the in-memory `Queue` to the next item,
// while also releasing the lock on `QueueWithStorage`. It ensures proper management of the queue.
func (q *QueueStorageData) Skip() {
	defer q.qs.mu.Unlock()
	q.q.Skip()
}

// `Remove` deletes the current item from both the persistent `Storage` and the in-memory `Queue`.
// It ensures that the item is removed from `Storage` first, and then from the `Queue`.
// Returns an `error` if the deletion from `Storage` fails.
func (q *QueueStorageData) Remove() error {
	defer q.qs.mu.Unlock()

	if err := q.qs.s.Delete(q.Data().ID); err != nil {
		return err
	}

	q.q.Remove()

	return nil
}

// `Data` returns the current `Data` item from the in-memory `Queue` for further processing.
func (q *QueueStorageData) Data() Data {
	return q.q.Data()
}
