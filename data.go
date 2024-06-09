package goqueue

type Data struct {
	ID    int64
	Bytes []byte
}

type QueueData struct {
	q   *Queue
	pos int
}

func (q *QueueData) Cancel() {
	q.q.mu.Unlock()
}

func (q *QueueData) Skip() {
	defer q.q.mu.Unlock()

	if q.q.qStartPos < len(q.q.q)-1 {
		q.q.qStartPos++
	} else {
		q.q.qStartPos = 0
	}
}

func (q *QueueData) Remove() {
	defer q.q.mu.Unlock()

	q.q.q = append(q.q.q[:q.pos], q.q.q[q.pos+1:]...)
}

func (q *QueueData) Data() Data {
	return q.q.q[q.pos]
}

type QueueStorageData struct {
	q  *QueueData
	qs *QueueWithStorage
}

func (q *QueueStorageData) Cancel() {
	defer q.qs.mu.Unlock()
	q.q.Cancel()
}

func (q *QueueStorageData) Skip() {
	defer q.qs.mu.Unlock()
	q.q.Skip()
}

func (q *QueueStorageData) Remove() error {
	defer q.qs.mu.Unlock()

	if err := q.qs.s.Delete(q.Data().ID); err != nil {
		return err
	}

	q.q.Remove()

	return nil
}

func (q *QueueStorageData) Data() Data {
	return q.q.Data()
}
