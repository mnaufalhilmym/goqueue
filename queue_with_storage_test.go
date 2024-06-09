package goqueue_test

import (
	"encoding/json"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	queue "github.com/mnaufalhilmym/goqueue"
)

type simpleStorage struct {
	mu sync.Mutex
	d  []queue.Data
}

func (s *simpleStorage) Insert(data queue.Data) (ID int64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	data.ID = 1
	if len(s.d) > 0 {
		data.ID = s.d[len(s.d)-1].ID + 1
	}
	s.d = append(s.d, data)

	return data.ID, nil
}

func (s *simpleStorage) GetAll() ([]queue.Data, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	d := make([]queue.Data, len(s.d))
	copy(d, s.d)
	return d, nil
}

func (s *simpleStorage) Delete(id int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	deleteIdx := -1
	for i, sd := range s.d {
		if sd.ID == id {
			deleteIdx = i
			break
		}
	}
	if deleteIdx >= 0 {
		s.d[deleteIdx] = s.d[len(s.d)-1]
		s.d = s.d[:len(s.d)-1]
	} else {
		return errors.New("Invalid data index")
	}

	return nil
}

func newSimpleStorage() simpleStorage {
	return simpleStorage{
		d: make([]queue.Data, 0),
	}
}

func TestInOutWithStorage(t *testing.T) {
	data := []testData{
		{"test", []byte("test-data")},
		{"test1", []byte("test-data1")},
		{"test2", []byte("test-data2")},
		{"test3", []byte("test-data3")},
		{"test4", []byte("test-data4")},
		{"test5", []byte("test-data5")},
		{"test6", []byte("test-data6")},
		{"test7", []byte("test-data7")},
		{"retry", []byte("test-retry")},
		{"test8", []byte("test-data8")},
		{"test9", []byte("test-data9")},
		{"test10", []byte("test-data10")},
		{"test11", []byte("test-data11")},
		{"test12", []byte("test-data12")},
		{"skipped", []byte("test-skipped")},
		{"test13", []byte("test-data13")},
		{"test14", []byte("test-data14")},
	}

	storage := newSimpleStorage()
	for i, d := range data {
		if i == 5 {
			break
		}
		dd, err := json.Marshal(d)
		if err != nil {
			t.Error(err)
			return
		}
		if _, err := storage.Insert(queue.Data{
			ID:    int64(i + 1),
			Bytes: dd,
		}); err != nil {
			t.Error(err)
			return
		}
	}

	q, cancel, err := queue.NewWithStorage(&storage)
	if err != nil {
		t.Error(err)
		return
	}
	defer cancel()

	fs := func(data []testData) {
		for _, d := range data {
			dd, err := json.Marshal(d)
			if err != nil {
				t.Error(err)
				return
			}
			if err := q.In(queue.Data{
				Bytes: dd,
			}); err != nil {
				t.Error(err)
				return
			}
		}
	}

	go fs(data[5 : 5+(len(data)-5)/2])
	go fs(data[5+(len(data)-5)/2:])

	resmu := new(sync.Mutex)
	var res []testData
	f := func(wg *sync.WaitGroup) {
		defer wg.Done()
		x := 0
		for {
			resmu.Lock()
			isDone := false
			if len(res) != len(data) {
				qData, err := q.Out()
				if err != nil {
					t.Error(err)
					return
				}
				if qData == nil {
					time.Sleep(100 * time.Microsecond)
					resmu.Unlock()
					continue
				}
				var data testData
				if err := json.Unmarshal(qData.Data().Bytes, &data); err != nil {
					t.Error(err, string(qData.Data().Bytes))
					os.Exit(1)
				}
				if data.Kind == "retry" && x == 0 {
					qData.Cancel()
					resmu.Unlock()
					x++
					continue
				}
				if data.Kind == "skipped" && x == 0 {
					qData.Skip()
					resmu.Unlock()
					x++
					continue
				}
				x = 0
				res = append(res, data)
				if err := qData.Remove(); err != nil {
					t.Error(err)
					return
				}
			} else {
				isDone = true
			}
			resmu.Unlock()
			if isDone {
				break
			}
		}
	}

	wg := new(sync.WaitGroup)
	wg.Add(1)
	go f(wg)
	wg.Add(1)
	go f(wg)
	wg.Wait()

resloop:
	for _, r := range res {
		idx := -1
		for ii, d := range data {
			if d.Kind == "skipped" {
				continue resloop
			}
			if d.Kind == r.Kind {
				idx = ii
				if r.Kind != d.Kind {
					t.Error("Invalid data[0]")
					return
				}
				if len(r.Data) != len(d.Data) {
					t.Error("Invalid data[1]")
					return
				}
				for ii, rd := range r.Data {
					if rd != d.Data[ii] {
						t.Error("Invalid data[1]")
						return
					}
				}
				break
			}
		}
		if idx < 0 {
			t.Error("Invalid data")
		}
	}
}
