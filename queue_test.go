package goqueue_test

import (
	"encoding/json"
	"sync"
	"testing"
	"time"

	queue "github.com/mnaufalhilmym/goqueue"
)

type testData struct {
	Kind string
	Data []byte
}

func TestInOut(t *testing.T) {
	data := []testData{
		{"test", []byte("test-data")},
		{"test1", []byte("test-data1")},
		{"test2", []byte("test-data2")},
		{"test3", []byte("test-data3")},
		{"test4", []byte("test-data4")},
		{"test5", []byte("test-data5")},
		{"test6", []byte("test-data6")},
		{"test7", []byte("test-data7")},
		{"test8", []byte("test-data8")},
		{"test9", []byte("test-data9")},
	}

	q, cancel := queue.New()
	defer cancel()

	fs := func(data []testData) {
		for i, d := range data {
			dd, err := json.Marshal(d)
			if err != nil {
				t.Error(err)
				return
			}
			if err := q.In(queue.Data{
				ID:    int64(i + 1),
				Bytes: dd,
			}); err != nil {
				t.Error(err)
				return
			}
		}
	}

	go fs(data[:len(data)/2])
	go fs(data[len(data)/2:])

	resmu := new(sync.Mutex)
	var res []testData
	f := func(wg *sync.WaitGroup) {
		defer wg.Done()
		for {
			resmu.Lock()
			isDone := false
			if len(res) != len(data) {
				time.Sleep(100 * time.Microsecond)
				qData, err := q.Out()
				if err != nil {
					t.Error(err)
					return
				}
				if qData == nil {
					resmu.Unlock()
					continue
				}
				var data testData
				if err := json.Unmarshal(qData.Data().Bytes, &data); err != nil {
					t.Error(err)
					return
				}
				res = append(res, data)
				qData.Remove()
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

	for _, r := range res {
		idx := -1
		for ii, d := range data {
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
