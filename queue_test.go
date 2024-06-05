package goqueue_test

import (
	"sync"
	"testing"
	"time"

	queue "github.com/mnaufalhilmym/goqueue"
)

func TestInOut(t *testing.T) {
	data := [][]any{
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

	fs := func(data [][]any) {
		for i, d := range data {
			if err := q.In(queue.Data{
				ID:   int64(i + 1),
				Kind: d[0].(string),
				Data: d[1].([]byte),
			}); err != nil {
				t.Error(err)
				return
			}
		}
	}

	go fs(data[:len(data)/2])
	go fs(data[len(data)/2:])

	resmu := new(sync.Mutex)
	var res [][]any
	f := func(wg *sync.WaitGroup) {
		defer wg.Done()
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
				res = append(res, []any{qData.Data().Kind, qData.Data().Data})
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
			if d[0].(string) == r[0].(string) {
				idx = ii
				if r[0].(string) != d[0].(string) {
					t.Error("Invalid data[0]")
					return
				}
				if len(r[1].([]byte)) != len(d[1].([]byte)) {
					t.Error("Invalid data[1]")
					return
				}
				for ii, rd := range r[1].([]byte) {
					if rd != d[1].([]byte)[ii] {
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
