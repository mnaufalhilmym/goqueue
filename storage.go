package goqueue

type Storage interface {
	Insert(data Data) (id int64, err error)
	GetAll() ([]Data, error)
	Delete(id int64) error
}
