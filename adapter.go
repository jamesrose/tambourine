package tambourine

type Adapter interface {
	publish(queue Queue, message Message) error
	consume(queue Queue, worker string) ([]Message, error)
}
