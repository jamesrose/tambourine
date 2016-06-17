package tambourine

type Session struct {
	adapter Adapter
}

func NewSession(adapter Adapter) Session {
	return Session{adapter: adapter}
}

func (s Session) Publish(queue Queue, message Message) error {
	return s.adapter.publish(queue, message)
}

func (s Session) Consume(queue Queue, worker string) ([]Message, error) {
	return s.adapter.consume(queue, worker)
}
