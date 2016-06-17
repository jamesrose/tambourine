package tambourine

import "fmt"

type Queue struct {
	Name string
}

func (q Queue) PrefixedName(prefix string) string {
	return fmt.Sprintf("%s_%s", prefix, q.Name)
}

func (q Queue) PrefixedNameAndWorker(prefix string, worker string) string {
	return fmt.Sprintf("%s_%s_%s", prefix, worker, q.Name)
}
