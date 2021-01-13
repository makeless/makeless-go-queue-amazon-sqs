package makeless_go_queue_amazon_sqs

import "sync"

type Queue struct {
	*sync.RWMutex
}
