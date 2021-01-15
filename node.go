package makeless_go_queue_amazon_sqs

import (
	"github.com/makeless/makeless-go/queue"
	"sync"
)

type Node struct {
	Data          []byte
	ReceiptHandle string

	next makeless_go_queue.Node
	*sync.RWMutex
}

func (node *Node) GetData() []byte {
	node.RLock()
	defer node.RUnlock()

	return node.Data
}

func (node *Node) GetReceiptHandle() string {
	node.RLock()
	defer node.RUnlock()

	return node.ReceiptHandle
}

func (node *Node) GetNext() makeless_go_queue.Node {
	node.RLock()
	defer node.RUnlock()

	return node.next
}

func (node *Node) SetNext(next makeless_go_queue.Node) {
	node.Lock()
	defer node.Unlock()

	node.next = next
}
