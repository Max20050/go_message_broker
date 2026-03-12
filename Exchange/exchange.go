package exchange

import (
	"fmt"
	"strings"
	"sync"

	"github.com/Max20050/go_message_broker/models"
	"github.com/Max20050/go_message_broker/queues"
)

// -----------------------------------------------------------------------
// Exchange interface & registry
// -----------------------------------------------------------------------

// Exchange defines how a message is routed to bound queues.
type Exchange interface {
	Name() string
	Type() string
	// Bind registers a queue with the given routing key.
	Bind(routingKey string, q *queues.Queue)
	// Unbind removes a binding.
	Unbind(routingKey string, queueName string)
	// Route delivers a message to matching bound queues.
	Route(routingKey string, msg models.StoredMessage) error
	// Bindings returns a map of routing keys to bound queue names.
	Bindings() map[string][]string
}

// ExchangeInfo holds metadata about an exchange for the admin panel.
type ExchangeInfo struct {
	Name     string        `json:"name"`
	Type     string        `json:"type"`
	Bindings []BindingInfo `json:"bindings"`
}

// BindingInfo describes a single binding in an exchange.
type BindingInfo struct {
	RoutingKey string `json:"routing_key"`
	QueueName  string `json:"queue_name"`
}

// -----------------------------------------------------------------------
// Registry – the server holds one of these to manage all exchanges.
// -----------------------------------------------------------------------

type Registry struct {
	mu        sync.RWMutex
	exchanges map[string]Exchange
}

func NewRegistry() *Registry {
	r := &Registry{
		exchanges: make(map[string]Exchange),
	}
	// The default exchange is a nameless direct exchange.
	// Publishing with Exchange="" uses the routing key as the queue name.
	r.exchanges[""] = NewDirectExchange("")
	return r
}

func (r *Registry) Declare(name string, kind string) (Exchange, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if ex, exists := r.exchanges[name]; exists {
		if ex.Type() != kind {
			return nil, fmt.Errorf("exchange %q already exists with type %s, cannot redeclare as %s", name, ex.Type(), kind)
		}
		return ex, nil
	}

	var ex Exchange
	switch kind {
	case "direct":
		ex = NewDirectExchange(name)
	case "fanout":
		ex = NewFanoutExchange(name)
	case "topic":
		ex = NewTopicExchange(name)
	default:
		return nil, fmt.Errorf("unknown exchange type: %s", kind)
	}
	r.exchanges[name] = ex
	return ex, nil
}

func (r *Registry) Get(name string) (Exchange, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ex, ok := r.exchanges[name]
	return ex, ok
}

// ListAll returns metadata for every registered exchange.
func (r *Registry) ListAll() []ExchangeInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]ExchangeInfo, 0, len(r.exchanges))
	for _, ex := range r.exchanges {
		info := ExchangeInfo{
			Name: ex.Name(),
			Type: ex.Type(),
		}
		for rk, queueNames := range ex.Bindings() {
			for _, qn := range queueNames {
				info.Bindings = append(info.Bindings, BindingInfo{
					RoutingKey: rk,
					QueueName:  qn,
				})
			}
		}
		result = append(result, info)
	}
	return result
}

// -----------------------------------------------------------------------
// Direct Exchange
// -----------------------------------------------------------------------

// DirectExchange routes a message to queues whose binding key exactly matches
// the routing key.  The default ("") exchange auto-binds every queue by name.
type DirectExchange struct {
	name string
	mu   sync.RWMutex
	// routingKey -> list of bound queues
	bindings map[string][]*queues.Queue
}

func NewDirectExchange(name string) *DirectExchange {
	return &DirectExchange{
		name:     name,
		bindings: make(map[string][]*queues.Queue),
	}
}

func (d *DirectExchange) Name() string { return d.name }
func (d *DirectExchange) Type() string { return "direct" }

func (d *DirectExchange) Bind(routingKey string, q *queues.Queue) {
	d.mu.Lock()
	defer d.mu.Unlock()
	// Avoid duplicate bindings
	for _, existing := range d.bindings[routingKey] {
		if existing.Name == q.Name {
			return
		}
	}
	d.bindings[routingKey] = append(d.bindings[routingKey], q)
}

func (d *DirectExchange) Unbind(routingKey string, queueName string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	qs := d.bindings[routingKey]
	for i, q := range qs {
		if q.Name == queueName {
			d.bindings[routingKey] = append(qs[:i], qs[i+1:]...)
			return
		}
	}
}

func (d *DirectExchange) Bindings() map[string][]string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	result := make(map[string][]string)
	for rk, qs := range d.bindings {
		for _, q := range qs {
			result[rk] = append(result[rk], q.Name)
		}
	}
	return result
}

func (d *DirectExchange) Route(routingKey string, msg models.StoredMessage) error {
	d.mu.RLock()
	targets := d.bindings[routingKey]
	d.mu.RUnlock()

	if len(targets) == 0 {
		return fmt.Errorf("no queue bound with routing key %q on exchange %q", routingKey, d.name)
	}

	for _, q := range targets {
		m := msg // copy per queue
		m.Head.QueueName = q.Name
		q.Enqueue(m)
	}
	return nil
}

// -----------------------------------------------------------------------
// Fanout Exchange
// -----------------------------------------------------------------------

// FanoutExchange delivers every message to ALL bound queues regardless
// of the routing key.
type FanoutExchange struct {
	name string
	mu   sync.RWMutex
	// We ignore routing keys; just keep a set of queues.
	queues []*queues.Queue
}

func NewFanoutExchange(name string) *FanoutExchange {
	return &FanoutExchange{
		name:   name,
		queues: make([]*queues.Queue, 0),
	}
}

func (f *FanoutExchange) Name() string { return f.name }
func (f *FanoutExchange) Type() string { return "fanout" }

func (f *FanoutExchange) Bind(_ string, q *queues.Queue) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, existing := range f.queues {
		if existing.Name == q.Name {
			return
		}
	}
	f.queues = append(f.queues, q)
}

func (f *FanoutExchange) Unbind(_ string, queueName string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i, q := range f.queues {
		if q.Name == queueName {
			f.queues = append(f.queues[:i], f.queues[i+1:]...)
			return
		}
	}
}

func (f *FanoutExchange) Bindings() map[string][]string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	result := make(map[string][]string)
	for _, q := range f.queues {
		result[""] = append(result[""], q.Name)
	}
	return result
}

func (f *FanoutExchange) Route(_ string, msg models.StoredMessage) error {
	f.mu.RLock()
	targets := make([]*queues.Queue, len(f.queues))
	copy(targets, f.queues)
	f.mu.RUnlock()

	if len(targets) == 0 {
		return fmt.Errorf("no queues bound to fanout exchange %q", f.name)
	}

	for _, q := range targets {
		m := msg
		m.Head.QueueName = q.Name
		q.Enqueue(m)
	}
	return nil
}

// -----------------------------------------------------------------------
// Topic Exchange
// -----------------------------------------------------------------------

// TopicExchange matches the routing key against patterns.
// Pattern rules (same as AMQP):
//   - Words are separated by "."
//   - "*" matches exactly one word
//   - "#" matches zero or more words
type TopicExchange struct {
	name string
	mu   sync.RWMutex
	// pattern -> list of bound queues
	bindings map[string][]*queues.Queue
}

func NewTopicExchange(name string) *TopicExchange {
	return &TopicExchange{
		name:     name,
		bindings: make(map[string][]*queues.Queue),
	}
}

func (t *TopicExchange) Name() string { return t.name }
func (t *TopicExchange) Type() string { return "topic" }

func (t *TopicExchange) Bind(routingKey string, q *queues.Queue) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, existing := range t.bindings[routingKey] {
		if existing.Name == q.Name {
			return
		}
	}
	t.bindings[routingKey] = append(t.bindings[routingKey], q)
}

func (t *TopicExchange) Unbind(routingKey string, queueName string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	qs := t.bindings[routingKey]
	for i, q := range qs {
		if q.Name == queueName {
			t.bindings[routingKey] = append(qs[:i], qs[i+1:]...)
			return
		}
	}
}

func (t *TopicExchange) Bindings() map[string][]string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	result := make(map[string][]string)
	for rk, qs := range t.bindings {
		for _, q := range qs {
			result[rk] = append(result[rk], q.Name)
		}
	}
	return result
}

func (t *TopicExchange) Route(routingKey string, msg models.StoredMessage) error {
	t.mu.RLock()
	// Collect all matching queues (deduplicated by name).
	matched := make(map[string]*queues.Queue)
	for pattern, qs := range t.bindings {
		if topicMatch(pattern, routingKey) {
			for _, q := range qs {
				matched[q.Name] = q
			}
		}
	}
	t.mu.RUnlock()

	if len(matched) == 0 {
		return fmt.Errorf("no queue matched routing key %q on topic exchange %q", routingKey, t.name)
	}

	for _, q := range matched {
		m := msg
		m.Head.QueueName = q.Name
		q.Enqueue(m)
	}
	return nil
}

// topicMatch checks if a routing key matches an AMQP-style topic pattern.
func topicMatch(pattern, routingKey string) bool {
	patternParts := strings.Split(pattern, ".")
	routingParts := strings.Split(routingKey, ".")
	return matchParts(patternParts, routingParts)
}

func matchParts(pattern, routing []string) bool {
	pi, ri := 0, 0
	for pi < len(pattern) && ri < len(routing) {
		switch pattern[pi] {
		case "#":
			// '#' at the end matches everything remaining.
			if pi == len(pattern)-1 {
				return true
			}
			// Try matching the rest of the pattern from every position.
			for ri2 := ri; ri2 <= len(routing); ri2++ {
				if matchParts(pattern[pi+1:], routing[ri2:]) {
					return true
				}
			}
			return false
		case "*":
			// '*' matches exactly one word.
			pi++
			ri++
		default:
			if pattern[pi] != routing[ri] {
				return false
			}
			pi++
			ri++
		}
	}
	// Consume trailing '#'
	for pi < len(pattern) && pattern[pi] == "#" {
		pi++
	}
	return pi == len(pattern) && ri == len(routing)
}
