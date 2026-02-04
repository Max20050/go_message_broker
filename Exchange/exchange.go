package exchange

type Exchange interface {
}

type DirectExchange struct{}

type FanoutExchange struct{}

type TopicExchange struct{}
