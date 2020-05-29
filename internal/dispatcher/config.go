package dispatcher

// Config of the dispatcher package
type Config struct {
	LoopDelay   int64 `env:"DISPATCHER_LOOP_DELAY" envDefault:"1000"`
	SequenceTTL int64 `env:"DISPATCHER_SEQUENCE_TTL" envDefault:"5000"`
}
