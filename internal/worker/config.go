package worker

// Config of the worker
type Config struct {
	TxProcessingTTL        int32 `env:"WORKER_TX_PROCESSING_TTL" envDefault:"3000"`
	BlocksAfterLastTx      int32 `env:"WORKER_BLOCKS_AFTER_LAST_TX" envDefault:"6"`
	WaitForNextHeightDelay int32 `env:"WORKER_WAIT_FOR_NEXT_HEIGHT_DELAY" envDefaul:"1000"`
}
