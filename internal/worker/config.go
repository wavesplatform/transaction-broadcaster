package worker

// Config of the worker
type Config struct {
	TxProcessingTTL       int32 `env:"TX_PROCESSING_TTL" envDefault:"3000"`
	BlocksAfterLastTx     int32 `env:"BLOCKS_AFTER_LAST_TX" envDefault:"6"`
	WaitForNextBlockDelay int32 `env:"WAIT_FOR_NEXT_BLOCK_DELAY" envDefaul:"1000"`
}
