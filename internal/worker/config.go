package worker

// Config of the worker
type Config struct {
	TxOutdateTime          int32 `env:"WORKER_TX_OUTDATE_TIME" envDefault:"14400000"`
	TxProcessingTTL        int32 `env:"WORKER_TX_PROCESSING_TTL" envDefault:"3000"`
	HeightsAfterLastTx     int32 `env:"WORKER_HEIGHTS_AFTER_LAST_TX" envDefault:"6"`
	WaitForNextHeightDelay int32 `env:"WORKER_WAIT_FOR_NEXT_HEIGHT_DELAY" envDefaul:"1000"`
}
