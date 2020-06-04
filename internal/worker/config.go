package worker

// Config of the worker
type Config struct {
	TxProcessingTTL            int32 `env:"WORKER_TX_PROCESSING_TTL" envDefault:"3000"`
	HeightsAfterLastTx         int32 `env:"WORKER_HEIGHTS_AFTER_LAST_TX" envDefault:"6"`
	WaitForNextHeightDelay     int32 `env:"WORKER_WAIT_FOR_NEXT_HEIGHT_DELAY" envDefaul:"1000"`
	NumberOfRevalidateAttempts int16 `env:"WORKER_NUMBER_OF_REVALIDATE_ATTEMPTS" envDefault:"6"`
}
