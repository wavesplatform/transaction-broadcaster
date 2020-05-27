package waves

import "net/url"

// Config of the waves package
type Config struct {
	NodeURL              url.URL `env:"WAVES_NODE_URL,required"`
	NodeAPIKey           string  `env:"WAVES_NODE_API_KEY,required"`
	WaitForTxStatusDelay int32   `env:"WAVES_WAIT_FOR_TX_STATUS_DELAY" envDefault:"1000"`
	WaitForTxTimeout     int32   `env:"WAVES_WAIT_FOR_TX_TIMEOUT" envDefault:"1000"`
}
