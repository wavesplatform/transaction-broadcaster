package waves

import "net/url"

// Config of the waves package
type Config struct {
	NodeURL              url.URL `env:"NODE_URL,required"`
	NodeAPIKey           string  `env:"NODE_API_KEY,required"`
	WaitForTxStatusDelay int32   `env:"WAIT_FOR_TX_STATUS_DELAY" envDefault:"1000"`
	WaitForTxTimeout     int32   `env:"WAIT_FOR_TX_TIMEOUT" envDefault:"1000"`
}
