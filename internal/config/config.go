package config

import (
	"github.com/caarlos0/env/v6"

	"github.com/waves-exchange/broadcaster/internal/dispatcher"
	"github.com/waves-exchange/broadcaster/internal/node"
	"github.com/waves-exchange/broadcaster/internal/repository"
	"github.com/waves-exchange/broadcaster/internal/worker"
)

// Config of the app
type Config struct {
	Port int  `env:"PORT" envDefault:"3000"`
	Dev  bool `env:"DEV" envDefault:"false"`

	Pg         repository.PgConfig
	Dispatcher dispatcher.Config
	Worker     worker.Config
	Node       node.Config
}

// Load returns config from environment variables
func Load() (*Config, error) {
	c := Config{}

	if err := env.Parse(&c); err != nil {
		return nil, err
	}

	if err := env.Parse(&c.Pg); err != nil {
		return nil, err
	}

	if err := env.Parse(&c.Dispatcher); err != nil {
		return nil, err
	}

	if err := env.Parse(&c.Worker); err != nil {
		return nil, err
	}

	if err := env.Parse(&c.Node); err != nil {
		return nil, err
	}

	return &c, nil
}
