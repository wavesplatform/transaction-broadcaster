package main

import (
	"fmt"

	"github.com/go-pg/pg/v9"

	"go.uber.org/zap"

	"github.com/wavesplatform/transaction-broadcaster/internal/api"
	"github.com/wavesplatform/transaction-broadcaster/internal/config"
	"github.com/wavesplatform/transaction-broadcaster/internal/log"
	"github.com/wavesplatform/transaction-broadcaster/internal/node"
	"github.com/wavesplatform/transaction-broadcaster/internal/repository"
)

func main() {
	cfg, cfgErr := config.Load()
	if cfgErr != nil {
		panic(cfgErr)
	}

	if logInitErr := log.Init(cfg.Dev); logInitErr != nil {
		panic(logInitErr)
	}

	logger := log.Logger.Named("main.main")
	logger.Info("successfull init")

	db := pg.Connect(&pg.Options{
		Addr:     fmt.Sprintf("%s:%d", cfg.Pg.Host, cfg.Pg.Port),
		User:     cfg.Pg.User,
		Database: cfg.Pg.Database,
		Password: cfg.Pg.Password,
	})

	repo := repository.New(db)

	nodeInteractor := node.New(cfg.Node.NodeURL, cfg.Node.NodeAPIKey, cfg.Node.WaitForTxStatusDelay, cfg.Node.WaitForTxTimeout, cfg.Node.WaitForNextHeightDelay)

	s := api.New(repo, nodeInteractor)
	addr := fmt.Sprintf(":%d", cfg.Port)

	logger.Info("starting REST API server", zap.Int("port", cfg.Port))
	if runError := s.Run(addr); runError != nil {
		panic(runError)
	}
}
