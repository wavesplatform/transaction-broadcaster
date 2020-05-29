package main

import (
	"fmt"

	"github.com/go-pg/pg/v9"

	"go.uber.org/zap"

	"github.com/waves-exchange/broadcaster/internal/api"
	"github.com/waves-exchange/broadcaster/internal/config"
	"github.com/waves-exchange/broadcaster/internal/dispatcher"
	"github.com/waves-exchange/broadcaster/internal/log"
	"github.com/waves-exchange/broadcaster/internal/node"
	"github.com/waves-exchange/broadcaster/internal/repository"
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

	sequenceChan := make(chan int64)

	nodeInteractor := node.New(cfg.Node.NodeURL, cfg.Node.NodeAPIKey, cfg.Node.WaitForTxStatusDelay, cfg.Node.WaitForTxTimeout, cfg.Node.WaitForNextHeightDelay)

	disp := dispatcher.New(repo, nodeInteractor, sequenceChan, cfg.Dispatcher.LoopDelay, cfg.Dispatcher.SequenceTTL, cfg.Worker.TxProcessingTTL, cfg.Worker.HeightsAfterLastTx, cfg.Worker.WaitForNextHeightDelay)

	go func() {
		if err := disp.RunLoop(); err != nil {
			panic(err)
		}
	}()
	logger.Info("dispatcher started")

	s := api.New(repo, nodeInteractor, sequenceChan)
	addr := fmt.Sprintf(":%d", cfg.Port)

	logger.Info("starting REST API server", zap.Int("port", cfg.Port))
	if runError := s.Run(addr); runError != nil {
		panic(runError)
	}
}
