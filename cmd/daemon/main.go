package main

import (
	"fmt"
	"sync"

	"github.com/go-pg/pg/v9"

	"github.com/wavesplatform/transaction-broadcaster/internal/config"
	"github.com/wavesplatform/transaction-broadcaster/internal/dispatcher"
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
		PoolSize: cfg.Pg.PoolSize,
	})

	repo := repository.New(db)

	nodeInteractor := node.New(cfg.Node.NodeURL, cfg.Node.NodeAPIKey, cfg.Node.WaitForTxStatusDelay, cfg.Node.WaitForTxTimeout, cfg.Node.WaitForNextHeightDelay)

	disp := dispatcher.New(repo, nodeInteractor, cfg.Dispatcher.LoopDelay, cfg.Dispatcher.SequenceTTL, cfg.Worker.TxOutdateTime, cfg.Worker.TxProcessingTTL, cfg.Worker.HeightsAfterLastTx, cfg.Worker.WaitForNextHeightDelay)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		if err := disp.RunLoop(); err != nil {
			panic(err)
		}
	}()

	logger.Info("dispatcher started")

	wg.Wait()
}
