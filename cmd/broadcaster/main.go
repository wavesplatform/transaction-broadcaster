package main

import (
	"fmt"
	"sync"

	"github.com/go-pg/pg/v9"

	"go.uber.org/zap"

	"github.com/waves-exchange/broadcaster/internal/api"
	"github.com/waves-exchange/broadcaster/internal/config"
	"github.com/waves-exchange/broadcaster/internal/dispatcher"
	"github.com/waves-exchange/broadcaster/internal/log"
	"github.com/waves-exchange/broadcaster/internal/sequence"
	"github.com/waves-exchange/broadcaster/internal/waves"
)

var wg sync.WaitGroup

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

	repo := sequence.NewRepo(db)
	service := sequence.NewService(repo)

	sequenceChan := make(chan int64)

	nodeInteractor := waves.NewNodeInteractor(cfg.Waves.NodeURL, cfg.Waves.NodeAPIKey, cfg.Waves.WaitForTxStatusDelay, cfg.Waves.WaitForTxTimeout)

	disp := dispatcher.Create(service, nodeInteractor, sequenceChan, cfg.Dispatcher.LoopDelay, cfg.Dispatcher.SequenceTTL)

	wg.Add(1)
	go func() {
		disp.RunLoop()
		wg.Done()
	}()
	logger.Info("dispatcher started")

	s := api.Create(service, nodeInteractor, sequenceChan)
	addr := fmt.Sprintf(":%d", cfg.Port)

	logger.Info("starting REST API server", zap.Int("port", cfg.Port))
	runError := s.Run(addr)
	if runError != nil {
		panic(runError)
	}

	wg.Wait()
}
