package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/go-pg/pg/v9"

	"go.uber.org/zap"

	"github.com/waves-exchange/broadcaster/internal/api"
	"github.com/waves-exchange/broadcaster/internal/config"
	"github.com/waves-exchange/broadcaster/internal/dispatcher"
	"github.com/waves-exchange/broadcaster/internal/log"
	"github.com/waves-exchange/broadcaster/internal/sequence"
	"github.com/waves-exchange/broadcaster/internal/waves"
	"github.com/waves-exchange/broadcaster/internal/worker"
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
	cancelChan := make(chan struct{})

	nodeInteractor := waves.NewNodeInteractor(cfg.NodeURL, cfg.NodeAPIKey)
	sequenceProcessingDelay := time.Duration(cfg.Worker.SequenceProcessingDelay) * time.Millisecond
	txProcessingDelay := time.Duration(cfg.Worker.TxProcessingDelay) * time.Millisecond

	var w worker.Worker
	for i := 0; i < cfg.Worker.Count; i++ {
		wg.Add(1)
		w = worker.New(service, nodeInteractor, sequenceChan, cancelChan, sequenceProcessingDelay, txProcessingDelay)
		go w.Start()
	}
	logger.Info("workers started", zap.Int("workers_count", cfg.Worker.Count))

	dispatcherDelay := time.Duration(cfg.Dispatcher.Delay) * time.Millisecond
	frozenDuration := time.Duration(cfg.Dispatcher.FrozenDuration) * time.Millisecond
	disp := dispatcher.Init(service, sequenceChan, cancelChan, dispatcherDelay, frozenDuration)

	wg.Add(1)
	go disp.RunLoop()
	logger.Info("dispatcher started", zap.Duration("delay", dispatcherDelay), zap.Duration("frozen_duration", frozenDuration))

	logger.Info("starting server", zap.Int("port", cfg.Port))
	s := api.Create(service, nodeInteractor)
	addr := fmt.Sprintf(":%d", cfg.Port)

	runError := s.Run(addr)
	if runError != nil {
		panic(runError)
	}

	// @todo close channels on sig*
	// close(cancelChan)
	// close(sequenceChan)

	wg.Wait()
}
