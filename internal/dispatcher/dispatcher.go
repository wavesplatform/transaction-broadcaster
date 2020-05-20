package dispatcher

import (
	"time"

	"github.com/waves-exchange/broadcaster/internal/log"
	"github.com/waves-exchange/broadcaster/internal/sequence"
	"go.uber.org/zap"
)

// Config of the dispatcher
type Config struct {
	Delay          int64 `env:"DISPATCHER_DELAY" envDefault:"1000"`
	FrozenDuration int64 `env:"DISPATCHER_FROZEN_DURATION" envDefault:"600000"`
}

// Dispatcher ...
type Dispatcher interface {
	RunLoop()
}

type dispatcherImpl struct {
	service        sequence.Service
	logger         *zap.Logger
	sequenceChan   chan<- int64
	cancelChan     <-chan struct{}
	delay          time.Duration
	frozenDuration time.Duration
}

// Init creates new Dispatcher
func Init(service sequence.Service, sequenceChan chan<- int64, cancelChan <-chan struct{}, delay time.Duration, frozenDuration time.Duration) Dispatcher {
	logger := log.Logger.Named("dispatcher")
	return &dispatcherImpl{
		service:        service,
		logger:         logger,
		sequenceChan:   sequenceChan,
		cancelChan:     cancelChan,
		delay:          delay,
		frozenDuration: frozenDuration,
	}
}

// RunLoop starts dispatcher infinite loop listening sequenceChan for the next sequence to process
func (d *dispatcherImpl) RunLoop() {
	for {
		select {
		case <-d.cancelChan:
			d.logger.Info("got message from cancelChan")
			return
		default:
			d.logger.Debug("dispatcher new loop")

			// Try to find new free (not under processing) sequences
			freeIds, err := d.service.GetFreeSequenceIds()
			if err != nil {
				d.logger.Error("error occured on getting free sequence ids", zap.Error(err))
				return
			}

			if len(freeIds) > 0 {
				d.logger.Debug("there are new free sequences", zap.Int64s("ids", freeIds))
			}

			for _, id := range freeIds {
				d.sequenceChan <- id
			}

			// Try to find frozen sequences (after crashes)
			frozenIds, err := d.service.GetFrozenSequenceIds(d.frozenDuration)
			if err != nil {
				d.logger.Error("error occured on getting frozen sequence ids", zap.Error(err))
				return
			}

			if len(frozenIds) > 0 {
				d.logger.Debug("there are frozen sequences", zap.Int64s("ids", frozenIds))
			}

			for _, id := range frozenIds {
				d.sequenceChan <- id
			}

			time.Sleep(d.delay)
		}
	}
}
