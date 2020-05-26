package dispatcher

import (
	"time"

	"github.com/waves-exchange/broadcaster/internal/waves"

	"github.com/waves-exchange/broadcaster/internal/log"
	"github.com/waves-exchange/broadcaster/internal/sequence"
	"github.com/waves-exchange/broadcaster/internal/worker"
	"go.uber.org/zap"
)

// Dispatcher ...
type Dispatcher interface {
	RunLoop()
}

type dispatcherImpl struct {
	service        sequence.Service
	nodeInteractor waves.NodeInteractor
	logger         *zap.Logger
	sequenceChan   chan int64
	resultsChan    chan worker.Result
	loopDelay      time.Duration
	sequenceTTL    time.Duration
}

// Create creates new Dispatcher
func Create(service sequence.Service, nodeInteractor waves.NodeInteractor, sequenceChan chan int64, loopDelay, sequenceTTL int64) Dispatcher {
	logger := log.Logger.Named("dispatcher")
	resultsChan := make(chan worker.Result)

	return &dispatcherImpl{
		service:        service,
		nodeInteractor: nodeInteractor,
		logger:         logger,
		sequenceChan:   sequenceChan,
		resultsChan:    resultsChan,
		loopDelay:      time.Duration(loopDelay) * time.Millisecond,
		sequenceTTL:    time.Duration(sequenceTTL) * time.Millisecond,
	}
}

// RunLoop starts dispatcher infinite loop listening sequenceChan for the next sequence to process
func (d *dispatcherImpl) RunLoop() {
	ticker := time.NewTicker(d.loopDelay)
	defer ticker.Stop()

	for {
		select {
		case seqID := <-d.sequenceChan:
			err := d.service.SetSequenceProcessingStateByID(seqID)
			if err != nil {
				d.logger.Error("error occured while setting sequence error state", zap.Error(err))
				return
			}
			d.runWorker(seqID)
		case res := <-d.resultsChan:
			if res.Error != nil {
				switch res.Error.(type) {
				case worker.RecoverableError:
					d.runWorker(res.SequenceID)
				case worker.NonRecoverableError:
					err := d.service.SetSequenceErrorStateByID(res.SequenceID, res.Error)
					if err != nil {
						d.logger.Error("error occured while setting sequence error state", zap.Error(err))
						return
					}
				case worker.FatalError:
					return
				default:
				}
			} else {
				err := d.service.SetSequenceDoneStateByID(res.SequenceID)
				if err != nil {
					d.logger.Error("error occured while setting sequence done state", zap.Error(err))
					return
				}
			}
		case <-ticker.C:
			hangingSequenceIds, err := d.service.GetHangingSequenceIds(d.sequenceTTL)
			if err != nil {
				d.logger.Error("error occured while getting hangins sequence ids", zap.Error(err))
				return
			}

			for _, seqID := range hangingSequenceIds {
				d.runWorker(seqID)
			}
		default:
			// nothing to do, just wait for the next message
		}
	}
}

func (d *dispatcherImpl) runWorker(seqID int64) {
	txProcessingTTL := int32(3000)
	nHeights := int32(6)
	waitForNextHeightDelay := int32(1000)
	w := worker.NewWorker(d.service, d.nodeInteractor, d.resultsChan, txProcessingTTL, nHeights, waitForNextHeightDelay)
	go w.Run(seqID)
}
