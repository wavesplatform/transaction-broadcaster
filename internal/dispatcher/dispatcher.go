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
			d.logger.Debug("got new sequence", zap.Int64("sequence_id", seqID))
			err := d.service.SetSequenceStateByID(seqID, sequence.SequenceStateProcessing)
			if err != nil {
				d.logger.Error("error occured while setting sequence processing state", zap.Error(err))
				return
			}
			d.runWorker(seqID)
		case res := <-d.resultsChan:
			d.logger.Debug("got new result", zap.Int64("sequence_id", res.SequenceID), zap.Bool("has_error", res.Error != nil))
			if res.Error != nil {
				switch res.Error.(type) {
				case worker.RecoverableError:
					d.logger.Debug("recoverable error", zap.String("message", res.Error.Error()))

					d.runWorker(res.SequenceID)
				case worker.NonRecoverableError:
					d.logger.Debug("non-recoverable error", zap.String("message", res.Error.Error()))

					err := d.service.SetSequenceErrorStateByID(res.SequenceID, res.Error)
					if err != nil {
						d.logger.Error("error occured while setting sequence error state", zap.Error(err))
						return
					}
				case worker.FatalError:
					d.logger.Debug("fatal error", zap.String("message", res.Error.Error()))

					return
				default:
				}
			} else {
				err := d.service.SetSequenceStateByID(res.SequenceID, sequence.SequenceStateDone)
				if err != nil {
					d.logger.Error("error occured while setting sequence done state", zap.Error(err))
					return
				}
			}
		case <-ticker.C:
			d.logger.Debug("next ticker tick")
			hangingSequenceIds, err := d.service.GetHangingSequenceIds(d.sequenceTTL)
			if err != nil {
				d.logger.Error("error occured while getting hangins sequence ids", zap.Error(err))
				return
			}

			for _, seqID := range hangingSequenceIds {
				// refresh sequence status
				err := d.service.SetSequenceStateByID(seqID, sequence.SequenceStateProcessing)
				if err != nil {
					d.logger.Error("error occurred while updating sequence state", zap.Error(err), zap.Int64("sequence_id", seqID))
					return
				}

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
