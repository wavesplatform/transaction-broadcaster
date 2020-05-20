package worker

import (
	"time"

	"github.com/waves-exchange/broadcaster/internal/log"
	"github.com/waves-exchange/broadcaster/internal/sequence"
	"github.com/waves-exchange/broadcaster/internal/waves"
	"go.uber.org/zap"
)

// Config of the worker
type Config struct {
	Count                   int   `env:"WORKERS_COUNT" envDefault:"20"`
	SequenceProcessingDelay int64 `env:"WORKERS_SEQUENCE_PROCESSING_DELAY" envDefault:"1000"`
	TxProcessingDelay       int64 `env:"WORKERS_TX_PROCESSING_DELAY" envDefault:"1000"`
}

// Worker ...
type Worker interface {
	Start()
}

type workerImpl struct {
	service                 sequence.Service
	nodeInteractor          waves.NodeInteractor
	logger                  *zap.Logger
	sequenceChan            <-chan int64
	cancelChan              <-chan struct{}
	sequenceProcessingDelay time.Duration
	txProcessingDelay       time.Duration
}

// New creates new worker
func New(service sequence.Service, nodeInteractor waves.NodeInteractor, sequenceChan <-chan int64, cancelChan <-chan struct{}, sequenceProcessingDelay, txProcessingDelay time.Duration) Worker {
	logger := log.Logger.Named("worker")

	return &workerImpl{
		service:                 service,
		nodeInteractor:          nodeInteractor,
		logger:                  logger,
		sequenceChan:            sequenceChan,
		cancelChan:              cancelChan,
		sequenceProcessingDelay: sequenceProcessingDelay,
		txProcessingDelay:       txProcessingDelay,
	}
}

// Start starts worker
func (w *workerImpl) Start() {
	for {
		select {
		case <-w.cancelChan:
			return
		case sequenceID := <-w.sequenceChan:
			w.logger.Debug("processing sequence", zap.Int64("sequence_id", sequenceID))

			// already broadcasted txs are not necessary
			seq, err := w.service.GetSequenceByID(sequenceID)
			if err != nil {
				w.logger.Error("error occured while getting sequence", zap.Error(err), zap.Int64("sequence_id", sequenceID))
				return
			}

			// take sequence for processing
			err = w.service.UpdateSequenceProcessingState(sequenceID, true)
			if err != nil {
				w.logger.Error("error occured while updating sequence processing state", zap.Error(err), zap.Int64("sequence_id", sequenceID))
				return
			}

			txs, err := w.service.GetSequenceTxsByID(sequenceID, seq.BroadcastedCount)
			if err != nil {
				w.logger.Error("error occured while getting sequence txs", zap.Error(err))
				return
			}
			w.logger.Debug("there is sequence with not broadcasted txs", zap.Int64("sequence_id", sequenceID), zap.Int("txs_count", len(txs)))

			for _, tx := range txs {
				isValid, wavesErr := w.nodeInteractor.ValidateTx(tx)

				// just release sequence for next try
				if wavesErr != nil {
					w.logger.Error("error occured while validating tx", zap.Error(wavesErr), zap.Int64("sequence_id", sequenceID))

					err = w.service.UpdateSequenceProcessingState(sequenceID, false)
					if err != nil {
						w.logger.Error("error occured while updating sequence processing state", zap.Error(err))
					}
					return
				}

				// set sequence error state
				if !isValid {
					w.logger.Debug("invalid tx", zap.Int64("sequence_id", sequenceID))

					err = w.service.SetSequenceErrorStateAndRelease(sequenceID)
					if err != nil {
						w.logger.Error("error occured while setting sequence error state", zap.Error(err))

						err = w.service.UpdateSequenceProcessingState(sequenceID, false)
						if err != nil {
							w.logger.Error("error occured while updating sequence processing state", zap.Error(err))
						}
					}
					return
				}
				w.logger.Debug("broadcast valid tx", zap.Int64("sequence_id", sequenceID))

				// broadcast tx
				txID, wavesErr := w.nodeInteractor.BroadcastTx(tx)
				if wavesErr != nil {
					w.logger.Debug("tx was not broadcasted", zap.Error(wavesErr), zap.Int64("sequence_id", sequenceID))

					if wavesErr.Code() == waves.BroadcastClientError {
						err = w.service.SetSequenceErrorStateAndRelease(sequenceID)
						w.logger.Error("error occured while setting sequence error state", zap.Error(err))
						return
					}

					w.logger.Error("error occured while broadcasting tx", zap.Error(err), zap.Int64("sequence_id", sequenceID), zap.String("tx_id", txID))
					err = w.service.UpdateSequenceProcessingState(sequenceID, false)
					if err != nil {
						w.logger.Error("error occured while updating sequence processing state", zap.Error(err))
					}
					return
				}
				w.logger.Debug("tx was succefully broadcasted to the blockhain", zap.String("tx_id", txID))

				// wait for tx
				wavesErr = w.nodeInteractor.WaitForTx(txID)
				if wavesErr != nil {
					w.logger.Error("error occured while waiting for tx", zap.Error(wavesErr), zap.Int64("sequence_id", sequenceID), zap.String("tx_id", txID))

					err = w.service.UpdateSequenceProcessingState(sequenceID, false)
					if err != nil {
						w.logger.Error("error occured while updating sequence processing state", zap.Error(err))
					}
					return
				}

				// tx was succefully arrived to the blockhain
				w.logger.Debug("tx was succefully arrived to the blockhain", zap.String("tx_id", txID))
				err = w.service.IncreaseSequenceBroadcastedCount(sequenceID)
				if err != nil {
					w.logger.Error("error occured while increasing sequence broadcasted count", zap.Error(err))
					return
				}
				time.Sleep(w.txProcessingDelay)
			}

			// release sequence from processing
			err = w.service.UpdateSequenceProcessingState(sequenceID, false)
			if err != nil {
				w.logger.Error("error occured while updating sequence processing state", zap.Error(err))
				return
			}
			w.logger.Debug("release sequence after processing", zap.Int64("sequence_id", sequenceID))

			time.Sleep(w.sequenceProcessingDelay)
		}
	}
}
