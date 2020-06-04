package dispatcher

import (
	"time"

	"github.com/wavesplatform/transaction-broadcaster/internal/log"
	"github.com/wavesplatform/transaction-broadcaster/internal/node"
	"github.com/wavesplatform/transaction-broadcaster/internal/repository"
	"github.com/wavesplatform/transaction-broadcaster/internal/worker"
	"go.uber.org/zap"
)

type workerError struct {
	Err        error
	SequenceID int64
}

func (e workerError) Error() string {
	return e.Err.Error()
}

// Dispatcher ...
type Dispatcher interface {
	RunLoop() error
}

type workerParams struct {
	txProcessingTTL, heightsAfterLastTx, waitForNextHeightDelay int32
	numberOfRevalidateAttempts                                  int16
}

type dispatcherImpl struct {
	repo                                repository.Repository
	nodeInteractor                      node.Interactor
	logger                              *zap.Logger
	sequenceChan, completedSequenceChan chan int64
	errorsChan                          chan workerError
	loopDelay                           time.Duration
	sequenceTTL                         time.Duration

	worker workerParams
}

// New returns instance of Dispatcher interface implementation
func New(repo repository.Repository, nodeInteractor node.Interactor, sequenceChan chan int64, loopDelay, sequenceTTL int64, txProcessingTTL, heightsAfterLastTx, waitForNextHeightDelay int32, numberOfRevalidateAttempts int16) Dispatcher {
	logger := log.Logger.Named("dispatcher")
	completedSequenceChan := make(chan int64)
	errorsChan := make(chan workerError)

	return &dispatcherImpl{
		repo:                  repo,
		nodeInteractor:        nodeInteractor,
		logger:                logger,
		sequenceChan:          sequenceChan,
		completedSequenceChan: completedSequenceChan,
		errorsChan:            errorsChan,
		loopDelay:             time.Duration(loopDelay) * time.Millisecond,
		sequenceTTL:           time.Duration(sequenceTTL) * time.Millisecond,

		worker: workerParams{
			txProcessingTTL:            txProcessingTTL,
			heightsAfterLastTx:         heightsAfterLastTx,
			waitForNextHeightDelay:     waitForNextHeightDelay,
			numberOfRevalidateAttempts: numberOfRevalidateAttempts,
		},
	}
}

// RunLoop starts dispatcher infinite work loop
func (d *dispatcherImpl) RunLoop() error {
	ticker := time.NewTicker(d.loopDelay)
	defer ticker.Stop()

	for {
		select {
		case seqID := <-d.sequenceChan:
			d.logger.Debug("got new sequence", zap.Int64("sequence_id", seqID))

			if err := d.repo.SetSequenceStateByID(seqID, repository.StateProcessing); err != nil {
				d.logger.Error("error occured while setting sequence processing state", zap.Error(err))
				return err
			}
			d.runWorker(seqID)
		case e := <-d.errorsChan:
			d.logger.Debug("got new error", zap.Error(e.Err), zap.Int64("sequence_id", e.SequenceID))

			switch e.Err.(type) {
			case worker.RecoverableError:
				d.logger.Debug("recoverable error", zap.String("message", e.Err.Error()))

				// refresh sequence status
				if err := d.repo.SetSequenceStateByID(e.SequenceID, repository.StateProcessing); err != nil {
					d.logger.Error("error occured while setting sequence processing state", zap.Error(err))
					return err
				}
				d.runWorker(e.SequenceID)
			case worker.NonRecoverableError:
				d.logger.Debug("non-recoverable error", zap.String("message", e.Err.Error()))

				if err := d.repo.SetSequenceErrorStateByID(e.SequenceID, e.Err); err != nil {
					d.logger.Error("error occured while setting sequence error state", zap.Error(err))
					return err
				}
			case worker.FatalError:
				d.logger.Debug("fatal error", zap.String("message", e.Err.Error()))

				return e.Err
			default:
			}
		case seqID := <-d.completedSequenceChan:
			d.logger.Debug("got new completed sequence")

			if err := d.repo.SetSequenceStateByID(seqID, repository.StateDone); err != nil {
				d.logger.Error("error occured while setting sequence done state", zap.Error(err))
				return err
			}
		case <-ticker.C:
			d.logger.Debug("next ticker tick")
			hangingSequenceIds, err := d.repo.GetHangingSequenceIds(d.sequenceTTL)
			if err != nil {
				d.logger.Error("error occured while getting hangins sequence ids", zap.Error(err))
				return err
			}

			for _, seqID := range hangingSequenceIds {
				// refresh sequence status
				if err := d.repo.SetSequenceStateByID(seqID, repository.StateProcessing); err != nil {
					d.logger.Error("error occurred while updating sequence state", zap.Error(err), zap.Int64("sequence_id", seqID))
					return err
				}

				d.runWorker(seqID)
			}
		default:
			// nothing to do, just wait for the next message
		}
	}
}

func (d *dispatcherImpl) runWorker(seqID int64) {
	w := worker.New(string(time.Now().Unix()), d.repo, d.nodeInteractor, d.worker.txProcessingTTL, d.worker.heightsAfterLastTx, d.worker.waitForNextHeightDelay, d.worker.numberOfRevalidateAttempts)
	go func(seqID int64) {
		if err := w.Run(seqID); err != nil {
			d.errorsChan <- workerError{
				Err:        err,
				SequenceID: seqID,
			}
			return
		}
		d.completedSequenceChan <- seqID
	}(seqID)
}
