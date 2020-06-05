package worker

import (
	"regexp"
	"time"

	"github.com/wavesplatform/transaction-broadcaster/internal/log"
	"github.com/wavesplatform/transaction-broadcaster/internal/node"
	"github.com/wavesplatform/transaction-broadcaster/internal/repository"
	"go.uber.org/zap"
)

var transactionTimestampErrorRE = regexp.MustCompile("Transaction timestamp \\d+ is more than \\d+ms")

// Worker represents worker interface
type Worker interface {
	Run(sequenceID int64) error
}

type workerImpl struct {
	repo                   repository.Repository
	nodeInteractor         node.Interactor
	logger                 *zap.Logger
	txProcessingTTL        time.Duration
	heightsAfterLastTx     int32
	waitForNextHeightDelay time.Duration
}

// New returns instance of Worker interface implementation
func New(workerID string, repo repository.Repository, nodeInteractor node.Interactor, txProcessingTTL, heightsAfterLastTx, waitForNextHeightDelay int32) Worker {
	logger := log.Logger.Named("worker-" + workerID)

	return &workerImpl{
		logger:                 logger,
		repo:                   repo,
		nodeInteractor:         nodeInteractor,
		txProcessingTTL:        time.Duration(txProcessingTTL) * time.Millisecond,
		heightsAfterLastTx:     heightsAfterLastTx,
		waitForNextHeightDelay: time.Duration(waitForNextHeightDelay) * time.Millisecond,
	}
}

// Run starts the worker processing sequenceID
func (w *workerImpl) Run(sequenceID int64) error {
	w.logger.Debug("start processing sequence", zap.Int64("sequence_id", sequenceID))

	txs, err := w.repo.GetSequenceTxsByID(sequenceID)
	if err != nil {
		w.logger.Error("error occurred while getting sequence txs", zap.Error(err))
		return NewFatalError(err.Error())
	}

	w.logger.Debug("going to process txs", zap.Int("txs_count", len(txs)))

	var confirmedTxs = make(map[string]*repository.SequenceTx)

	for _, tx := range txs {
		if tx.State == repository.TransactionStateConfirmed {
			confirmedTxs[tx.ID] = tx
			continue
		}

		if len(confirmedTxs) > 0 {
			var confirmedTxIDs []string
			for txID := range confirmedTxs {
				confirmedTxIDs = append(confirmedTxIDs, txID)
			}

			availability, wavesErr := w.nodeInteractor.GetTxsAvailability(confirmedTxIDs)
			if wavesErr != nil {
				w.logger.Error("error occurred while fetching txs statuses", zap.Error(wavesErr), zap.Int64("sequence_id", sequenceID))
				return NewRecoverableError(wavesErr.Error())
			}

			for txID, isAvailable := range availability {
				if !isAvailable {
					w.logger.Debug("one of confirmed tx was pulled out", zap.Int64("sequence_id", sequenceID), zap.String("tx_id", txID))

					if err := w.repo.SetSequenceTxsStateAfter(sequenceID, txID, repository.TransactionStatePending); err != nil {
						w.logger.Error("error occured while setting txs pending state", zap.Error(err), zap.Int64("sequence_id", sequenceID), zap.String("after_tx_id", txID))
						return NewFatalError(err.Error())
					}

					return NewRecoverableError(err.Error())
				}
			}
		}

		switch tx.State {
		case repository.TransactionStateProcessing:
			if time.Now().Sub(tx.UpdatedAt) < w.txProcessingTTL {
				w.logger.Debug("tx is under processing, processing delay is not over", zap.Int64("sequence_id", sequenceID), zap.String("tx_id", tx.ID))
				return nil
			}
			fallthrough
		case repository.TransactionStatePending, repository.TransactionStateValidated, repository.TransactionStateUnconfirmed:
			if err := w.processTx(tx); err != nil {
				w.logger.Error("error occured while processing tx", zap.Error(err), zap.Int64("sequence_id", sequenceID), zap.String("tx_id", tx.ID))
				return err
			}
			confirmedTxs[tx.ID] = tx
		case repository.TransactionStateError:
			return NewNonRecoverableError(tx.ErrorMessage)
		}
	}

	startHeight := int32(0)
	var confirmedTxIDs []string
	for txID, tx := range confirmedTxs {
		if startHeight < tx.Height {
			startHeight = tx.Height
		}
		confirmedTxIDs = append(confirmedTxIDs, txID)
	}

	targetHeight := startHeight + w.heightsAfterLastTx

	if err = w.waitForTargetHeight(targetHeight, sequenceID, confirmedTxIDs); err != nil {
		return err
	}

	return nil
}

func (w *workerImpl) processTx(tx *repository.SequenceTx) error {
	switch tx.State {
	case repository.TransactionStatePending:
		w.logger.Debug("process tx", zap.Int64("sequence_id", tx.SequenceID), zap.String("tx_id", tx.ID))

		if err := w.repo.SetSequenceTxState(tx, repository.TransactionStateProcessing); err != nil {
			return NewFatalError(err.Error())
		}
		fallthrough
	case repository.TransactionStateProcessing:
		w.logger.Debug("validate tx", zap.Int64("sequence_id", tx.SequenceID), zap.String("tx_id", tx.ID))

		if err := w.validateTx(tx); err != nil {
			return err
		}

		if err := w.repo.SetSequenceTxState(tx, repository.TransactionStateValidated); err != nil {
			return NewFatalError(err.Error())
		}

		fallthrough
	case repository.TransactionStateValidated:
		w.logger.Debug("broadcast tx", zap.Int64("sequence_id", tx.SequenceID), zap.String("tx_id", tx.ID))

		if err := w.broadcastTx(tx); err != nil {
			return err
		}

		if err := w.repo.SetSequenceTxState(tx, repository.TransactionStateUnconfirmed); err != nil {
			return NewFatalError(err.Error())
		}

		fallthrough
	case repository.TransactionStateUnconfirmed:
		w.logger.Debug("wait for tx", zap.Int64("sequence_id", tx.SequenceID), zap.String("tx_id", tx.ID))

		height, err := w.waitForTxConfirmation(tx)
		if err != nil {
			return err
		}

		if err := w.repo.SetSequenceTxConfirmedState(tx, height); err != nil {
			return NewFatalError(err.Error())
		}

		fallthrough
	case repository.TransactionStateConfirmed:
		w.logger.Debug("tx appeared in the blockchain", zap.Int64("sequence_id", tx.SequenceID), zap.String("tx_id", tx.ID))
		return nil
	default:
		return nil
	}
}

func (w *workerImpl) validateTx(tx *repository.SequenceTx) error {
	validationResult, wavesErr := w.nodeInteractor.ValidateTx(tx.Tx)
	if wavesErr != nil {
		w.logger.Error("error occurred while validating tx", zap.Error(wavesErr), zap.Int64("sequence_id", tx.SequenceID), zap.String("tx_id", tx.ID))
		return NewRecoverableError(wavesErr.Error())
	}

	if !validationResult.IsValid {
		w.logger.Debug("invalid tx", zap.Int64("sequence_id", tx.SequenceID), zap.String("tx_id", tx.ID))

		// check where error is about transaction timestamp
		isTimestampError := transactionTimestampErrorRE.MatchString(validationResult.ErrorMessage)
		if !isTimestampError {
			if err := w.repo.SetSequenceTxErrorMessage(tx, validationResult.ErrorMessage); err != nil {
				w.logger.Error("error occured while setting tx error message", zap.Error(err), zap.Int64("sequence_id", tx.SequenceID), zap.String("tx_id", tx.ID), zap.String("error_message", validationResult.ErrorMessage))
				return NewFatalError(err.Error())
			}

			if err := w.nodeInteractor.WaitForNextHeight(); err != nil {
				return NewRecoverableError(err.Error())
			}

			return w.processTx(tx)
		}

		if err := w.repo.SetSequenceTxState(tx, repository.TransactionStateError); err != nil {
			w.logger.Error("error occured while setting tx error state", zap.Error(err), zap.Int64("sequence_id", tx.SequenceID), zap.String("tx_id", tx.ID))
			return NewFatalError(err.Error())
		}

		errorMessage := validationResult.ErrorMessage
		if len(tx.ErrorMessage) > 0 {
			errorMessage = tx.ErrorMessage
		}
		return NewNonRecoverableError(errorMessage)
	}

	return nil
}

func (w *workerImpl) broadcastTx(tx *repository.SequenceTx) error {
	_, wavesErr := w.nodeInteractor.BroadcastTx(tx.Tx)
	if wavesErr != nil {
		if wavesErr.Code() == node.BroadcastClientError {
			w.logger.Error("error occurred while setting sequence error state", zap.Error(wavesErr), zap.Int64("sequence_id", tx.SequenceID), zap.String("tx_id", tx.ID))
			return NewNonRecoverableError(wavesErr.Error())
		}

		w.logger.Error("error occurred while broadcasting tx", zap.Error(wavesErr), zap.Int64("sequence_id", tx.SequenceID), zap.String("tx_id", tx.ID))
		return NewRecoverableError(wavesErr.Error())
	}

	return nil
}

func (w *workerImpl) waitForTxConfirmation(tx *repository.SequenceTx) (int32, error) {
	height, wavesErr := w.nodeInteractor.WaitForTxStatus(tx.ID, node.TransactionStatusConfirmed)
	if wavesErr != nil {
		return 0, NewRecoverableError(wavesErr.Error())
	}
	return height, nil
}

// waitForTargetHeight waits for target height
// and on each height checking its checks that none of confirmed txs was not pulled out from the blockchain
func (w *workerImpl) waitForTargetHeight(targetHeight int32, seqID int64, confirmedTxIDs []string) error {
	currentHeight, wavesErr := w.nodeInteractor.GetCurrentHeight()
	if wavesErr != nil {
		w.logger.Error("error occurred while getting current height", zap.Error(wavesErr))
		return NewRecoverableError(wavesErr.Error())
	}

	w.logger.Debug("start waiting for target height", zap.Int("confirmed_txs_count", len(confirmedTxIDs)), zap.Int32("target_height", targetHeight), zap.Int32("current_height", currentHeight))

	if currentHeight >= targetHeight {
		return nil
	}

	ticker := time.NewTicker(w.waitForNextHeightDelay)
	defer ticker.Stop()

	for range ticker.C {
		// refresh sequence status
		if err := w.repo.SetSequenceStateByID(seqID, repository.StateProcessing); err != nil {
			w.logger.Error("error occurred while updating sequence state", zap.Error(err), zap.Int64("sequence_id", seqID))
			return NewFatalError(err.Error())
		}

		availability, wavesErr := w.nodeInteractor.GetTxsAvailability(confirmedTxIDs)
		if wavesErr != nil {
			w.logger.Error("error occurred while waiting for n blocks after last tx", zap.Error(wavesErr))
			return NewRecoverableError(wavesErr.Error())
		}

		for txID, isAvailable := range availability {
			if !isAvailable {
				w.logger.Debug("one of confirmed tx was pulled out", zap.String("tx_id", txID))

				if err := w.repo.SetSequenceTxsStateAfter(seqID, txID, repository.TransactionStatePending); err != nil {
					w.logger.Error("error occured while setting txs pending state", zap.Error(err), zap.Int64("sequence_id", seqID), zap.String("after_tx_id", txID))
					return NewFatalError(err.Error())
				}

				return NewRecoverableError("error occured while waiting for the Ns block after last tx: one of tx was pulled out from the blockchain")
			}
		}

		currentHeight, wavesErr := w.nodeInteractor.GetCurrentHeight()
		if wavesErr != nil {
			return NewRecoverableError(wavesErr.Error())
		}

		// success
		if currentHeight >= targetHeight {
			w.logger.Debug("blockchain reached target height")
			break
		}
	}

	return nil
}
