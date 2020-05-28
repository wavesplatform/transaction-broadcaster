package worker

import (
	"fmt"
	"time"

	"github.com/waves-exchange/broadcaster/internal/log"
	"github.com/waves-exchange/broadcaster/internal/node"
	"github.com/waves-exchange/broadcaster/internal/sequence"
	"go.uber.org/zap"
)

// Worker represents worker interface
type Worker interface {
	Run(sequenceID int64) error
}

type workerImpl struct {
	service                sequence.Service
	nodeInteractor         node.Interactor
	logger                 *zap.Logger
	txProcessingTTL        time.Duration
	heightsAfterLastTx     int32
	waitForNextHeightDelay time.Duration
}

// New returns instance of Worker interface implementation
func New(service sequence.Service, nodeInteractor node.Interactor, txProcessingTTL, heightsAfterLastTx, waitForNextHeightDelay int32) Worker {
	logger := log.Logger.Named("worker")

	return &workerImpl{
		logger:                 logger,
		service:                service,
		nodeInteractor:         nodeInteractor,
		txProcessingTTL:        time.Duration(txProcessingTTL) * time.Millisecond,
		heightsAfterLastTx:     heightsAfterLastTx,
		waitForNextHeightDelay: time.Duration(waitForNextHeightDelay) * time.Millisecond,
	}
}

// Run starts the worker processing sequenceID
func (w *workerImpl) Run(sequenceID int64) error {
	w.logger.Debug("start processing sequence", zap.Int64("sequence_id", sequenceID))

	txs, err := w.service.GetSequenceTxsByID(sequenceID)
	if err != nil {
		w.logger.Error("error occurred while getting sequence txs", zap.Error(err))
		return NewFatalError(err.Error())
	}

	w.logger.Debug("going to process txs", zap.Int("txs_count", len(txs)))

	confirmedTxs := map[string]*sequence.SequenceTx{}

	for _, tx := range txs {
		if tx.State == sequence.TransactionStateConfirmed {
			confirmedTxs[tx.ID] = tx
			continue
		}

		if len(confirmedTxs) > 0 {
			confirmedTxIDs := []string{}
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

					err := w.service.SetSequenceTxsStateAfter(sequenceID, txID, sequence.TransactionStatePending)
					if err != nil {
						w.logger.Error("error occured while setting txs pending state", zap.Error(err), zap.Int64("sequence_id", sequenceID), zap.String("after_tx_id", txID))
						return NewFatalError(err.Error())
					}

					return NewRecoverableError(err.Error())
				}
			}
		}

		switch tx.State {
		case sequence.TransactionStatePending:
			err := w.processTx(tx, true)
			if err != nil {
				w.logger.Error("error occured while processing tx", zap.Error(err), zap.Int64("sequence_id", sequenceID), zap.String("tx_id", tx.ID))
				return err
			}
			confirmedTxs[tx.ID] = tx
		case sequence.TransactionStateProcessing:
			if time.Now().Sub(tx.UpdatedAt) < w.txProcessingTTL {
				w.logger.Debug("tx is under processing, processing delay is not over", zap.Int64("sequence_id", sequenceID), zap.String("tx_id", tx.ID))
				return nil
			}

		case sequence.TransactionStateUnconfirmed:
			err := w.processTx(tx, true)
			if err != nil {
				w.logger.Error("error occured while processing tx", zap.Error(err), zap.Int64("sequence_id", sequenceID), zap.String("tx_id", tx.ID))
				return err
			}
			confirmedTxs[tx.ID] = tx
		case sequence.TransactionStateError:
			return NewNonRecoverableError(tx.ErrorMessage)
		}
	}

	startHeight := int32(0)
	confirmedTxIDs := []string{}
	for txID, tx := range confirmedTxs {
		if startHeight < tx.Height {
			startHeight = tx.Height
		}
		confirmedTxIDs = append(confirmedTxIDs, txID)
	}

	targetHeight := startHeight + w.heightsAfterLastTx

	err = w.waitForTargetHeight(targetHeight, sequenceID, confirmedTxIDs)
	if err != nil {
		return err
	}

	return nil
}

func (w *workerImpl) processTx(tx *sequence.SequenceTx, isFirstTry bool) error {
	switch tx.State {
	case sequence.TransactionStatePending:
		w.logger.Debug("process tx", zap.Int64("sequence_id", tx.SequenceID), zap.String("tx_id", tx.ID))

		err := w.service.SetSequenceTxState(tx, sequence.TransactionStateProcessing)
		if err != nil {
			return NewFatalError(err.Error())
		}
		fallthrough
	case sequence.TransactionStateProcessing:
		w.logger.Debug("validate tx", zap.Int64("sequence_id", tx.SequenceID), zap.String("tx_id", tx.ID))

		validationResult, wavesErr := w.nodeInteractor.ValidateTx(tx.Tx)
		if wavesErr != nil {
			w.logger.Error("error occurred while validating tx", zap.Error(wavesErr), zap.Int64("sequence_id", tx.SequenceID), zap.String("tx_id", tx.ID))
			return NewRecoverableError(wavesErr.Error())
		}

		if !validationResult.IsValid {
			w.logger.Debug("invalid tx", zap.Int64("sequence_id", tx.SequenceID), zap.String("tx_id", tx.ID))

			// try to revalidate tx on the next blockchain height
			if isFirstTry {
				w.nodeInteractor.WaitForNextHeight()
				return w.processTx(tx, false)
			}

			err := w.service.SetSequenceTxErrorState(tx, validationResult.ErrorMessage)
			if err != nil {
				w.logger.Error("error occured while setting tx error state", zap.Error(err), zap.Int64("sequence_id", tx.SequenceID), zap.String("tx_id", tx.ID))
				return NewFatalError(err.Error())
			}

			return NewNonRecoverableError(fmt.Sprintf("tx %s is invalid", tx.ID))
		}

		err := w.service.SetSequenceTxState(tx, sequence.TransactionStateValidated)
		if err != nil {
			return NewFatalError(err.Error())
		}

		fallthrough
	case sequence.TransactionStateValidated:
		w.logger.Debug("broadcast tx", zap.Int64("sequence_id", tx.SequenceID), zap.String("tx_id", tx.ID))

		_, wavesErr := w.nodeInteractor.BroadcastTx(tx.Tx)
		if wavesErr != nil {
			if wavesErr.Code() == node.BroadcastClientError {
				w.logger.Error("error occurred while setting sequence error state", zap.Error(wavesErr), zap.Int64("sequence_id", tx.SequenceID), zap.String("tx_id", tx.ID))
				return NewNonRecoverableError(wavesErr.Error())
			}

			w.logger.Error("error occurred while broadcasting tx", zap.Error(wavesErr), zap.Int64("sequence_id", tx.SequenceID), zap.String("tx_id", tx.ID))
			return NewRecoverableError(wavesErr.Error())
		}

		err := w.service.SetSequenceTxState(tx, sequence.TransactionStateUnconfirmed)
		if err != nil {
			return NewFatalError(err.Error())
		}

		fallthrough
	case sequence.TransactionStateUnconfirmed:
		w.logger.Debug("wait for tx", zap.Int64("sequence_id", tx.SequenceID), zap.String("tx_id", tx.ID))

		height, wavesErr := w.nodeInteractor.WaitForTxStatus(tx.ID, node.TransactionStatusConfirmed)
		if wavesErr != nil {
			return NewRecoverableError(wavesErr.Error())
		}

		err := w.service.SetSequenceTxConfirmedState(tx, height)
		if err != nil {
			return NewFatalError(err.Error())
		}

		fallthrough
	case sequence.TransactionStateConfirmed:
		w.logger.Debug("tx appeared in the blockchain", zap.Int64("sequence_id", tx.SequenceID), zap.String("tx_id", tx.ID))
		return nil
	default:
		return nil
	}
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
		err := w.service.SetSequenceStateByID(seqID, sequence.StateProcessing)
		if err != nil {
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

				err := w.service.SetSequenceTxsStateAfter(seqID, txID, sequence.TransactionStatePending)
				if err != nil {
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
