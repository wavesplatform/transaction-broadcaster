package worker

import (
	"fmt"
	"time"

	"github.com/waves-exchange/broadcaster/internal/log"
	"github.com/waves-exchange/broadcaster/internal/sequence"
	"github.com/waves-exchange/broadcaster/internal/waves"
	"go.uber.org/zap"
)

// Worker represents worker interface
type Worker interface {
	Run(sequenceID int64)
}

type workerImpl struct {
	service               sequence.Service
	nodeInteractor        waves.NodeInteractor
	logger                *zap.Logger
	resultsChan           chan<- Result
	txProcessingTTL       time.Duration
	blocksAfterLastTx     int32
	waitForNextBlockDelay time.Duration
}

// NewWorker creates new worker
func NewWorker(service sequence.Service, nodeInteractor waves.NodeInteractor, resultsChan chan<- Result, txProcessingTTL, blocksAfterLastTx, waitForNextBlockDelay int32) Worker {
	logger := log.Logger.Named("worker")

	return &workerImpl{
		logger:                logger,
		service:               service,
		nodeInteractor:        nodeInteractor,
		resultsChan:           resultsChan,
		txProcessingTTL:       time.Duration(txProcessingTTL) * time.Millisecond,
		blocksAfterLastTx:     blocksAfterLastTx,
		waitForNextBlockDelay: time.Duration(waitForNextBlockDelay) * time.Millisecond,
	}
}

// Run starts the worker processing sequenceID
func (w *workerImpl) Run(sequenceID int64) {
	w.logger.Debug("start processing sequence", zap.Int64("sequence_id", sequenceID))

	txs, err := w.service.GetSequenceTxsByID(sequenceID)
	if err != nil {
		w.logger.Error("error occurred while getting sequence txs", zap.Error(err))
		w.resultsChan <- CreateResult(sequenceID, NewFatalError(err.Error()))
		return
	}

	w.logger.Debug("going to process txs", zap.Int("txs_count", len(txs)))

	confirmedTxs := map[waves.TransactionID]*sequence.SequenceTx{}

	for _, tx := range txs {
		if tx.State == sequence.TransactionStateConfirmed {
			confirmedTxs[waves.TransactionID(tx.ID)] = tx
			continue
		}

		confirmedTxIDs := []waves.TransactionID{}
		for txID := range confirmedTxs {
			confirmedTxIDs = append(confirmedTxIDs, txID)
		}

		availability, wavesErr := w.nodeInteractor.GetTxsAvailability(confirmedTxIDs)
		if wavesErr != nil {
			w.logger.Error("error occurred while fetching txs statuses", zap.Error(wavesErr), zap.Int64("sequence_id", sequenceID))
			w.resultsChan <- CreateResult(sequenceID, NewRecoverableError(wavesErr.Error()))
			return
		}

		for txID, isAvailable := range availability {
			if !isAvailable {
				w.logger.Debug("one of confirmed tx was pulled out", zap.Int64("sequence_id", sequenceID), zap.String("tx_id", string(txID)))

				err := w.service.SetSequenceTxsStateAfter(sequenceID, string(txID), sequence.TransactionStatePending)
				if err != nil {
					w.logger.Error("error occured while setting txs pending state", zap.Error(err), zap.Int64("sequence_id", sequenceID), zap.String("after_tx_id", string(txID)))
					w.resultsChan <- CreateResult(sequenceID, NewFatalError(err.Error()))
					return
				}

				w.resultsChan <- CreateResult(sequenceID, NewRecoverableError(err.Error()))
				return
			}
		}

		switch tx.State {
		case sequence.TransactionStatePending:
			err := w.processTx(tx, true)
			if err != nil {
				w.logger.Error("error occured while processing tx", zap.Error(err), zap.Int64("sequence_id", sequenceID), zap.String("tx_id", string(tx.ID)))
				w.resultsChan <- CreateResult(tx.SequenceID, err)
				return
			}
		case sequence.TransactionStateProcessing:
			if time.Now().Sub(tx.UpdatedAt) < w.txProcessingTTL {
				w.logger.Debug("tx is under processing, processing delay is not over", zap.Int64("sequence_id", sequenceID), zap.String("tx_id", string(tx.ID)))
				return
			}

		case sequence.TransactionStateUnconfirmed:
			err := w.processTx(tx, true)
			if err != nil {
				w.logger.Error("error occured while processing tx", zap.Error(err), zap.Int64("sequence_id", sequenceID), zap.String("tx_id", string(tx.ID)))
				w.resultsChan <- CreateResult(tx.SequenceID, err)
				return
			}
		case sequence.TransactionStateError:
			w.resultsChan <- CreateResult(sequenceID, NewNonRecoverableError(tx.ErrorMessage))
			return
		}
	}

	err = w.waitNBlocksAfterLastTx(confirmedTxs)
	if err != nil {
		w.resultsChan <- CreateResult(sequenceID, err)
		return
	}

	w.resultsChan <- Result{
		SequenceID: sequenceID,
		Error:      nil,
	}
}

func (w *workerImpl) processTx(tx *sequence.SequenceTx, isFirstTry bool) error {
	switch tx.State {
	case sequence.TransactionStatePending:
		w.logger.Debug("process tx", zap.Int64("sequence_id", tx.SequenceID), zap.String("tx_id", string(tx.ID)))

		err := w.service.SetSequenceTxState(tx, sequence.TransactionStateProcessing)
		if err != nil {
			return NewFatalError(err.Error())
		}
		fallthrough
	case sequence.TransactionStateProcessing:
		w.logger.Debug("validate tx", zap.Int64("sequence_id", tx.SequenceID), zap.String("tx_id", string(tx.ID)))

		validationResult, wavesErr := w.nodeInteractor.ValidateTx(tx.Tx)
		if wavesErr != nil {
			w.logger.Error("error occurred while validating tx", zap.Error(wavesErr), zap.Int64("sequence_id", tx.SequenceID), zap.String("tx_id", string(tx.ID)))
			return NewRecoverableError(wavesErr.Error())
		}

		if !validationResult.IsValid {
			w.logger.Debug("invalid tx", zap.Int64("sequence_id", tx.SequenceID), zap.String("tx_id", string(tx.ID)))

			// try to revalidate tx on the next blockchain height
			if isFirstTry {
				w.nodeInteractor.WaitForNextHeight()
				return w.processTx(tx, false)
			}

			err := w.service.SetSequenceTxErrorState(tx, validationResult.ErrorMessage)
			if err != nil {
				w.logger.Error("error occured while settings tx error state", zap.Error(err), zap.Int64("sequence_id", tx.SequenceID), zap.String("tx_id", string(tx.ID)))
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
		w.logger.Debug("broadcast tx", zap.Int64("sequence_id", tx.SequenceID), zap.String("tx_id", string(tx.ID)))

		_, wavesErr := w.nodeInteractor.BroadcastTx(tx.Tx)
		if wavesErr != nil {
			if wavesErr.Code() == waves.BroadcastClientError {
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
		w.logger.Debug("wait for tx", zap.Int64("sequence_id", tx.SequenceID), zap.String("tx_id", string(tx.ID)))

		wavesErr := w.nodeInteractor.WaitForTxStatus(waves.TransactionID(tx.ID), waves.TransactionStatusConfirmed)
		if wavesErr != nil {
			return NewRecoverableError(wavesErr.Error())
		}

		err := w.service.SetSequenceTxState(tx, sequence.TransactionStateConfirmed)
		if err != nil {
			return NewFatalError(err.Error())
		}

		fallthrough
	case sequence.TransactionStateConfirmed:
		w.logger.Debug("tx appeared in the blockchain", zap.Int64("sequence_id", tx.SequenceID), zap.String("tx_id", string(tx.ID)))
		return nil
	default:
		return nil
	}
}

func (w *workerImpl) waitNBlocksAfterLastTx(confirmedTxs map[waves.TransactionID]*sequence.SequenceTx) error {
	w.logger.Debug("start waiting for n blocks after last tx", zap.Int("confirmed_txs_count", len(confirmedTxs)))

	confirmedTxIDs := []waves.TransactionID{}
	for txID := range confirmedTxs {
		confirmedTxIDs = append(confirmedTxIDs, txID)
	}

	startHeight, wavesErr := w.nodeInteractor.GetCurrentHeight()
	if wavesErr != nil {
		w.logger.Error("error occurred while getting current height", zap.Error(wavesErr))
		return NewRecoverableError(wavesErr.Error())
	}

	ticker := time.NewTicker(w.waitForNextBlockDelay)
	defer ticker.Stop()

	for range ticker.C {
		availability, wavesErr := w.nodeInteractor.GetTxsAvailability(confirmedTxIDs)
		if wavesErr != nil {
			w.logger.Error("error occurred while waiting for n blocks after last tx", zap.Error(wavesErr))
			return NewRecoverableError(wavesErr.Error())
		}

		for txID, isAvailable := range availability {
			if !isAvailable {
				w.logger.Debug("one of confirmed tx was pulled out", zap.String("tx_id", string(txID)))

				err := w.service.SetSequenceTxsStateAfter(confirmedTxs[txID].SequenceID, string(txID), sequence.TransactionStatePending)
				if err != nil {
					w.logger.Error("error occured while setting txs pending state", zap.Error(err), zap.Int64("sequence_id", confirmedTxs[txID].SequenceID), zap.String("after_tx_id", string(txID)))
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
		if currentHeight >= startHeight+w.blocksAfterLastTx {
			break
		}
	}

	return nil
}
