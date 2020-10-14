package worker

import (
	"encoding/json"
	"regexp"
	"time"

	"github.com/wavesplatform/transaction-broadcaster/internal/log"
	"github.com/wavesplatform/transaction-broadcaster/internal/node"
	"github.com/wavesplatform/transaction-broadcaster/internal/repository"
	"go.uber.org/zap"
)

var transactionTimestampErrorRE = regexp.MustCompile("Transaction timestamp \\d+ is more than \\d+ms")
var transactionDuplicateErrorRE = regexp.MustCompile("State check failed. Reason: Transaction (\\w+) is already in the state on a height of \\d+")

type txWithTimestamp struct {
	Timestamp int64 `json:"timestamp"`
}

// Worker represents worker interface
type Worker interface {
	Run(sequenceID int64) ErrorWithReason
}

type workerImpl struct {
	repo                   repository.Repository
	nodeInteractor         node.Interactor
	logger                 *zap.Logger
	txProcessingTTL        time.Duration
	heightsAfterLastTx     int32
	waitForNextHeightDelay time.Duration
	txOutdateTime          time.Duration
}

// New returns instance of Worker interface implementation
func New(workerID string, repo repository.Repository, nodeInteractor node.Interactor, txOutdateTime, txProcessingTTL, heightsAfterLastTx, waitForNextHeightDelay int32) Worker {
	logger := log.Logger.Named("worker-" + workerID)

	return &workerImpl{
		logger:                 logger,
		repo:                   repo,
		nodeInteractor:         nodeInteractor,
		txProcessingTTL:        time.Duration(txProcessingTTL) * time.Millisecond,
		heightsAfterLastTx:     heightsAfterLastTx,
		waitForNextHeightDelay: time.Duration(waitForNextHeightDelay) * time.Millisecond,
		txOutdateTime:          time.Duration(txOutdateTime) * time.Millisecond,
	}
}

// Run starts the worker processing sequenceID
func (w *workerImpl) Run(sequenceID int64) ErrorWithReason {
	w.logger.Debug("start processing sequence", zap.Int64("sequence_id", sequenceID))

	txs, err := w.repo.GetSequenceTxsByID(sequenceID)
	if err != nil {
		w.logger.Error("error occurred while getting sequence txs", zap.Int64("sequence_id", sequenceID), zap.Error(err))
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

			if err := w.checkTxsAvailability(sequenceID, confirmedTxIDs); err != nil {
				return err
			}
		}

		switch tx.State {
		case repository.TransactionStateProcessing:
			if time.Now().Sub(tx.UpdatedAt) < w.txProcessingTTL {
				w.logger.Debug("tx is under processing, processing ttl is not over", zap.Int64("sequence_id", sequenceID), zap.Int16("position_in_sequence", tx.PositionInSequence))
				return NewRecoverableError("error occured while processing tx: tx is under processing, processing TTL is not over")
			}
			fallthrough
		case repository.TransactionStatePending, repository.TransactionStateValidated, repository.TransactionStateUnconfirmed:
			// will mutate tx - sets ID and height
			if err := w.processTx(tx); err != nil {
				w.logger.Error("error occured while processing tx", zap.Int64("sequence_id", sequenceID), zap.Int16("position_in_sequence", tx.PositionInSequence), zap.Error(err))
				return err
			}
			confirmedTxs[tx.ID] = tx
		case repository.TransactionStateError:
			return NewNonRecoverableError(tx.ErrorMessage, 0)
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

	if err := w.waitForTargetHeight(targetHeight, sequenceID, confirmedTxIDs); err != nil {
		return err
	}

	return nil
}

// notes: mutate tx - sets State, ID and height
func (w *workerImpl) processTx(tx *repository.SequenceTx) ErrorWithReason {
	switch tx.State {
	case repository.TransactionStatePending:
		w.logger.Debug("process tx", zap.Int64("sequence_id", tx.SequenceID), zap.Int16("position_in_sequence", tx.PositionInSequence))

		if err := w.repo.SetSequenceTxState(tx.SequenceID, tx.PositionInSequence, repository.TransactionStateProcessing); err != nil {
			return NewFatalError(err.Error())
		}
		tx.State = repository.TransactionStateProcessing

		fallthrough
	case repository.TransactionStateProcessing:
		w.logger.Debug("validate tx", zap.Int64("sequence_id", tx.SequenceID), zap.Int16("position_in_sequence", tx.PositionInSequence))

		if err := w.validateTx(tx); err != nil {
			return err
		}

		if err := w.repo.SetSequenceTxState(tx.SequenceID, tx.PositionInSequence, repository.TransactionStateValidated); err != nil {
			return NewFatalError(err.Error())
		}
		tx.State = repository.TransactionStateValidated

		fallthrough
	case repository.TransactionStateValidated:
		w.logger.Debug("broadcast tx", zap.Int64("sequence_id", tx.SequenceID), zap.Int16("position_in_sequence", tx.PositionInSequence))

		// will mutate tx - sets ID
		if err := w.broadcastTx(tx); err != nil {
			return err
		}

		if err := w.repo.SetSequenceTxState(tx.SequenceID, tx.PositionInSequence, repository.TransactionStateUnconfirmed); err != nil {
			return NewFatalError(err.Error())
		}
		tx.State = repository.TransactionStateUnconfirmed

		fallthrough
	case repository.TransactionStateUnconfirmed:
		w.logger.Debug("wait for tx confirmation", zap.Int64("sequence_id", tx.SequenceID), zap.Int16("position_in_sequence", tx.PositionInSequence), zap.String("tx_id", tx.ID))

		height, err := w.waitForTxConfirmation(tx.ID)
		if err != nil {
			if err.Code() == node.TxNotFoundError {
				if err := w.repo.SetSequenceTxState(tx.SequenceID, tx.PositionInSequence, repository.TransactionStatePending); err != nil {
					return NewFatalError(err.Error())
				}
			}
			return NewRecoverableError(err.Error())
		}

		if err := w.repo.SetSequenceTxConfirmedState(tx.SequenceID, tx.PositionInSequence, height); err != nil {
			return NewFatalError(err.Error())
		}
		tx.State = repository.TransactionStateConfirmed
		tx.Height = height

		fallthrough
	case repository.TransactionStateConfirmed:
		w.logger.Debug("tx appeared in the blockchain", zap.Int64("sequence_id", tx.SequenceID), zap.Int16("position_in_sequence", tx.PositionInSequence), zap.String("tx_id", tx.ID))
		return nil
	default:
		return nil
	}
}

func (w *workerImpl) validateTx(tx *repository.SequenceTx) ErrorWithReason {
	validationResult, wavesErr := w.nodeInteractor.ValidateTx(tx.Tx)
	if wavesErr != nil {
		w.logger.Error("error occurred while validating tx", zap.Int64("sequence_id", tx.SequenceID), zap.Int16("position_in_sequence", tx.PositionInSequence), zap.Error(wavesErr))
		return NewRecoverableError(wavesErr.Error())
	}

	if !validationResult.IsValid {
		w.logger.Debug("invalid tx", zap.Int64("sequence_id", tx.SequenceID), zap.Int16("position_in_sequence", tx.PositionInSequence))

		// check whether error is about transaction duplicate
		matches := transactionDuplicateErrorRE.FindStringSubmatch(validationResult.ErrorMessage)
		if len(matches) > 1 {
			// transaction is already in the blockchain
			return nil
		}

		// check whether error is about transaction timestamp
		isTimestampError := transactionTimestampErrorRE.MatchString(validationResult.ErrorMessage)

		isOutdated, err := w.isTxOutdated(tx.Tx)
		if err != nil {
			return NewNonRecoverableError(err.Error(), 0)
		}

		if isOutdated {
			w.logger.Debug("tx is outdated (local check)", zap.Int64("sequence_id", tx.SequenceID), zap.Int16("position_in_sequence", tx.PositionInSequence))
		}

		// write error message only if it was not set already
		// otherwise root error will be overwritten by timestamp error
		if len(tx.ErrorMessage) == 0 {
			if err := w.repo.SetSequenceTxErrorMessage(tx.SequenceID, tx.PositionInSequence, validationResult.ErrorMessage); err != nil {
				w.logger.Error("error occured while setting tx error message", zap.Int64("sequence_id", tx.SequenceID), zap.Int16("position_in_sequence", tx.PositionInSequence), zap.String("error_message", validationResult.ErrorMessage), zap.Error(err))
				return NewFatalError(err.Error())
			}
			tx.ErrorMessage = validationResult.ErrorMessage
		}

		if !isTimestampError && !isOutdated {
			if err := w.nodeInteractor.WaitForNextHeight(); err != nil {
				return NewRecoverableError(err.Error())
			}

			return w.validateTx(tx)
		}

		if err := w.repo.SetSequenceTxState(tx.SequenceID, tx.PositionInSequence, repository.TransactionStateError); err != nil {
			w.logger.Error("error occured while setting tx error state", zap.Int64("sequence_id", tx.SequenceID), zap.Int16("position_in_sequence", tx.PositionInSequence), zap.Error(err))
			return NewFatalError(err.Error())
		}

		errorMessage := validationResult.ErrorMessage
		if len(tx.ErrorMessage) > 0 {
			errorMessage = tx.ErrorMessage
		}

		return NewNonRecoverableError(errorMessage, 0)
	}

	// tx is valid, reset error message that may have been set
	if err := w.repo.ResetSequenceTxErrorMessage(tx.SequenceID, tx.PositionInSequence); err != nil {
		w.logger.Error("error occured while resetting tx error message after its validating", zap.Int64("sequence_id", tx.SequenceID), zap.Int16("position_in_sequence", tx.PositionInSequence), zap.Error(err))
		return NewFatalError(err.Error())
	}
	tx.ErrorMessage = ""

	return nil
}

// broadcastTx broadcasts transaction to the blockhain
// whether transaction successfully broadcasted, its sets tx.ID to retrieved txID
// mutate tx
func (w *workerImpl) broadcastTx(tx *repository.SequenceTx) ErrorWithReason {
	txID, wavesErr := w.nodeInteractor.BroadcastTx(tx.Tx)

	if wavesErr != nil {
		// check whether error is about transaction duplicate
		matches := transactionDuplicateErrorRE.FindStringSubmatch(wavesErr.Error())
		if len(matches) > 1 {
			// transaction is already in the blockchain
			txID = matches[1]
		} else {
			w.logger.Error("error occurred while broadcasting tx", zap.Int64("sequence_id", tx.SequenceID), zap.Int16("position_in_sequence", tx.PositionInSequence), zap.Error(wavesErr))

			if wavesErr.Code() == node.BroadcastClientError {
				return NewNonRecoverableError(wavesErr.Error(), wavesErr.NodeErrorCode())
			}

			return NewRecoverableError(wavesErr.Error())
		}
	}

	// tx was broadcasted, reset error message that may have been set
	if err := w.repo.ResetSequenceTxErrorMessage(tx.SequenceID, tx.PositionInSequence); err != nil {
		w.logger.Error("error occured while resetting tx error message after its broadcasting", zap.Int64("sequence_id", tx.SequenceID), zap.Int16("position_in_sequence", tx.PositionInSequence), zap.Error(err))
		return NewFatalError(err.Error())
	}
	tx.ErrorMessage = ""

	if err := w.repo.SetSequenceTxID(tx.SequenceID, tx.PositionInSequence, txID); err != nil {
		return NewFatalError(err.Error())
	}
	tx.ID = txID

	return nil
}

func (w *workerImpl) waitForTxConfirmation(txID string) (int32, node.Error) {
	height, wavesErr := w.nodeInteractor.WaitForTxStatus(txID, node.TransactionStatusConfirmed)
	if wavesErr != nil {
		return 0, wavesErr
	}
	return height, nil
}

// waitForTargetHeight waits for target height
// and on each height checking its checks that none of confirmed txs was not pulled out from the blockchain
func (w *workerImpl) waitForTargetHeight(targetHeight int32, seqID int64, confirmedTxIDs []string) ErrorWithReason {
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
			w.logger.Error("error occurred while updating sequence state", zap.Int64("sequence_id", seqID), zap.Error(err))
			return NewFatalError(err.Error())
		}

		if err := w.checkTxsAvailability(seqID, confirmedTxIDs); err != nil {
			return err
		}

		currentHeight, wavesErr := w.nodeInteractor.GetCurrentHeight()
		if wavesErr != nil {
			w.logger.Error("error occurred while getting current height", zap.Error(wavesErr))
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

func (w *workerImpl) checkTxsAvailability(sequenceID int64, confirmedTxIDs []string) ErrorWithReason {
	availability, wavesErr := w.nodeInteractor.GetTxsAvailability(confirmedTxIDs)
	if wavesErr != nil {
		w.logger.Error("error occurred while fetching txs statuses", zap.Int64("sequence_id", sequenceID), zap.Error(wavesErr))
		return NewRecoverableError(wavesErr.Error())
	}

	for txID, isAvailable := range availability {
		if !isAvailable {
			w.logger.Debug("one of confirmed tx was pulled out", zap.Int64("sequence_id", sequenceID), zap.String("tx_id", txID))

			if err := w.repo.SetSequenceTxsStateAfter(sequenceID, txID, repository.TransactionStatePending); err != nil {
				w.logger.Error("error occured while setting txs pending state", zap.Int64("sequence_id", sequenceID), zap.String("after_tx_id", txID), zap.Error(err))
				return NewFatalError(err.Error())
			}

			return NewRecoverableError("error occured while waiting for the Ns block after last tx: one of tx was pulled out from the blockchain")
		}
	}

	return nil
}

// isTxOutdated retrieves timestamp from tx (via parsing json)
// and checks whether tx is outdated
func (w *workerImpl) isTxOutdated(tx string) (bool, error) {
	t := txWithTimestamp{}

	err := json.Unmarshal([]byte(tx), &t)
	if err != nil {
		return false, err
	}

	return time.Now().Sub(time.Unix(0, t.Timestamp*int64(time.Millisecond))) >= w.txOutdateTime, nil
}
