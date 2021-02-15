package worker

import (
	"errors"
	"fmt"
	"testing"
	"time"

	//

	"github.com/wavesplatform/transaction-broadcaster/internal/repository"

	"github.com/stretchr/testify/mock"
	"github.com/wavesplatform/transaction-broadcaster/internal/log"
	"github.com/wavesplatform/transaction-broadcaster/internal/node"
)

type MockRepository struct {
	mock.Mock
}

func (m MockRepository) GetSequenceByID(id int64) (*repository.Sequence, error) {
	args := m.Called(id)
	return args.Get(0).(*repository.Sequence), args.Error(1)
}
func (m MockRepository) GetSequenceTxsByID(sequenceID int64) ([]*repository.SequenceTx, error) {
	args := m.Called(sequenceID)
	return args.Get(0).([]*repository.SequenceTx), args.Error(1)
}
func (m MockRepository) GetHangingSequenceIds(ttl time.Duration) ([]int64, error) {
	args := m.Called(ttl)
	return args.Get(0).([]int64), args.Error(1)
}
func (m MockRepository) CreateSequence(txs []repository.TxWithIDDto) (int64, error) {
	args := m.Called(txs)
	return args.Get(0).(int64), args.Error(1)
}
func (m MockRepository) SetSequenceStateByID(sequenceID int64, newState repository.State) error {
	args := m.Called(sequenceID, newState)
	return args.Error(0)
}
func (m MockRepository) SetSequenceErrorStateByID(sequenceID int64, err error) error {
	args := m.Called(sequenceID, err)
	return args.Error(0)
}
func (m MockRepository) SetSequenceTxState(tx *repository.SequenceTx, newState repository.TransactionState) error {
	args := m.Called(tx, newState)
	return args.Error(0)
}
func (m MockRepository) SetSequenceTxConfirmedState(tx *repository.SequenceTx, height int32) error {
	args := m.Called(tx, height)
	return args.Error(0)
}
func (m MockRepository) SetSequenceTxErrorState(tx *repository.SequenceTx, errorMessage string) error {
	args := m.Called(tx, errorMessage)
	return args.Error(0)
}
func (m MockRepository) SetSequenceTxsStateAfter(sequenceID int64, txID string, newState repository.TransactionState) error {
	args := m.Called(sequenceID, txID, newState)
	return args.Error(0)
}

type MockNodeInteractor struct {
	mock.Mock
}

func (m MockNodeInteractor) ValidateTx(tx string) (*node.ValidationResult, node.Error) {
	args := m.Called(tx)
	if args.Get(1) != nil {
		return args.Get(0).(*node.ValidationResult), args.Get(1).(node.Error)
	}
	return args.Get(0).(*node.ValidationResult), nil
}

func (m MockNodeInteractor) BroadcastTx(tx string) (string, node.Error) {
	args := m.Called(tx)
	if args.Get(1) != nil {
		return "", args.Get(1).(node.Error)
	}
	return args.String(0), nil
}

func (m MockNodeInteractor) WaitForTxStatus(tx string, status node.TransactionStatus) (int32, node.Error) {
	args := m.Called(tx, status)
	if args.Get(1) != nil {
		return args.Get(0).(int32), args.Get(1).(node.Error)
	}
	return args.Get(0).(int32), nil
}

func (m MockNodeInteractor) GetCurrentHeight() (int32, node.Error) {
	args := m.Called()
	if args.Get(1) != nil {
		return int32(0), args.Get(1).(node.Error)
	}
	return args.Get(0).(int32), nil
}

func (m MockNodeInteractor) WaitForTargetHeight(targetHeight int32) node.Error {
	args := m.Called(targetHeight)
	if args.Get(0) != nil {
		return args.Get(0).(node.Error)
	}
	return nil
}

func (m MockNodeInteractor) WaitForNextHeight() node.Error {
	args := m.Called()
	if args.Get(0) != nil {
		return args.Get(0).(node.Error)
	}
	return nil
}

func (m MockNodeInteractor) GetTxsAvailability(txs []string) (node.Availability, node.Error) {
	args := m.Called(txs)
	if args.Get(1) != nil {
		return args.Get(0).(node.Availability), args.Get(1).(node.Error)
	}
	return args.Get(0).(node.Availability), nil
}

// Testing worker broadcastTx method
func TestBroadcastTx_Success(t *testing.T) {
	log.Init(testing.Verbose())
	logger := log.Logger.Named("testing")

	tx := &repository.SequenceTx{
		ID: "transactionID",
		Tx: `{"id":"transactionID"}`,
	}

	n := MockNodeInteractor{}
	n.On("BroadcastTx", tx.Tx).Return(tx.ID, nil).Once()

	w := &workerImpl{
		logger:         logger,
		nodeInteractor: n,
	}

	t.Run("Successful", func(t *testing.T) {
		err := w.broadcastTx(tx)

		if err != nil {
			t.Fail()
		}

		n.AssertExpectations(t)
	})
}

func TestBroadcastTx_NonRecoverableError(t *testing.T) {
	log.Init(testing.Verbose())
	logger := log.Logger.Named("testing")

	tx := &repository.SequenceTx{
		ID: "transactionID",
		Tx: `{"id":"transactionID"}`,
	}

	n := MockNodeInteractor{}
	n.On("BroadcastTx", tx.Tx).Return(nil, node.NewError(node.BroadcastClientError, "state error")).Once()

	w := &workerImpl{
		logger:         logger,
		nodeInteractor: n,
	}

	t.Run("NonRecoverableError", func(t *testing.T) {
		err := w.broadcastTx(tx)

		if err == nil || !errors.Is(err, NewNonRecoverableError("state error")) {
			t.Fail()
		}

		n.AssertExpectations(t)
	})
}

func TestBroadcastTx_RecoverableError(t *testing.T) {
	log.Init(testing.Verbose())
	logger := log.Logger.Named("testing")

	tx := &repository.SequenceTx{
		ID: "transactionID",
		Tx: `{"id":"transactionID"}`,
	}

	n := MockNodeInteractor{}
	n.On("BroadcastTx", tx.Tx).Return(nil, node.NewError(node.BroadcastServerError, "connection refused")).Once()

	w := &workerImpl{
		logger:         logger,
		nodeInteractor: n,
	}

	t.Run("RecoverableError", func(t *testing.T) {
		err := w.broadcastTx(tx)

		if err == nil || !errors.Is(err, NewRecoverableError("connection refused")) {
			t.Fail()
		}
		fmt.Println(err, !errors.Is(err, NewRecoverableError("connection refused")))
		n.AssertExpectations(t)
	})
}

// Testing worker waitForTxConfirmation method
func TestWaitForTxConfirmation_Success(t *testing.T) {
	log.Init(testing.Verbose())
	logger := log.Logger.Named("testing")

	tx := &repository.SequenceTx{
		ID: "transactionID",
	}

	expectedHeight := int32(time.Now().Unix() / 1000)

	n := MockNodeInteractor{}
	n.On("WaitForTxStatus", tx.ID, (node.TransactionStatus)(node.TransactionStatusConfirmed)).Return(expectedHeight, nil).Once()

	w := &workerImpl{
		logger:         logger,
		nodeInteractor: n,
	}

	t.Run("Successful", func(t *testing.T) {
		height, err := w.waitForTxConfirmation(tx)

		if err != nil {
			t.Fail()
		}

		if height != expectedHeight {
			t.Fail()
		}

		n.AssertExpectations(t)
	})
}

func TestWaitForTxConfirmation_CheckTimeoutError(t *testing.T) {
	log.Init(testing.Verbose())
	logger := log.Logger.Named("testing")

	tx := &repository.SequenceTx{
		ID: "transactionID",
	}
	nodeError := node.NewError(3, "some error")
	expectedError := NewRecoverableError(nodeError.Error())

	n := MockNodeInteractor{}
	n.On("WaitForTxStatus", tx.ID, (node.TransactionStatus)(node.TransactionStatusConfirmed)).Return(int32(0), nodeError).Once()

	w := &workerImpl{
		logger:         logger,
		nodeInteractor: n,
	}

	t.Run("CheckTimeoutError", func(t *testing.T) {
		_, err := w.waitForTxConfirmation(tx)

		if err == nil || !errors.Is(err, expectedError) {
			t.Fail()
		}

		n.AssertExpectations(t)
	})
}
