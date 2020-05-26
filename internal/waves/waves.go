package waves

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/waves-exchange/broadcaster/internal/log"
	"go.uber.org/zap"
)

// TransactionID represents type of id of tx
type TransactionID string

type transactionModel struct {
	id TransactionID
}

type validateTxResponse struct {
	valid bool
	trace []string
}

// ValidationResult represents result of validate tx operation
type ValidationResult struct {
	IsValid      bool
	ErrorMessage string
}

type broadcastResponse struct {
	ID TransactionID
}

type transactionsStatusRequest struct {
	ids []TransactionID
}

type transactionStatusResponse struct {
	ID            TransactionID
	Status        TransactionStatus
	Height        int32
	Confirmations int32
}

type transactionsStatusResponse []transactionStatusResponse

type blocksHeightResponse struct {
	Height int32
}

type errorResponse struct {
	Message string
}

// TransactionStatus represents current status of transaction
type TransactionStatus string

//
const (
	TransactionStatusNotFound    = "not_found"
	TransactionStatusUnconfirmed = "unconfirmed"
	TransactionStatusConfirmed   = "confirmed"
)

// Availability represents map of transactionId:isTransactionAvailable (tx has status not not_found)
type Availability map[TransactionID]bool

// NodeInteractor ...
type NodeInteractor interface {
	ValidateTx(string) (*ValidationResult, Error)
	BroadcastTx(string) (TransactionID, Error)
	WaitForTxStatus(TransactionID, TransactionStatus) Error
	GetCurrentHeight() (int32, Error)
	WaitForNHeights(int32) Error
	WaitForNextHeight() Error
	GetTxsAvailability([]TransactionID) (Availability, Error)
}

type impl struct {
	nodeURL              url.URL
	nodeAPIKey           string
	logger               *zap.Logger
	waitForTxStatusDelay time.Duration
	waitForTxTimeout     time.Duration
}

const (
	errorWaitForTxStatusTimeoutMessage = "Wait for tx status time deadline is reached."
)

// NewNodeInteractor creates new Waves Node interactor
func NewNodeInteractor(nodeURL url.URL, nodeAPIKey string, waitForTxStatusDelay, waitForTxTimeout int32) NodeInteractor {
	logger := log.Logger.Named("nodeInteractor")

	return &impl{
		nodeURL:              nodeURL,
		nodeAPIKey:           nodeAPIKey,
		logger:               logger,
		waitForTxStatusDelay: time.Duration(waitForTxStatusDelay) * time.Millisecond,
		waitForTxTimeout:     time.Duration(waitForTxTimeout) * time.Millisecond,
	}
}

// GetCurrentHeight returns current blockhain height
func (r *impl) GetCurrentHeight() (int32, Error) {
	blocksHeightURL := r.nodeURL
	blocksHeightURL.Path = "/blocks/height"
	resp, err := http.Get(blocksHeightURL.String())
	if err != nil {
		return 0, NewWavesError(InternalError, err.Error())
	}

	if resp != nil {
		defer resp.Body.Close()
	}

	blocksHeight := blocksHeightResponse{}
	err = json.NewDecoder(resp.Body).Decode(&blocksHeight)
	if err != nil {
		return 0, NewWavesError(InternalError, err.Error())
	}
	return blocksHeight.Height, nil
}

// ValidateTx validates given tx using node
func (r *impl) ValidateTx(tx string) (*ValidationResult, Error) {
	validateURL := r.nodeURL
	validateURL.Path = "/debug/validate"
	reader := strings.NewReader(tx)
	req, err := http.NewRequest("POST", validateURL.String(), reader)
	if err != nil {
		r.logger.Error("Cannot create validateTx request", zap.Error(err))
		return nil, NewWavesError(InternalError, err.Error())
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-Key", r.nodeAPIKey)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, NewWavesError(InternalError, err.Error())
	}

	if resp != nil {
		defer resp.Body.Close()
	}

	validateTx := validateTxResponse{}
	err = json.NewDecoder(resp.Body).Decode(&validateTx)
	if err != nil {
		return nil, NewWavesError(InternalError, err.Error())
	}

	return &ValidationResult{
		IsValid:      validateTx.valid,
		ErrorMessage: strings.Join(validateTx.trace, ";"),
	}, nil
}

// BroadcastTx broadcasts given tx to blockhain
func (r *impl) BroadcastTx(tx string) (TransactionID, Error) {
	broadcastURL := r.nodeURL
	broadcastURL.Path = "/transactions/broadcast"
	reader := strings.NewReader(tx)
	resp, err := http.Post(broadcastURL.String(), "application/json", reader)
	if err != nil {
		return "", NewWavesError(InternalError, err.Error())
	}

	if resp != nil {
		defer resp.Body.Close()
	}

	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == http.StatusBadRequest {
			t := transactionModel{}
			reader := strings.NewReader(tx)
			err = json.NewDecoder(reader).Decode(&t)
			if err != nil {
				return "", NewWavesError(InternalError, err.Error())
			}

			txStatus, wavesErr := r.getTxStatus(t.id)
			if wavesErr != nil {
				return "", wavesErr
			}

			if txStatus != TransactionStatusNotFound {
				return t.id, nil
			}

			var errorResponseDto errorResponse
			err = json.NewDecoder(resp.Body).Decode(&errorResponseDto)
			if err != nil {
				return "", NewWavesError(InternalError, err.Error())
			}

			return "", NewWavesError(BroadcastClientError, errorResponseDto.Message)
		}

		return "", NewWavesError(BroadcastServerError, resp.Status)
	}

	var broadcastResponseDto broadcastResponse
	err = json.NewDecoder(resp.Body).Decode(&broadcastResponseDto)
	if err != nil {
		return "", NewWavesError(InternalError, err.Error())
	}

	return broadcastResponseDto.ID, nil
}

// WaitForTx waits for tx status appearance in the blockchain
func (r *impl) WaitForTxStatus(txID TransactionID, waitForStatus TransactionStatus) Error {
	start := time.Now()
	for {
		status, err := r.getTxStatus(txID)
		if err != nil {
			return err
		}

		if status == waitForStatus {
			return nil
		}

		now := time.Now()
		if now.Sub(start) > r.waitForTxTimeout {
			return NewWavesError(WaitForTxStatusTimeoutError, errorWaitForTxStatusTimeoutMessage)
		}

		time.Sleep(r.waitForTxStatusDelay)
	}
}

// WaitForNHeights waits for n heights in the blockchain
func (r *impl) WaitForNHeights(nHeights int32) Error {
	currentHeight, err := r.GetCurrentHeight()
	if err != nil {
		return err
	}

	var newHeight int32
	ticker := time.NewTicker(1 * time.Second)
	for range ticker.C {
		newHeight, err = r.GetCurrentHeight()
		if err != nil {
			return NewWavesError(InternalError, err.Error())
		}

		if newHeight > currentHeight+nHeights {
			ticker.Stop()
		}
	}

	return nil
}

// WaitForNextHeight waits for the next height in the blockchain
func (r *impl) WaitForNextHeight() Error {
	return r.WaitForNHeights(1)
}

// GetTxsAvailability
func (r *impl) GetTxsAvailability(txIDs []TransactionID) (Availability, Error) {
	txsStatusURL := r.nodeURL
	txsStatusURL.Path = "/transactions/status"

	req, err := json.Marshal(transactionsStatusRequest{
		ids: txIDs,
	})
	if err != nil {
		return nil, NewWavesError(InternalError, err.Error())
	}

	reader := bytes.NewReader(req)

	resp, err := http.Post(txsStatusURL.String(), "application/json", reader)
	if err != nil {
		return nil, NewWavesError(InternalError, err.Error())
	}

	if resp != nil {
		defer resp.Body.Close()
	}

	txStatuses := transactionsStatusResponse{}
	err = json.NewDecoder(resp.Body).Decode(&txStatuses)
	if err != nil {
		return nil, NewWavesError(InternalError, err.Error())
	}

	availability := Availability{}
	for _, txStatus := range txStatuses {
		availability[txStatus.ID] = txStatus.Status != TransactionStatusNotFound
	}

	return availability, nil
}

func (r *impl) getTxStatus(txID TransactionID) (TransactionStatus, Error) {
	txStatusURL := r.nodeURL
	txStatusURL.Path = "/transactions/status"
	q := url.Values{}
	q.Set("id", string(txID))
	txStatusURL.RawQuery = q.Encode()

	resp, err := http.Get(txStatusURL.String())
	if err != nil {
		return "", NewWavesError(InternalError, err.Error())
	}

	if resp != nil {
		defer resp.Body.Close()
	}

	if resp.StatusCode != http.StatusOK {
		return "", NewWavesError(GetTxStatusError, resp.Status)
	}

	txStatus := transactionStatusResponse{}
	err = json.NewDecoder(resp.Body).Decode(&txStatus)
	if err != nil {
		return "", NewWavesError(InternalError, err.Error())
	}

	return txStatus.Status, nil
}
