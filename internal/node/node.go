package node

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

type transactionModel struct {
	id string
}

type validateTxResponse struct {
	Valid bool
	Error string
}

type broadcastResponse struct {
	ID string
}

type transactionsStatusRequest struct {
	IDs []string `json:"ids"`
}

type transactionStatusResponse struct {
	ID            string
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

// ValidationResult represents result of ValidateTx
type ValidationResult struct {
	IsValid      bool
	ErrorMessage string
}

// TransactionStatus represents current status of transaction
type TransactionStatus string

const (
	// TransactionStatusNotFound represents state when tx does not exists in the blockchain
	TransactionStatusNotFound = "not_found"
	// TransactionStatusUnconfirmed represents state when tx located in utx-pool
	TransactionStatusUnconfirmed = "unconfirmed"
	// TransactionStatusConfirmed represents state when tx is confirmed
	TransactionStatusConfirmed = "confirmed"
)

// Availability represents map of string:isTransactionAvailable (tx has status not TransactionStatusNotFound)
type Availability map[string]bool

// Interactor ...
type Interactor interface {
	ValidateTx(string) (*ValidationResult, Error)
	BroadcastTx(string) (string, Error)
	WaitForTxStatus(string, TransactionStatus) (int32, Error)
	GetCurrentHeight() (int32, Error)
	WaitForNHeights(int32) Error
	WaitForNextHeight() Error
	GetTxsAvailability([]string) (Availability, Error)
}

type impl struct {
	nodeURL              url.URL
	nodeAPIKey           string
	logger               *zap.Logger
	waitForTxStatusDelay time.Duration
	waitForTxTimeout     time.Duration
}

// New returns instance of Interactor interface implementation
func New(nodeURL url.URL, nodeAPIKey string, waitForTxStatusDelay, waitForTxTimeout int32) Interactor {
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
		return 0, NewError(InternalError, err.Error())
	}

	if resp != nil {
		defer resp.Body.Close()
	}

	blocksHeight := blocksHeightResponse{}
	err = json.NewDecoder(resp.Body).Decode(&blocksHeight)
	if err != nil {
		return 0, NewError(InternalError, err.Error())
	}
	return blocksHeight.Height, nil
}

// ValidateTx validates given tx using node
func (r *impl) ValidateTx(tx string) (*ValidationResult, Error) {
	validateURL := r.nodeURL
	validateURL.Path = "/debug/validate"

	req, err := http.NewRequest("POST", validateURL.String(), strings.NewReader(tx))
	if err != nil {
		r.logger.Error("Cannot create validateTx request", zap.Error(err))
		return nil, NewError(InternalError, err.Error())
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-Key", r.nodeAPIKey)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, NewError(InternalError, err.Error())
	}

	if resp != nil {
		defer resp.Body.Close()
	}

	if resp.StatusCode == http.StatusOK {
		validateTx := validateTxResponse{}
		err = json.NewDecoder(resp.Body).Decode(&validateTx)
		if err != nil {
			return nil, NewError(InternalError, err.Error())
		}

		return &ValidationResult{
			IsValid:      validateTx.Valid,
			ErrorMessage: validateTx.Error,
		}, nil
	}

	validateTxError := errorResponse{}
	err = json.NewDecoder(resp.Body).Decode(&validateTxError)
	if err != nil {
		return nil, NewError(InternalError, err.Error())
	}

	return &ValidationResult{
		IsValid:      false,
		ErrorMessage: validateTxError.Message,
	}, nil
}

// BroadcastTx broadcasts given tx to blockhain
func (r *impl) BroadcastTx(tx string) (string, Error) {
	broadcastURL := r.nodeURL
	broadcastURL.Path = "/transactions/broadcast"

	resp, err := http.Post(broadcastURL.String(), "application/json", strings.NewReader(tx))
	if err != nil {
		return "", NewError(InternalError, err.Error())
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
				return "", NewError(InternalError, err.Error())
			}

			txStatus, wavesErr := r.getTxStatus(t.id)
			if wavesErr != nil {
				return "", wavesErr
			}

			if txStatus.Status != TransactionStatusNotFound {
				return t.id, nil
			}

			var errorResponseDto errorResponse
			err = json.NewDecoder(resp.Body).Decode(&errorResponseDto)
			if err != nil {
				return "", NewError(InternalError, err.Error())
			}

			return "", NewError(BroadcastClientError, errorResponseDto.Message)
		}

		return "", NewError(BroadcastServerError, resp.Status)
	}

	var broadcastResponseDto broadcastResponse
	err = json.NewDecoder(resp.Body).Decode(&broadcastResponseDto)
	if err != nil {
		return "", NewError(InternalError, err.Error())
	}

	return broadcastResponseDto.ID, nil
}

// WaitForTx waits for tx status appearance in the blockchain
func (r *impl) WaitForTxStatus(txID string, waitForStatus TransactionStatus) (int32, Error) {
	start := time.Now()
	for {
		status, err := r.getTxStatus(txID)
		if err != nil {
			return 0, err
		}

		if status.Status == waitForStatus {
			return status.Height, nil
		}

		now := time.Now()
		if now.Sub(start) > r.waitForTxTimeout {
			return 0, NewError(WaitForTxStatusTimeoutError, "wait for tx status time deadline is reached")
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
			return NewError(InternalError, err.Error())
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
func (r *impl) GetTxsAvailability(txIDs []string) (Availability, Error) {
	txsStatusURL := r.nodeURL
	txsStatusURL.Path = "/transactions/status"

	req, err := json.Marshal(transactionsStatusRequest{
		IDs: txIDs,
	})
	if err != nil {
		return nil, NewError(InternalError, err.Error())
	}

	resp, err := http.Post(txsStatusURL.String(), "application/json", bytes.NewBuffer(req))
	if err != nil {
		return nil, NewError(InternalError, err.Error())
	}

	if resp != nil {
		defer resp.Body.Close()
	}

	if resp.StatusCode != http.StatusOK {
		errorResponseDto := errorResponse{}
		err = json.NewDecoder(resp.Body).Decode(&errorResponseDto)
		if err != nil {
			return nil, NewError(InternalError, err.Error())
		}
		return nil, NewError(InternalError, errorResponseDto.Message)
	}

	txStatuses := transactionsStatusResponse{}
	err = json.NewDecoder(resp.Body).Decode(&txStatuses)
	if err != nil {
		return nil, NewError(InternalError, err.Error())
	}

	availability := Availability{}
	for _, txStatus := range txStatuses {
		availability[txStatus.ID] = txStatus.Status != TransactionStatusNotFound
	}

	return availability, nil
}

func (r *impl) getTxStatus(txID string) (*transactionStatusResponse, Error) {
	txStatusURL := r.nodeURL
	txStatusURL.Path = "/transactions/status"

	q := url.Values{}
	q.Set("id", txID)
	txStatusURL.RawQuery = q.Encode()

	resp, err := http.Get(txStatusURL.String())
	if err != nil {
		return nil, NewError(InternalError, err.Error())
	}

	if resp != nil {
		defer resp.Body.Close()
	}

	if resp.StatusCode != http.StatusOK {
		return nil, NewError(GetTxStatusError, resp.Status)
	}

	txStatus := transactionsStatusResponse{}
	err = json.NewDecoder(resp.Body).Decode(&txStatus)
	if err != nil {
		return nil, NewError(InternalError, err.Error())
	}

	return &txStatus[0], nil
}
