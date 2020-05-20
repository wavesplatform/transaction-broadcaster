package waves

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/waves-exchange/broadcaster/internal/log"
	"go.uber.org/zap"
)

type broadcastResponse struct {
	id string
}

// NodeInteractor ...
type NodeInteractor interface {
	ValidateTx(string) (bool, Error)
	BroadcastTx(string) (string, Error)
	WaitForTx(string) Error
}

type impl struct {
	nodeURL          url.URL
	nodeAPIKey       string
	logger           *zap.Logger
	waitForTxDelay   time.Duration
	waitForTxTimeout time.Duration
}

const (
	errorWaitForTxTimeoutMessage = "Wait for tx time duration is reached."
)

// NewNodeInteractor creates new Waves Node interactor
func NewNodeInteractor(nodeURL url.URL, nodeAPIKey string) NodeInteractor {
	logger := log.Logger.Named("nodeInteractor")
	waitForTxDelay := time.Duration(1 * time.Second)
	waitForTxTimeout := time.Duration(30 * time.Second)

	return &impl{
		nodeURL:          nodeURL,
		nodeAPIKey:       nodeAPIKey,
		logger:           logger,
		waitForTxDelay:   waitForTxDelay,
		waitForTxTimeout: waitForTxTimeout,
	}
}

// ValidateTx validates given tx using node
func (r *impl) ValidateTx(tx string) (bool, Error) {
	validateURL := r.nodeURL
	validateURL.Path = "/debug/validate"
	reader := strings.NewReader(tx)

	req, err := http.NewRequest("POST", validateURL.String(), reader)
	if err != nil {
		r.logger.Error("Cannot create validateTx request", zap.Error(err))
		return false, NewWavesError(ValidateTxServerError, err.Error())
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-Key", r.nodeAPIKey)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false, NewWavesError(ValidateTxServerError, err.Error())
	}

	if resp != nil {
		defer resp.Body.Close()
	}

	return resp.StatusCode == http.StatusOK, nil
}

// BroadcastTx broadcasts given tx to blockhain
func (r *impl) BroadcastTx(tx string) (string, Error) {
	broadcastURL := r.nodeURL
	broadcastURL.Path = "/transactions/broadcast"

	reader := strings.NewReader(tx)
	resp, err := http.Post(broadcastURL.String(), "application/json", reader)
	if err != nil {
		return "", NewWavesError(BroadcastServerError, err.Error())
	}

	if resp != nil {
		defer resp.Body.Close()
	}

	if resp.StatusCode != http.StatusOK {
		return "", NewWavesError(BroadcastClientError, fmt.Sprintf("Broadcasting tx error, status code: %d", resp.StatusCode))
	}

	var broadcastResponseDto broadcastResponse
	err = json.NewDecoder(resp.Body).Decode(&broadcastResponseDto)
	if err != nil {
		return "", NewWavesError(NodeInteractorInternalError, err.Error())
	}
	return broadcastResponseDto.id, nil
}

// WaitForTx waits for tx arriving in blockchain
func (r *impl) WaitForTx(txID string) Error {
	start := time.Now()
	for {
		found, err := r.isTxInBlockhain(txID)
		if err != nil {
			return NewWavesError(WaitForTxServerError, err.Error())
		}

		if found {
			return nil
		}

		now := time.Now()
		if now.Sub(start) > r.waitForTxTimeout {
			return NewWavesError(WaitForTxTimeoutError, errorWaitForTxTimeoutMessage)
		}

		time.Sleep(r.waitForTxDelay)
	}
}

func (r *impl) isTxInBlockhain(txID string) (bool, error) {
	checkTxURL := r.nodeURL
	checkTxURL.Path = fmt.Sprintf("/transactions/info/%s", txID)

	resp, err := http.Get(checkTxURL.String())
	if err != nil {
		return false, err
	}

	if resp != nil {
		defer resp.Body.Close()
	}

	return resp.StatusCode == http.StatusOK, nil
}
