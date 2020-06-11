package api

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/wavesplatform/transaction-broadcaster/internal/node"
	"github.com/wavesplatform/transaction-broadcaster/internal/repository"
	"go.uber.org/zap"
)

func getSequence(logger *zap.Logger, renderError errorRenderer, repo repository.Repository) func(*gin.Context) {
	return func(c *gin.Context) {
		rawID := c.Param("id")

		if rawID == "" {
			renderError(c, http.StatusBadRequest, MissingRequiredParameter("id"))
			return
		}

		id, err := strconv.ParseInt(rawID, 10, 64)
		if err != nil {
			renderError(c, http.StatusBadRequest, InvalidParameterValue("id", fmt.Sprintf("Error occured while parsing id: %s.", err.Error()), nil))
			return
		}

		sequence, err := repo.GetSequenceByID(id)
		if err != nil {
			logger.Error("cannot get sequence from db", zap.String("req_id", c.Request.Header.Get("X-Request-Id")), zap.Error(err))
			renderError(c, http.StatusInternalServerError, InternalServerError())
			return
		}

		if sequence == nil {
			c.JSON(http.StatusNotFound, gin.H{
				"message": "Sequence not found",
			})
			return
		}

		c.JSON(http.StatusOK, sequence)
	}
}

func createSequence(logger *zap.Logger, renderError errorRenderer, repo repository.Repository, nodeInteractor node.Interactor, sequenceChan chan<- int64) func(*gin.Context) {
	return func(c *gin.Context) {
		// retrieve transactions from post request body
		buf := bytes.Buffer{}
		_, err := buf.ReadFrom(c.Request.Body)
		if err != nil {
			logger.Error("cannot get request body", zap.String("req_id", c.Request.Header.Get("X-Request-Id")), zap.Error(err))
			renderError(c, http.StatusInternalServerError, InternalServerError())
			return
		}

		transactions, err := parseTransactions(buf.String())
		if err != nil {
			renderError(c, http.StatusBadRequest, InvalidParameterValue("transactions", "Invalid request.", nil))
			return
		}

		if len(transactions) == 0 {
			renderError(c, http.StatusBadRequest, InvalidParameterValue("transactions", "There are not any transactions in the request.", nil))
			return
		}

		var txs []string
		// for tx uniqueness checking
		var txHashes = make(map[string]int)
		for idx, tx := range transactions {
			txHash := md5.Sum([]byte(tx))
			txHashString := hex.EncodeToString(txHash[:])
			if _, ok := txHashes[txHashString]; ok {
				logger.Error("there are duplicates in the transactions array", zap.String("req_id", c.Request.Header.Get("X-Request-Id")), zap.Error(err))
				renderError(c, http.StatusBadRequest, InvalidParameterValue("transactions", "There are duplicates in the transactions array.", errorDetails{
					"duplicates": []int{txHashes[txHashString], idx},
				}))
				return
			}
			txs = append(txs, tx)
			txHashes[txHashString] = idx
		}

		// validate the first tx
		validationResult, wavesErr := nodeInteractor.ValidateTx(transactions[0])
		if wavesErr != nil {
			logger.Error("cannot validate the first tx of sequence", zap.String("req_id", c.Request.Header.Get("X-Request-Id")), zap.Error(wavesErr))
			renderError(c, http.StatusInternalServerError, InternalServerError())
			return
		}
		if !validationResult.IsValid {
			renderError(c, http.StatusBadRequest, InvalidParameterValue("transactions", "The first transaction is invalid.", errorDetails{
				"errorMessage": validationResult.ErrorMessage,
			}))
			return
		}

		sequenceID, err := repo.CreateSequence(txs)
		if err != nil {
			logger.Error("cannot create sequence", zap.String("req_id", c.Request.Header.Get("X-Request-Id")), zap.Error(err))
			renderError(c, http.StatusInternalServerError, InternalServerError())
			return
		}

		sequenceChan <- sequenceID

		c.JSON(http.StatusCreated, gin.H{
			"id": sequenceID,
		})
	}
}
