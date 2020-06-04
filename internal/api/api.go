package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/wavesplatform/transaction-broadcaster/internal/log"
	"github.com/wavesplatform/transaction-broadcaster/internal/node"
	"github.com/wavesplatform/transaction-broadcaster/internal/repository"
	"go.uber.org/zap"

	"github.com/gin-gonic/gin"
)

var errInternalServerError = errors.New("Internal Server Error")

type txDto struct {
	ID string `json:"id"`
}

type txsRequest struct {
	Txs []string `json:"transactions" binding:"required"`
}

func accessLog(logger *zap.Logger) func(*gin.Context) {
	return func(c *gin.Context) {
		t := time.Now()

		c.Next()

		duration := time.Since(t)

		logger.Info(
			"handle request",
			zap.String("method", c.Request.Method),
			zap.String("path", c.Request.URL.Path),
			zap.Int("status", c.Writer.Status()),
			zap.Int("cl", c.Writer.Size()),
			zap.String("req_id", c.Request.Header.Get("X-Request-Id")),
			zap.String("ip", c.ClientIP()),
			zap.String("protocol", c.Request.Proto),
			zap.String("ua", c.Request.UserAgent()),
			zap.Duration("latency", duration),
		)
	}
}

func createErrorRenderer(logger *zap.Logger) func(*gin.Context, int, Error) {
	return func(ctx *gin.Context, status int, err Error) {
		logger.Warn("rendering http error",
			zap.Int("status", status),
			zap.Error(err),
		)

		ctx.JSON(status, SingleHTTPError(err))
	}
}

func parseTransactions(request string) ([]string, error) {
	var transactions []string
	b := strings.Builder{}
	skipStringLen := len(`{"transactions":[`)
	if len(request) < skipStringLen {
		return nil, errors.New("length of request string is too small")
	}
	transactionsString := request[skipStringLen : len(request)-1]
	bracketsCount := 0
	for _, ch := range transactionsString {
		if ch == '{' {
			bracketsCount++
		}
		if ch == '}' {
			bracketsCount--
			// json object was closed
			if bracketsCount == 0 {
				// dont forget append the last bracket
				b.WriteRune(ch)
				transactions = append(transactions, b.String())
				// reset for retrieving the next object
				b.Reset()
			}
		}
		// do not add runes between brackets
		if bracketsCount > 0 {
			b.WriteRune(ch)
		}
	}

	return transactions, nil
}

// New ...
func New(repo repository.Repository, nodeInteractor node.Interactor, sequenceChan chan<- int64) *gin.Engine {
	logger := log.Logger.Named("server.requestHandler")

	renderError := createErrorRenderer(logger)

	r := gin.New()

	gin.DisableConsoleColor()

	r.Use(gin.Recovery(), accessLog(logger))

	r.GET("/sequences/:id", func(c *gin.Context) {
		rawID := c.Param("id")

		if rawID == "" {
			renderError(c, http.StatusBadRequest, MissingRequiredParameter("id"))
			return
		}

		id, err := strconv.ParseInt(rawID, 10, 64)
		if err != nil {
			renderError(c, http.StatusBadRequest, InvalidParameterValue("id", err.Error()))
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
	}).POST("/sequences", func(c *gin.Context) {
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
			renderError(c, http.StatusBadRequest, InvalidParameterValue("transactions", "Invalid request."))
			return
		}

		if len(transactions) == 0 {
			renderError(c, http.StatusBadRequest, InvalidParameterValue("transactions", "There is no any transactions in the request."))
			return
		}

		var txs []repository.TxWithIDDto
		// for txID uniqueness checking
		var txIDs = make(map[string]bool)
		t := txDto{}
		for _, tx := range transactions {
			if err := json.NewDecoder(strings.NewReader(tx)).Decode(&t); err != nil {
				logger.Error("cannot decode one of the sequence's transaction", zap.String("req_id", c.Request.Header.Get("X-Request-Id")), zap.Error(err))
				renderError(c, http.StatusBadRequest, InvalidParameterValue("transactions", fmt.Sprintf("Error occurred while decoding transactions: %s.", err.Error())))
				return
			}
			if _, ok := txIDs[t.ID]; ok {
				logger.Error("there are duplicates in the transactions array", zap.String("req_id", c.Request.Header.Get("X-Request-Id")), zap.Error(err))
				renderError(c, http.StatusBadRequest, InvalidParameterValue("transactions", "There are duplicates in the transactions array"))
				return
			}
			txs = append(txs, repository.TxWithIDDto{
				ID: t.ID,
				Tx: tx,
			})
			txIDs[t.ID] = true
		}

		// validate the first tx
		validationResult, wavesErr := nodeInteractor.ValidateTx(transactions[0])
		if wavesErr != nil {
			logger.Error("cannot validate the first tx of sequence", zap.String("req_id", c.Request.Header.Get("X-Request-Id")), zap.Error(wavesErr))
			renderError(c, http.StatusInternalServerError, InternalServerError())
			return
		}
		if !validationResult.IsValid {
			renderError(c, http.StatusBadRequest, InvalidParameterValue("transactions", fmt.Sprintf("The first transaction is invalid: %s.", validationResult.ErrorMessage)))
			return
		}

		sequenceID, err := repo.CreateSequence(txs)
		if err != nil {
			logger.Error("cannot create sequence", zap.String("req_id", c.Request.Header.Get("X-Request-Id")), zap.Error(err))
			renderError(c, http.StatusInternalServerError, InternalServerError())
			return
		}

		sequenceChan <- sequenceID

		c.JSON(http.StatusOK, gin.H{
			"id": sequenceID,
		})
	})

	return r
}
