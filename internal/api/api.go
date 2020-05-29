package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/waves-exchange/broadcaster/internal/log"
	"github.com/waves-exchange/broadcaster/internal/node"
	"github.com/waves-exchange/broadcaster/internal/repository"
	"go.uber.org/zap"

	"github.com/gin-gonic/gin"
)

var errInternalServerError = errors.New("Internal Server Error")

type txDto struct {
	ID string `json:"id"`
}

type txsRequest struct {
	Txs []string `json:"txs" binding:"required"`
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
		// retrieve transactions sequence from post request body
		req := txsRequest{}
		if err := c.ShouldBindJSON(&req); err != nil {
			if err.Error() == "EOF" {
				renderError(c, http.StatusBadRequest, MissingRequiredParameter("txs"))
			} else if err.Error() == "Key: 'txsRequest.Txs' Error:Field validation for 'Txs' failed on the 'required' tag" {
				renderError(c, http.StatusBadRequest, MissingRequiredParameter("txs"))
			} else {
				renderError(c, http.StatusBadRequest, InvalidParameterValue("txs", "txs parameter has to be an array of string"))
			}

			return
		}

		if len(req.Txs) == 0 {
			renderError(c, http.StatusBadRequest, InvalidParameterValue("txs", "There is no any txs in the request."))
			return
		}

		// validate the first tx
		validationResult, wavesErr := nodeInteractor.ValidateTx(req.Txs[0])
		if wavesErr != nil {
			logger.Error("cannot validate the first tx of sequence", zap.String("req_id", c.Request.Header.Get("X-Request-Id")), zap.Error(wavesErr))
			renderError(c, http.StatusInternalServerError, InternalServerError())
			return
		}
		if !validationResult.IsValid {
			renderError(c, http.StatusBadRequest, InvalidParameterValue("txs", fmt.Sprintf("The first tx is invalid: %s.", validationResult.ErrorMessage)))
			return
		}

		var txs []repository.TxWithIDDto
		t := txDto{}
		for _, tx := range req.Txs {
			if err := json.NewDecoder(strings.NewReader(tx)).Decode(&t); err != nil {
				logger.Error("cannot decode one of the sequence's tx", zap.String("req_id", c.Request.Header.Get("X-Request-Id")), zap.Error(err))
				renderError(c, http.StatusInternalServerError, InternalServerError())
				return
			}
			txs = append(txs, repository.TxWithIDDto{
				ID: t.ID,
				Tx: tx,
			})
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
