package api

import (
	"time"

	"github.com/wavesplatform/transaction-broadcaster/internal/log"
	"github.com/wavesplatform/transaction-broadcaster/internal/node"
	"github.com/wavesplatform/transaction-broadcaster/internal/repository"
	"go.uber.org/zap"

	"github.com/gin-gonic/gin"
)

type errorRenderer func(*gin.Context, int, Error)

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

func createErrorRenderer(logger *zap.Logger) errorRenderer {
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

	r.GET("/sequences/:id", getSequence(logger, renderError, repo)).POST("/sequences", createSequence(logger, renderError, repo, nodeInteractor, sequenceChan))

	return r
}
