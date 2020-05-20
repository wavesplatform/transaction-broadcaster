package log

import (
	"net/http"
	"time"

	ltsv "github.com/hnakamur/zap-ltsv"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// AccessLogger internally uses logger for  requests logging
type AccessLogger func(*http.Request, int, int64, time.Duration)

// Logger is global variable
var Logger *zap.Logger

// Init initialize zap logger
func Init(dev bool) error {
	logCfg := ltsv.NewProductionConfig()
	logCfg.DisableCaller = !dev
	logCfg.EncoderConfig.NameKey = "loc"
	logCfg.EncoderConfig.LevelKey = "lvl"
	logCfg.EncoderConfig.EncodeDuration = zapcore.StringDurationEncoder
	logCfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	logCfg.Development = dev

	if dev {
		logCfg.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
	}

	err := ltsv.RegisterLTSVEncoder()
	if err != nil {
		return err
	}

	l, err := logCfg.Build()
	if err != nil {
		return err
	}

	Logger = l
	return nil
}

// CreateAccessLogger returns AccessLogger
func CreateAccessLogger(logger *zap.Logger, msg string) AccessLogger {
	return func(req *http.Request, statusCode int, contentLength int64, duration time.Duration) {
		logger.Info(
			msg,
			zap.String("path", req.URL.Path),
			zap.String("req_id", req.Header.Get("X-Request-Id")),
			zap.String("protocol", req.Proto),
			zap.String("method", req.Method),
			zap.Int("status", statusCode),
			zap.Duration("response_time", duration),
			zap.Int64("content_len", contentLength),
			zap.Strings("fwd_for", req.Header.Values("X-Forwarded-For")),
			zap.String("ip", req.RemoteAddr),
			zap.String("ua", req.UserAgent()),
		)
	}
}
