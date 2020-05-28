package log

import (
	ltsv "github.com/hnakamur/zap-ltsv"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

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
