package worker

import "fmt"

// RecoverableError represents recoverable error
type RecoverableError struct {
	reason string
}

// ErrorWithReason ...
type ErrorWithReason interface {
	error
	Reason() string
}

// ErrorWithReasonAndCode ...
type ErrorWithReasonAndCode interface {
	ErrorWithReason
	ErrorCode() uint16
}

// NewRecoverableError returns new RecoverableError based on err
func NewRecoverableError(reason string) ErrorWithReason {
	return RecoverableError{
		reason: reason,
	}
}

func (e RecoverableError) Error() string {
	return fmt.Sprintf("recoverable error with reason: %s.", e.reason)
}

// Reason returns error reason
func (e RecoverableError) Reason() string {
	return e.reason
}

// NonRecoverableError represents non recoverable error
type NonRecoverableError struct {
	reason string
	code   uint16
}

// NewNonRecoverableError returns new NonRecoverableError based on err
func NewNonRecoverableError(reason string, code uint16) ErrorWithReasonAndCode {
	return NonRecoverableError{
		reason: reason,
		code:   code,
	}
}

func (e NonRecoverableError) Error() string {
	return fmt.Sprintf("non-recoverable error with reason: %s.", e.reason)
}

// Reason returns error reason
func (e NonRecoverableError) Reason() string {
	return e.reason
}

// Code returns error code
func (e NonRecoverableError) ErrorCode() uint16 {
	return e.code
}

// FatalError represents fatal error (aka exception)
type FatalError struct {
	reason string
}

// NewFatalError returns new FatalError based on err
func NewFatalError(reason string) ErrorWithReason {
	return FatalError{
		reason: reason,
	}
}

func (e FatalError) Error() string {
	return fmt.Sprintf("fatal error with reason: %s.", e.reason)
}

// Reason returns error reason
func (e FatalError) Reason() string {
	return e.reason
}
