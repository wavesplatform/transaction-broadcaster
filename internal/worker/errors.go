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
}

// NewNonRecoverableError returns new NonRecoverableError based on err
func NewNonRecoverableError(reason string) ErrorWithReason {
	return NonRecoverableError{
		reason: reason,
	}
}

func (e NonRecoverableError) Error() string {
	return fmt.Sprintf("non-recoverable error with reason: %s.", e.reason)
}

// Reason returns error reason
func (e NonRecoverableError) Reason() string {
	return e.reason
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
