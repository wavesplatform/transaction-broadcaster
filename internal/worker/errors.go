package worker

import "fmt"

// RecoverableError represents recoverable error
type RecoverableError struct {
	message string
}

// NewRecoverableError returns new RecoverableError based on err
func NewRecoverableError(message string) RecoverableError {
	return RecoverableError{
		message: message,
	}
}

func (e RecoverableError) Error() string {
	return fmt.Sprintf("Recoverable error with reason: %s.", e.message)
}

// NonRecoverableError represents non recoverable error
type NonRecoverableError struct {
	message string
}

// NewNonRecoverableError returns new NonRecoverableError based on err
func NewNonRecoverableError(message string) NonRecoverableError {
	return NonRecoverableError{
		message: message,
	}
}

func (e NonRecoverableError) Error() string {
	return fmt.Sprintf("Nonrecoverable error with reason: %s.", e.message)
}

// FatalError represents fatal error (aka exception)
type FatalError struct {
	message string
}

// NewFatalError returns new FatalError based on err
func NewFatalError(message string) FatalError {
	return FatalError{
		message: message,
	}
}

func (e FatalError) Error() string {
	return fmt.Sprintf("Fatal error with reason: %s.", e.message)
}
