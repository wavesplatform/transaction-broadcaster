package api

import (
	"fmt"
)

const _internalServerErrorMessage = "Internal Server Error"

const (
	// common validation errors
	_missingRequiredParameter = 950200
	_invalidParameterValue    = 950201

	// service errors
	_txsDuplicatesError  = 951001
	_invalidFirstTxError = 951002
)

type errorDetails map[string]interface{}

type apiErrorImpl struct {
	code    uint32
	details errorDetails
}

// Error represents API error
type Error interface {
	Error() string
	Message() string
	Details() errorDetails
	Code() uint32
}

// HTTPError represents single api http error
type HTTPError struct {
	Code    uint32       `json:"code"`
	Message string       `json:"message"`
	Details errorDetails `json:"details,omitempty"`
}

// HTTPErrors represents array of http errors
type HTTPErrors struct {
	Errors []HTTPError `json:"errors"`
}

// NewError returns instance of Error interface implementation
func NewError(code uint32, details errorDetails) Error {
	return &apiErrorImpl{code: code, details: details}
}

// MissingRequiredParameter ...
func MissingRequiredParameter(parameterName string) Error {
	details := errorDetails{
		"parameter": parameterName,
	}
	return NewError(_missingRequiredParameter, details)
}

// InvalidParameterValue ...
func InvalidParameterValue(parameterName string, reason string) Error {
	details := errorDetails{
		"parameter": parameterName,
		"reason":    reason,
	}
	return NewError(_invalidParameterValue, details)
}

// TxsDuplicatesError ...
func TxsDuplicatesError(meta errorDetails) Error {
	return NewError(_txsDuplicatesError, meta)
}

// InvalidFirstTxError ...
func InvalidFirstTxError(reason string) Error {
	details := errorDetails{
		"reason": reason,
	}
	return NewError(_invalidFirstTxError, details)
}

// SingleHTTPError returns HTTPErrors build from single HTTPError
func SingleHTTPError(err Error) HTTPErrors {
	return HTTPErrors{
		Errors: []HTTPError{{
			Code:    err.Code(),
			Message: err.Message(),
			Details: err.Details(),
		}},
	}
}

// Error
func (err *apiErrorImpl) Error() string {
	return fmt.Sprintf("API Error: %s [%d]", err.Message(), err.Code())
}

// Message ...
func (err *apiErrorImpl) Message() string {
	switch err.code {
	case _missingRequiredParameter:
		return "Missing required parameter."
	case _invalidParameterValue:
		return "Invalid parameter value."

	case _txsDuplicatesError:
		return "There are duplicates in the transactions array."
	case _invalidFirstTxError:
		return "The first transaction is invalid."

	default:
		return _internalServerErrorMessage
	}
}

// Details ...
func (err *apiErrorImpl) Details() errorDetails {
	return err.details
}

// Code ...
func (err *apiErrorImpl) Code() uint32 {
	return err.code
}
