package api

import (
	"fmt"
)

const (
	// validation errors
	_missingRequiredParameter = 10200
	_invalidParameterValue    = 10201

	// internal server errors
	_internalServerError = 10500
)

type errorDetails map[string]interface{}

type apiErrorImpl struct {
	code    uint16
	details errorDetails
}

// Error represents API error
type Error interface {
	Error() string
	Message() string
	Details() errorDetails
	Code() uint16
}

// HTTPError represents single api http error
type HTTPError struct {
	Code    uint16       `json:"code"`
	Message string       `json:"message"`
	Details errorDetails `json:"details,omitempty"`
}

// HTTPErrors represents array of http errors
type HTTPErrors struct {
	Errors []HTTPError `json:"errors"`
}

// NewError returns instance of Error interface implementation
func NewError(code uint16, details errorDetails) Error {
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
func InvalidParameterValue(parameterName string, reason string, meta errorDetails) Error {
	details := errorDetails{
		"parameter": parameterName,
		"reason":    reason,
	}
	if meta != nil {
		for key, value := range meta {
			if key != "parameter" && key != "reason" {
				details[key] = value
			}
		}
	}
	return NewError(_invalidParameterValue, details)
}

// InternalServerError ...
func InternalServerError() Error {
	return NewError(_internalServerError, nil)
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

	case _internalServerError:
		fallthrough
	default:
		return "Internal server error."
	}
}

// Details ...
func (err *apiErrorImpl) Details() errorDetails {
	return err.details
}

// Code ...
func (err *apiErrorImpl) Code() uint16 {
	return err.code
}
