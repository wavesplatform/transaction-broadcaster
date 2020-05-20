package api

const (
	// validation errors
	missingRequiredParameter = 10200
	invalidParameterValue    = 10201

	// internal server errors
	internalServerError = 10500
)

// error ...
type apiErrorImpl struct {
	code    uint16
	details map[string]string // at this moment there is only string at the value type
}

// Error represents api error
type Error interface {
	Message() string
	Details() map[string]string
	Code() uint16
}

// HTTPError represents single api http error
type HTTPError struct {
	Code    uint16            `json:"code"`
	Message string            `json:"message"`
	Details map[string]string `json:"details,omitempty"`
}

// HTTPErrors represents array of http errors
type HTTPErrors struct {
	Errors []HTTPError `json:"errors"`
}

// MissingRequiredParameter ...
func MissingRequiredParameter(parameterName string) Error {
	details := map[string]string{
		"parameter": parameterName,
	}
	return &apiErrorImpl{code: missingRequiredParameter, details: details}
}

// InvalidParameterValue ...
func InvalidParameterValue(parameterName string, reason string) Error {
	details := map[string]string{
		"parameter": parameterName,
		"reason":    reason,
	}
	return &apiErrorImpl{code: invalidParameterValue, details: details}
}

// InternalServerError ...
func InternalServerError() Error {
	return &apiErrorImpl{code: internalServerError, details: nil}
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

// Message ...
func (err *apiErrorImpl) Message() string {
	switch err.code {
	case missingRequiredParameter:
		return "Missing required parameter."
	case invalidParameterValue:
		return "Invalid parameter value."

	case internalServerError:
		fallthrough
	default:
		return "Internal server error."
	}
}

// Details ...
func (err *apiErrorImpl) Details() map[string]string {
	return err.details
}

// Code ...
func (err *apiErrorImpl) Code() uint16 {
	return err.code
}
