package node

// Waves error codes
const (
	BroadcastClientError = iota + 0
	BroadcastServerError
	GetTxStatusError
	WaitForTxStatusTimeoutError
	TxNotFoundError
	InternalError = 999
)

type wavesErrorImpl struct {
	code          uint16
	nodeErrorCode uint16
	message       string
}

// Error ...
type Error interface {
	error
	Code() uint16
	NodeErrorCode() uint16
}

// NewError creates new error
func NewError(code uint16, message string) Error {
	return wavesErrorImpl{
		code:    code,
		message: message,
	}
}

// WithNodeError adds node error code to NewError
func WithNodeError(err Error, nodeErrorCode uint16) Error {
	return wavesErrorImpl{
		code:          err.Code(),
		message:       err.Error(),
		nodeErrorCode: nodeErrorCode,
	}
}

func (e wavesErrorImpl) Code() uint16 {
	return e.code
}

func (e wavesErrorImpl) NodeErrorCode() uint16 {
	return e.nodeErrorCode
}

func (e wavesErrorImpl) Error() string {
	return e.message
}
