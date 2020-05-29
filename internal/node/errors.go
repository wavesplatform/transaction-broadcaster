package node

// Waves error codes
const (
	BroadcastClientError = iota + 0
	BroadcastServerError
	GetTxStatusError
	WaitForTxStatusTimeoutError
	InternalError = 999
)

type wavesErrorImpl struct {
	code    uint16
	message string
}

// Error ...
type Error interface {
	error
	Code() uint16
}

// NewError creates new error
func NewError(code uint16, message string) Error {
	return wavesErrorImpl{
		code:    code,
		message: message,
	}
}

func (e wavesErrorImpl) Code() uint16 {
	return e.code
}

func (e wavesErrorImpl) Error() string {
	return e.message
}
