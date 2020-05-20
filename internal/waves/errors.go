package waves

// Waves error codes
const (
	ValidateTxServerError = iota + 0
	BroadcastClientError
	BroadcastServerError
	WaitForTxTimeoutError
	WaitForTxServerError
	NodeInteractorInternalError = 999
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

// NewWavesError creates new waves error
func NewWavesError(code uint16, message string) Error {
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
