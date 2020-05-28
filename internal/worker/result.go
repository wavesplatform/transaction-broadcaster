package worker

// Result ...
type Result struct {
	SequenceID int64
	Error      error
}

// NewResult returns new instance of Result
func NewResult(seqID int64, err error) Result {
	return Result{
		SequenceID: seqID,
		Error:      err,
	}
}
