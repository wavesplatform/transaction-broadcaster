package worker

// Result ...
type Result struct {
	SequenceID int64
	Error      error
}

// CreateResult returns new instance of Result
func CreateResult(seqID int64, err error) Result {
	return Result{
		SequenceID: seqID,
		Error:      err,
	}
}
