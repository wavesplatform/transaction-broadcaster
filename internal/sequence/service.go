package sequence

import (
	"time"

	"github.com/go-pg/pg"
)

// Sequence represents sequence type with json marshaling description
type Sequence struct {
	ID               int64         `json:"id"`
	BroadcastedCount uint32        `json:"broadcastedCount"`
	TotalCount       uint32        `json:"totalCount"`
	State            SequenceState `json:"state"`
	ErrorMessage     string        `json:"errorMessage"`
	CreatedAt        time.Time     `json:"createdAt"`
	UpdatedAt        time.Time     `json:"updatedAt"`
}

// SequenceTx represents sequence transactions type
type SequenceTx struct {
	ID                 string           `json:"id"`
	SequenceID         int64            `json:"-"`
	State              TransactionState `json:"state"`
	ErrorMessage       string           `json:"errorMessage,omitempty"`
	PositionInSequence uint16           `json:"positionInSequence"`
	Tx                 string           `json:"tx"`
	CreatedAt          time.Time        `json:"createdAt"`
	UpdatedAt          time.Time        `json:"updatedAt"`
}

// Service ...
type Service interface {
	GetSequenceByID(id int64) (*Sequence, error)
	GetSequenceTxsByID(sequenceID int64) ([]*SequenceTx, error)
	GetHangingSequenceIds(ttl time.Duration) ([]int64, error)
	CreateSequence(txs []TxWithIDDto) (int64, error)
	SetSequenceProcessingStateByID(sequenceID int64) error
	SetSequenceDoneStateByID(sequenceID int64) error
	SetSequenceErrorStateByID(sequenceID int64, err error) error
	IncreaseSequenceBroadcastedCount(sequence Sequence) error
	SetSequenceTxState(tx *SequenceTx, newState TransactionState) error
	SetSequenceTxErrorState(tx *SequenceTx, errorMessage string) error
	SetSequenceTxsStateAfter(sequenceID int64, txID string, newState TransactionState) error
}

type serviceImpl struct {
	repo Repo
}

// NewService ...
func NewService(repo Repo) Service {
	return &serviceImpl{repo: repo}
}

// GetSequenceByID ...
func (s *serviceImpl) GetSequenceByID(sequenceID int64) (*Sequence, error) {
	seqDto, err := s.repo.GetSequenceByID(sequenceID)
	if err != nil {
		if err.Error() == pg.ErrNoRows.Error() {
			return nil, nil
		}
		return nil, err
	}

	seq := Sequence{
		ID:               seqDto.ID,
		BroadcastedCount: seqDto.BroadcastedCount,
		TotalCount:       seqDto.TotalCount,
		State:            seqDto.State,
		ErrorMessage:     seqDto.ErrorMessage,
		CreatedAt:        seqDto.CreatedAt,
		UpdatedAt:        seqDto.UpdatedAt,
	}
	return &seq, nil
}

func (s *serviceImpl) GetSequenceTxsByID(sequenceID int64) ([]*SequenceTx, error) {
	txsDto, err := s.repo.GetSequenceTxsByID(sequenceID)
	if err != nil {
		return nil, err
	}

	txs := []*SequenceTx{}
	for _, txDto := range txsDto {
		txs = append(txs, &SequenceTx{
			ID:                 txDto.ID,
			SequenceID:         txDto.SequenceID,
			State:              txDto.State,
			ErrorMessage:       txDto.ErrorMessage,
			PositionInSequence: txDto.PositionInSequence,
			Tx:                 txDto.Tx,
			CreatedAt:          txDto.CreatedAt,
			UpdatedAt:          txDto.UpdatedAt,
		})
	}

	return txs, nil
}

// GetHangingSequenceIds tries to get hanging sequence ids
// Hanging sequences - with is_processing=true, not totally broadcasted and not updated for ttl.
func (s *serviceImpl) GetHangingSequenceIds(ttl time.Duration) ([]int64, error) {
	return s.repo.GetHangingSequenceIds(ttl)
}

// CreateSequence ...
func (s *serviceImpl) CreateSequence(txs []TxWithIDDto) (int64, error) {
	return s.repo.CreateSequence(txs)
}

// SetSequenceProcessingState ...
func (s *serviceImpl) SetSequenceProcessingStateByID(sequenceID int64) error {
	return s.repo.SetSequenceState(sequenceID, SequenceStateProcessing)
}

// SetSequenceDoneState ...
func (s *serviceImpl) SetSequenceDoneStateByID(sequenceID int64) error {
	return s.repo.SetSequenceState(sequenceID, SequenceStateDone)
}

// SetSequenceErrorState ...
func (s *serviceImpl) SetSequenceErrorStateByID(sequenceID int64, err error) error {
	return s.repo.SetSequenceErrorState(sequenceID, err)
}

// IncreaseSequenceBroadcastedCount ...
func (s *serviceImpl) IncreaseSequenceBroadcastedCount(sequence Sequence) error {
	return s.repo.IncreaseSequenceBroadcastedCount(sequence.ID)
}

// SetSequenceTxState
func (s *serviceImpl) SetSequenceTxState(tx *SequenceTx, newState TransactionState) error {
	return s.repo.SetSequenceTxState(tx.SequenceID, tx.ID, newState)
}

// SetSequenceTxErrorState
func (s *serviceImpl) SetSequenceTxErrorState(tx *SequenceTx, errorMessage string) error {
	return s.repo.SetSequenceTxErrorState(tx.SequenceID, tx.ID, errorMessage)
}

// SetSequenceTxsStateAfter
func (s *serviceImpl) SetSequenceTxsStateAfter(sequenceID int64, txID string, newState TransactionState) error {
	return s.repo.SetSequenceTxsStateAfter(sequenceID, txID, newState)
}
