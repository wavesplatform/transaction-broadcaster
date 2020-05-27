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
	ErrorMessage     string        `json:"errorMessage,omitempty"`
	CreatedAt        time.Time     `json:"createdAt"`
	UpdatedAt        time.Time     `json:"updatedAt"`
}

// SequenceTx represents sequence transactions type
type SequenceTx struct {
	ID                 string           `json:"id"`
	SequenceID         int64            `json:"-"`
	State              TransactionState `json:"state"`
	Height             int32            `json:"height"`
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
	SetSequenceStateByID(sequenceID int64, newState SequenceState) error
	SetSequenceErrorStateByID(sequenceID int64, err error) error
	SetSequenceTxState(tx *SequenceTx, newState TransactionState) error
	SetSequenceTxConfirmedState(tx *SequenceTx, height int32) error
	SetSequenceTxErrorState(tx *SequenceTx, errorMessage string) error
	SetSequenceTxsStateAfter(sequenceID int64, txID string, newState TransactionState) error
}

type serviceImpl struct {
	repo Repo
}

// NewService returns instance of Service interface implementation
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
			Height:             txDto.Height,
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

// SetSequenceStateByID
func (s *serviceImpl) SetSequenceStateByID(sequenceID int64, newState SequenceState) error {
	return s.repo.SetSequenceState(sequenceID, newState)
}

// SetSequenceErrorState ...
func (s *serviceImpl) SetSequenceErrorStateByID(sequenceID int64, err error) error {
	return s.repo.SetSequenceErrorState(sequenceID, err)
}

// SetSequenceTxState
func (s *serviceImpl) SetSequenceTxState(tx *SequenceTx, newState TransactionState) error {
	return s.repo.SetSequenceTxState(tx.SequenceID, tx.ID, newState)
}

// SetSequenceTxConfirmedState
func (s *serviceImpl) SetSequenceTxConfirmedState(tx *SequenceTx, height int32) error {
	return s.repo.SetSequenceTxConfirmedState(tx.SequenceID, tx.ID, height)
}

// SetSequenceTxErrorState
func (s *serviceImpl) SetSequenceTxErrorState(tx *SequenceTx, errorMessage string) error {
	return s.repo.SetSequenceTxErrorState(tx.SequenceID, tx.ID, errorMessage)
}

// SetSequenceTxsStateAfter
func (s *serviceImpl) SetSequenceTxsStateAfter(sequenceID int64, txID string, newState TransactionState) error {
	return s.repo.SetSequenceTxsStateAfter(sequenceID, txID, newState)
}
