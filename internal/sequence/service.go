package sequence

import (
	"time"

	"github.com/go-pg/pg"
)

// Sequence represents sequence type with json marshaling description
type Sequence struct {
	ID               int64 `json:"id"`
	BroadcastedCount int32 `json:"broadcastedCount"`
	TotalCount       int32 `json:"totalCount"`
	HasError         bool  `json:"hasError"`
	IsProcessing     bool  `json:"-"` // skip in json
	CreatedAt        int64 `json:"createdAt"`
	UpdatedAt        int64 `json:"updatedAt"`
}

// SequenceTxs represents sequence transactions type
type SequenceTxs []string

// Service ...
type Service interface {
	GetSequenceByID(id int64) (*Sequence, error)
	GetSequenceTxsByID(id int64, after int32) (SequenceTxs, error)
	GetFreeSequenceIds() ([]int64, error)
	GetFrozenSequenceIds(frozenTimeout time.Duration) ([]int64, error)
	CreateSequence(txs []string) (int64, error)
	SetSequenceErrorStateAndRelease(id int64) error
	UpdateSequenceProcessingState(id int64, isProcessing bool) error
	IncreaseSequenceBroadcastedCount(id int64) error
}

type serviceImpl struct {
	repo Repo
}

// NewService ...
func NewService(repo Repo) Service {
	return &serviceImpl{repo: repo}
}

// GetSequenceByID ...
func (s *serviceImpl) GetSequenceByID(id int64) (*Sequence, error) {
	seqDto, err := s.repo.GetSequenceByID(id)
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
		HasError:         seqDto.HasError,
		IsProcessing:     seqDto.IsProcessing,
		CreatedAt:        seqDto.CreatedAt.Unix() * 1000,
		UpdatedAt:        seqDto.UpdatedAt.Unix() * 1000,
	}
	return &seq, nil
}

func (s *serviceImpl) GetSequenceTxsByID(id int64, after int32) (SequenceTxs, error) {
	txs, err := s.repo.GetSequenceTxsByID(id, after)
	if err != nil {
		return nil, err
	}

	return SequenceTxs(txs), nil
}

// GetFreeSequenceIds ...
func (s *serviceImpl) GetFreeSequenceIds() ([]int64, error) {
	return s.repo.GetFreeSequenceIds()
}

// GetFrozenSequenceIds tries to get frozen sequence ids
// Frozen sequences - with is_processing=true, not totally broadcasted and not updated for frozenTimeout.
func (s *serviceImpl) GetFrozenSequenceIds(frozenTimeout time.Duration) ([]int64, error) {
	return s.repo.GetFrozenSequenceIds(frozenTimeout)
}

// CreateSequence ...
func (s *serviceImpl) CreateSequence(txs []string) (int64, error) {
	return s.repo.CreateSequence(txs)
}

// SetSequenceErrorStateAndRelease ...
func (s *serviceImpl) SetSequenceErrorStateAndRelease(id int64) error {
	return s.repo.SetSequenceErrorStateAndRelease(id)
}

// UpdateSequenceProcessingState ...
func (s *serviceImpl) UpdateSequenceProcessingState(id int64, isProcessing bool) error {
	return s.repo.UpdateSequenceProcessingState(id, isProcessing)
}

// IncreaseSequenceBroadcastedCount ...
func (s *serviceImpl) IncreaseSequenceBroadcastedCount(id int64) error {
	return s.repo.IncreaseSequenceBroadcastedCount(id)
}
