package sequence

import (
	"time"

	//
	"github.com/go-pg/pg/v9"
)

// SequenceModel ...
type SequenceModel struct {
	ID               int64
	BroadcastedCount int32
	TotalCount       int32
	HasError         bool
	IsProcessing     bool
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

// SequenceTxsModel ...
type SequenceTxsModel []string

// SequenceWithTxsModel ...
type SequenceWithTxsModel struct {
	SequenceModel
	Txs SequenceTxsModel
}

// Repo ...
type Repo interface {
	GetSequenceByID(id int64) (*SequenceModel, error)
	GetSequenceTxsByID(id int64, after int32) (SequenceTxsModel, error)
	GetFreeSequenceIds() ([]int64, error)
	GetFrozenSequenceIds(frozenTimeout time.Duration) ([]int64, error)
	CreateSequence(txs []string) (int64, error)
	SetSequenceErrorStateAndRelease(id int64) error
	UpdateSequenceProcessingState(id int64, isProcessing bool) error
	IncreaseSequenceBroadcastedCount(id int64) error
}

// PgConfig ...
type PgConfig struct {
	Host     string `env:"PGHOST,required"`
	Port     int    `env:"PGPORT" envDefault:"5432"`
	Database string `env:"PGDATABASE,required"`
	User     string `env:"PGUSER,required"`
	Password string `env:"PGPASSWORD,required"`
}

type repoImpl struct {
	Conn *pg.DB
}

// NewRepo ...
func NewRepo(db *pg.DB) Repo {
	return &repoImpl{Conn: db}
}

// GetSequenceByID ...
func (r *repoImpl) GetSequenceByID(id int64) (*SequenceModel, error) {
	var seq SequenceModel

	_, err := r.Conn.QueryOne(&seq, "select s.id, s.broadcasted_count, s.total_count, s.has_error, s.is_processing, s.created_at, s.updated_at from sequences s where id=?0", id)
	if err != nil {
		return nil, err
	}

	return &seq, nil
}

// GetSequenceTxsByID ...
func (r *repoImpl) GetSequenceTxsByID(id int64, after int32) (SequenceTxsModel, error) {
	var seqTxs SequenceTxsModel

	_, err := r.Conn.Query(&seqTxs, "select tx from sequences_txs where sequence_id=?0 and position_in_sequence>=?1", id, after)
	if err != nil {
		return nil, err
	}

	return seqTxs, nil
}

// GetFreeSequenceIds ...
func (r *repoImpl) GetFreeSequenceIds() ([]int64, error) {
	ids := []int64{}

	_, err := r.Conn.Query(&ids, "select s.id from sequences s where broadcasted_count < total_count and has_error=false and is_processing=?0 order by id asc", false)
	if err != nil {
		return nil, err
	}

	return ids, nil
}

// GetFrozenSequenceIds tries to get frozen sequence ids
// Frozen sequences - with is_processing=true, not totally broadcasted and not updated for frozenTimeout.
func (r *repoImpl) GetFrozenSequenceIds(frozenTimeout time.Duration) ([]int64, error) {
	ids := []int64{}

	_, err := r.Conn.Query(&ids, "select s.id from sequences s where broadcasted_count < total_count and is_processing=true and has_error=false and updated_at < NOW() - interval '?0 seconds' order by id asc", frozenTimeout.Seconds())
	if err != nil {
		return nil, err
	}

	return ids, nil
}

// CreateSequence ...
func (r *repoImpl) CreateSequence(txs []string) (int64, error) {
	var sequenceID int64

	err := r.Conn.RunInTransaction(func(t *pg.Tx) error {
		_, err := t.QueryOne(&sequenceID, "insert into sequences(total_count) values(?0) returning id;", len(txs))
		if err != nil {
			return err

		}

		for i, tx := range txs {
			t.Exec("insert into sequences_txs(sequence_id, position_in_sequence, tx) values(?0, ?1, ?2);", sequenceID, i, tx)
		}

		return nil
	})

	if err != nil {
		return 0, err
	}

	return sequenceID, nil
}

// SetSequenceErrorState ...
func (r *repoImpl) SetSequenceErrorStateAndRelease(id int64) error {
	_, err := r.Conn.Exec("update sequences set is_processing=false, has_error=true, updated_at=NOW() where id=?0", id)
	return err
}

// UpdateSequenceProcessingState ...
func (r *repoImpl) UpdateSequenceProcessingState(id int64, isProcessing bool) error {
	_, err := r.Conn.Exec("update sequences set is_processing=?0, updated_at=NOW() where id=?1", isProcessing, id)
	return err
}

// IncreaseSequenceBroadcastedCount ...
func (r *repoImpl) IncreaseSequenceBroadcastedCount(id int64) error {
	_, err := r.Conn.Exec("update sequences set broadcasted_count=broadcasted_count + 1, updated_at=NOW() where id=?0", id)
	return err
}
