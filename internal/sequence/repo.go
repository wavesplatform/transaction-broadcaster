package sequence

import (
	"time"

	//
	"github.com/go-pg/pg/v9"
)

// SequenceState type represents type of sequence state
type SequenceState uint8

// Enum of SequenceState
const (
	SequenceStatePending SequenceState = iota
	SequenceStateProcessing
	SequenceStateDone
	SequenceStateError
)

// TransactionState type represents type of transaction state
type TransactionState uint8

// Enum for TransactionState
const (
	TransactionStatePending TransactionState = iota
	TransactionStateProcessing
	TransactionStateValidated
	TransactionStateUnconfirmed
	TransactionStateConfirmed
	TransactionStateError
)

// SequenceModel ...
type SequenceModel struct {
	ID               int64
	BroadcastedCount uint32
	TotalCount       uint32
	State            SequenceState
	ErrorMessage     string
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

// SequenceTxModel ...
type SequenceTxModel struct {
	ID                 string
	SequenceID         int64
	State              TransactionState
	ErrorMessage       string
	PositionInSequence uint16
	Tx                 string
	CreatedAt          time.Time
	UpdatedAt          time.Time
}

// SequenceWithTxsModel ...
type SequenceWithTxsModel struct {
	SequenceModel
	Txs []*SequenceTxModel
}

type TxWithIDDto struct {
	ID string
	Tx string
}

// Repo ...
type Repo interface {
	GetSequenceByID(sequenceID int64) (*SequenceModel, error)
	GetSequenceTxsByID(sequenceID int64) ([]*SequenceTxModel, error)
	GetHangingSequenceIds(ttl time.Duration) ([]int64, error)
	CreateSequence(txs []TxWithIDDto) (int64, error)
	SetSequenceState(sequenceID int64, newState SequenceState) error
	SetSequenceErrorState(sequenceID int64, err error) error
	IncreaseSequenceBroadcastedCount(sequenceID int64) error
	SetSequenceTxState(sequenceID int64, txID string, newState TransactionState) error
	SetSequenceTxErrorState(sequenceID int64, txID string, errorMessage string) error
	SetSequenceTxsStateAfter(sequenceID int64, txID string, newState TransactionState) error
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
func (r *repoImpl) GetSequenceByID(sequenceID int64) (*SequenceModel, error) {
	var seq SequenceModel

	_, err := r.Conn.QueryOne(&seq, "select s.id, s.broadcasted_count, s.total_count, s.has_error, s.is_processing, s.created_at, s.updated_at from sequences s where id=?0", sequenceID)
	if err != nil {
		return nil, err
	}

	return &seq, nil
}

// GetSequenceTxsByID returns sequence txs by sequenceID
func (r *repoImpl) GetSequenceTxsByID(sequenceID int64) ([]*SequenceTxModel, error) {
	var seqTxs []*SequenceTxModel

	_, err := r.Conn.Query(&seqTxs, "select tx_id as id, sequence_id, state, error_message, position_in_sequence, tx, created_at, updated_at from sequences_txs where sequence_id=?0", sequenceID)
	if err != nil {
		return nil, err
	}

	return seqTxs, nil
}

// GetHangingSequenceIds tries to get hanging sequence ids
// Hanging sequences - with is_processing=true, not totally broadcasted and not updated for ttl.
func (r *repoImpl) GetHangingSequenceIds(ttl time.Duration) ([]int64, error) {
	ids := []int64{}

	_, err := r.Conn.Query(&ids, "select s.id from sequences s where broadcasted_count < total_count and state=?0 and updated_at < NOW() - interval '?1 seconds' order by id asc", SequenceStateProcessing, ttl.Seconds())
	if err != nil {
		return nil, err
	}

	return ids, nil
}

// CreateSequence ...
func (r *repoImpl) CreateSequence(txs []TxWithIDDto) (int64, error) {
	var sequenceID int64

	err := r.Conn.RunInTransaction(func(tr *pg.Tx) error {
		_, err := tr.QueryOne(&sequenceID, "insert into sequences(total_count) values(?0) returning id;", len(txs))
		if err != nil {
			return err

		}

		for i, t := range txs {
			tr.Exec("insert into sequences_txs(sequence_id, tx_id, position_in_sequence, tx) values(?0, ?1, ?2, ?3);", sequenceID, t.ID, i, t.Tx)
		}

		return nil
	})

	if err != nil {
		return 0, err
	}

	return sequenceID, nil
}

// SetSequenceState ...
func (r *repoImpl) SetSequenceState(id int64, newState SequenceState) error {
	_, err := r.Conn.Exec("update sequences set state=?1 updated_at=NOW() where id=?0", id, newState)
	return err
}

// SetSequenceErrorState ...
func (r *repoImpl) SetSequenceErrorState(id int64, e error) error {
	_, err := r.Conn.Exec("update sequences set state=?1, error_message=?2 updated_at=NOW() where id=?0", id, SequenceStateError, e.Error())
	return err
}

// IncreaseSequenceBroadcastedCount ...
func (r *repoImpl) IncreaseSequenceBroadcastedCount(id int64) error {
	_, err := r.Conn.Exec("update sequences set broadcasted_count=broadcasted_count + 1, updated_at=NOW() where id=?0", id)
	return err
}

// SetSequenceTxState ...
func (r *repoImpl) SetSequenceTxState(sequenceID int64, txID string, newState TransactionState) error {
	_, err := r.Conn.Exec("update sequences_txs set state=?0, updated_at=NOW() where sequence_id=?1 and tx_id=?2", newState, sequenceID, txID)
	return err
}

// SetSequenceTxErrorState ...
func (r *repoImpl) SetSequenceTxErrorState(sequenceID int64, txID string, errorMessage string) error {
	_, err := r.Conn.Exec("update sequences_txs set state=?0, error_message=?1, updated_at=NOW() where sequence_id=?2 and tx_id=?3", TransactionStateError, errorMessage, sequenceID, txID)
	return err
}

// SetSequenceTxsStateAfter ...
func (r *repoImpl) SetSequenceTxsStateAfter(sequenceID int64, txID string, newState TransactionState) error {
	_, err := r.Conn.Exec("update sequences_txs set state=?0, updated_at=NOW() where sequence_id=?1 and position_in_sequence >= (select position_in_sequence from sequences_txs where sequence_id=?1 and tx_id=?2)", newState, sequenceID, txID)
	return err
}
