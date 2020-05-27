package sequence

import (
	"encoding/json"
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

// MarshalJSON override default serializaion of SequenceState type
func (st SequenceState) MarshalJSON() ([]byte, error) {
	var s string
	switch st {
	case SequenceStatePending:
		s = "pending"
	case SequenceStateProcessing:
		s = "processing"
	case SequenceStateDone:
		s = "done"
	case SequenceStateError:
		s = "error"
	default:
		s = "pending"
	}

	return json.Marshal(s)
}

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

// MarshalJSON override default serializaion of TransactionState type
func (st TransactionState) MarshalJSON() ([]byte, error) {
	var s string
	switch st {
	case TransactionStatePending:
		s = "pending"
	case TransactionStateProcessing:
		s = "processing"
	case TransactionStateValidated:
		s = "validated"
	case TransactionStateUnconfirmed:
		s = "unconfirmed"
	case TransactionStateConfirmed:
		s = "confirmed"
	case TransactionStateError:
		s = "error"
	default:
		s = "pending"
	}

	return json.Marshal(s)
}

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
	Height             int32
	ErrorMessage       string
	PositionInSequence uint16
	Tx                 string
	CreatedAt          time.Time
	UpdatedAt          time.Time
}

type TxWithIDDto struct {
	ID string
	Tx string
}

// PgConfig represents application PostgreSQL config
type PgConfig struct {
	Host     string `env:"PGHOST,required"`
	Port     int    `env:"PGPORT" envDefault:"5432"`
	Database string `env:"PGDATABASE,required"`
	User     string `env:"PGUSER,required"`
	Password string `env:"PGPASSWORD,required"`
}

// Repo ...
type Repo interface {
	GetSequenceByID(sequenceID int64) (*SequenceModel, error)
	GetSequenceTxsByID(sequenceID int64) ([]*SequenceTxModel, error)
	GetHangingSequenceIds(ttl time.Duration) ([]int64, error)
	CreateSequence(txs []TxWithIDDto) (int64, error)
	SetSequenceState(sequenceID int64, newState SequenceState) error
	SetSequenceErrorState(sequenceID int64, err error) error
	SetSequenceTxState(sequenceID int64, txID string, newState TransactionState) error
	SetSequenceTxConfirmedState(sequenceID int64, txID string, height int32) error
	SetSequenceTxErrorState(sequenceID int64, txID string, errorMessage string) error
	SetSequenceTxsStateAfter(sequenceID int64, txID string, newState TransactionState) error
}

type repoImpl struct {
	Conn *pg.DB
}

// NewRepo returns instance of Repo interface implementation
func NewRepo(db *pg.DB) Repo {
	return &repoImpl{Conn: db}
}

// GetSequenceByID ...
func (r *repoImpl) GetSequenceByID(sequenceID int64) (*SequenceModel, error) {
	var seq SequenceModel

	_, err := r.Conn.QueryOne(&seq, "select id, state, error_message, created_at, updated_at, coalesce((select count(*) from sequences_txs where sequence_id=?0 and state=?1), 0) as broadcasted_count, (select count(*) from sequences_txs where sequence_id=?0) as total_count from sequences where id=?0", sequenceID, TransactionStateConfirmed)
	if err != nil {
		return nil, err
	}

	return &seq, nil
}

// GetSequenceTxsByID returns sequence txs by sequenceID
func (r *repoImpl) GetSequenceTxsByID(sequenceID int64) ([]*SequenceTxModel, error) {
	var seqTxs []*SequenceTxModel

	_, err := r.Conn.Query(&seqTxs, "select tx_id as id, sequence_id, state, height, error_message, position_in_sequence, tx, created_at, updated_at from sequences_txs where sequence_id=?0", sequenceID)
	if err != nil {
		return nil, err
	}

	return seqTxs, nil
}

// GetHangingSequenceIds tries to get hanging sequence ids
// Hanging sequences - with state=processing and not updated for ttl.
func (r *repoImpl) GetHangingSequenceIds(ttl time.Duration) ([]int64, error) {
	ids := []int64{}

	_, err := r.Conn.Query(&ids, "select s.id from sequences s where state=?0 and updated_at < NOW() - interval '?1 seconds' order by id asc", SequenceStateProcessing, ttl.Seconds())
	if err != nil {
		return nil, err
	}

	return ids, nil
}

// CreateSequence ...
func (r *repoImpl) CreateSequence(txs []TxWithIDDto) (int64, error) {
	var sequenceID int64

	err := r.Conn.RunInTransaction(func(tr *pg.Tx) error {
		_, err := tr.QueryOne(&sequenceID, "insert into sequences(state) values(?0) returning id;", SequenceStatePending)
		if err != nil {
			return err

		}

		for i, t := range txs {
			_, err := tr.Exec("insert into sequences_txs(sequence_id, tx_id, state, position_in_sequence, tx) values(?0, ?1, ?2, ?3, ?4);", sequenceID, t.ID, TransactionStatePending, i, t.Tx)
			if err != nil {
				return err
			}
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
	_, err := r.Conn.Exec("update sequences set state=?1, updated_at=NOW() where id=?0", id, newState)
	return err
}

// SetSequenceErrorState ...
func (r *repoImpl) SetSequenceErrorState(id int64, e error) error {
	_, err := r.Conn.Exec("update sequences set state=?1, error_message=?2, updated_at=NOW() where id=?0", id, SequenceStateError, e.Error())
	return err
}

// SetSequenceTxState ...
func (r *repoImpl) SetSequenceTxState(sequenceID int64, txID string, newState TransactionState) error {
	_, err := r.Conn.Exec("update sequences_txs set state=?0, updated_at=NOW() where sequence_id=?1 and tx_id=?2", newState, sequenceID, txID)
	return err
}

// SetSequenceTxConfirmedState ...
func (r *repoImpl) SetSequenceTxConfirmedState(sequenceID int64, txID string, height int32) error {
	_, err := r.Conn.Exec("update sequences_txs set state=?0, height=?1, updated_at=NOW() where sequence_id=?2 and tx_id=?3", TransactionStateConfirmed, height, sequenceID, txID)
	return err
}

// SetSequenceTxErrorState ...
func (r *repoImpl) SetSequenceTxErrorState(sequenceID int64, txID string, errorMessage string) error {
	_, err := r.Conn.Exec("update sequences_txs set state=?0, error_message=?1, updated_at=NOW() where sequence_id=?2 and tx_id=?3", TransactionStateError, errorMessage, sequenceID, txID)
	return err
}

// SetSequenceTxsStateAfter ...
func (r *repoImpl) SetSequenceTxsStateAfter(sequenceID int64, txID string, newState TransactionState) error {
	_, err := r.Conn.Exec("update sequences_txs set state=?0, updated_at=NOW() where sequence_id=?1 and position_in_sequence>=(select position_in_sequence from sequences_txs where sequence_id=?1 and tx_id=?2)", newState, sequenceID, txID)
	return err
}
