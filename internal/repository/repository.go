package repository

import (
	"encoding/json"
	"time"

	//
	"github.com/go-pg/pg/v9"
)

// PgConfig represents application PostgreSQL config
type PgConfig struct {
	Host     string `env:"PGHOST,required"`
	Port     int    `env:"PGPORT" envDefault:"5432"`
	Database string `env:"PGDATABASE,required"`
	User     string `env:"PGUSER,required"`
	Password string `env:"PGPASSWORD,required"`
}

type ErrorInfo struct {
	ErrorMessage string `json:"message,omitempty"`
	ErrorCode    int16  `json:"code,omitempty"`
}

// Sequence represents sequence type with json marshaling description
type Sequence struct {
	ID               int64  `json:"id"`
	BroadcastedCount uint32 `json:"broadcasted_count"`
	TotalCount       uint32 `json:"total_count"`
	State            State  `json:"state"`
	ErrorInfo        `json:"error"`
	CreatedAt        time.Time `json:"created_at"`
	UpdatedAt        time.Time `json:"updated_at"`
}

// MarshalJSON overrides default json serializer
// Its serializes time as unix timestamp
func (s *Sequence) MarshalJSON() ([]byte, error) {
	type JSONSequence Sequence
	var info *ErrorInfo

	if s.ErrorInfo == (ErrorInfo{}) {
		info = nil
	} else {
		info = &s.ErrorInfo
	}

	return json.Marshal(&struct {
		*JSONSequence
		ErrorInfo *ErrorInfo `json:"error"`
		CreatedAt int64      `json:"created_at"`
		UpdatedAt int64      `json:"updated_at"`
	}{
		JSONSequence: (*JSONSequence)(s),
		CreatedAt:    s.CreatedAt.Unix()*1000 + int64(s.CreatedAt.Nanosecond()/1000000),
		UpdatedAt:    s.UpdatedAt.Unix()*1000 + int64(s.UpdatedAt.Nanosecond()/1000000),
		ErrorInfo:    info,
	})
}

// SequenceTx represents sequence transaction type
type SequenceTx struct {
	ID                 string           `json:"id"`
	SequenceID         int64            `json:"-"`
	State              TransactionState `json:"state"`
	Height             int32            `json:"height"`
	ErrorMessage       string           `json:"error_message,omitempty"`
	PositionInSequence int16            `json:"position_in_sequence"`
	Tx                 string           `json:"tx"`
	CreatedAt          time.Time        `json:"created_at"`
	UpdatedAt          time.Time        `json:"updated_at"`
}

// MarshalJSON overrides default json serializer
// Its serializes time as unix timestamp
func (stx *SequenceTx) MarshalJSON() ([]byte, error) {
	type JSONSequenceTx SequenceTx
	return json.Marshal(&struct {
		*JSONSequenceTx
		CreatedAt int64 `json:"created_at"`
		UpdatedAt int64 `json:"updated_at"`
	}{
		JSONSequenceTx: (*JSONSequenceTx)(stx),
		CreatedAt:      stx.CreatedAt.Unix()*1000 + int64(stx.CreatedAt.Nanosecond()/1000000),
		UpdatedAt:      stx.UpdatedAt.Unix()*1000 + int64(stx.UpdatedAt.Nanosecond()/1000000),
	})
}

// TxWithIDDto ...
type TxWithIDDto struct {
	ID string
	Tx string
}

// State type represents type of sequence state
type State uint8

// Enum of State
const (
	StatePending State = iota
	StateProcessing
	StateDone
	StateError
)

// MarshalJSON override default serializaion of State type
func (st State) MarshalJSON() ([]byte, error) {
	var s string
	switch st {
	case StatePending:
		s = "pending"
	case StateProcessing:
		s = "processing"
	case StateDone:
		s = "done"
	case StateError:
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

// Repository ...
type Repository interface {
	GetSequenceByID(id int64) (*Sequence, error)
	GetSequenceTxsByID(sequenceID int64) ([]*SequenceTx, error)
	GetSequenceTx(sequenceID int64, positionInSequence int16) (*SequenceTx, error)
	GetNewSequenceIds() ([]int64, error)
	GetHangingSequenceIds(ttl time.Duration, excluding []int64) ([]int64, error)
	CreateSequence(txs []string) (int64, error)
	SetSequenceStateByID(sequenceID int64, newState State) error
	SetSequenceErrorStateByID(sequenceID int64, errorMessage string, errorCode uint16) error
	SetSequenceTxID(sequenceID int64, positionInSequence int16, txID string) error
	SetSequenceTxState(sequenceID int64, positionInSequence int16, newState TransactionState) error
	SetSequenceTxConfirmedState(sequenceID int64, positionInSequence int16, height int32) error
	SetSequenceTxsStateAfter(sequenceID int64, txID string, newState TransactionState) error
	SetSequenceTxErrorMessage(sequenceID int64, positionInSequence int16, errorMessage string) error
	ResetSequenceTxErrorMessage(sequenceID int64, positionInSequence int16) error
}

type repoImpl struct {
	Conn *pg.DB
}

// New returns instance of Repository interface implementation
func New(db *pg.DB) Repository {
	return &repoImpl{Conn: db}
}

func (r *repoImpl) GetSequenceByID(sequenceID int64) (*Sequence, error) {
	seq := Sequence{}

	_, err := r.Conn.QueryOne(&seq, "select id, state, error_message, error_code, created_at, updated_at, coalesce((select count(*) from sequences_txs where sequence_id=?0 and state=?1), 0) as broadcasted_count, (select count(*) from sequences_txs where sequence_id=?0) as total_count from sequences where id=?0", sequenceID, TransactionStateConfirmed)
	if err != nil {
		if err.Error() == pg.ErrNoRows.Error() {
			return nil, nil
		}
		return nil, err
	}

	return &seq, nil
}

func (r *repoImpl) GetSequenceTxsByID(sequenceID int64) ([]*SequenceTx, error) {
	var txs []*SequenceTx

	_, err := r.Conn.Query(&txs, "select tx_id as id, sequence_id, state, height, error_message, position_in_sequence, tx, created_at, updated_at from sequences_txs where sequence_id=?0 order by position_in_sequence asc", sequenceID)
	if err != nil {
		return nil, err
	}

	return txs, nil
}

func (r *repoImpl) GetSequenceTx(sequenceID int64, positionInSequence int16) (*SequenceTx, error) {
	tx := SequenceTx{}
	_, err := r.Conn.Query(&tx, "select tx_id as id, sequence_id, state, height, error_message, position_in_sequence, tx, created_at, updated_at from sequences_txs where sequence_id=?0 and position_in_sequence=?1", sequenceID, positionInSequence)
	if err != nil {
		return nil, err
	}

	return &tx, nil
}

// GetNewSequenceIds tries to new sequences ids
func (r *repoImpl) GetNewSequenceIds() ([]int64, error) {
	var ids []int64

	var err error
	_, err = r.Conn.Query(&ids, "select s.id from sequences s where s.state=?0 order by s.id asc", StatePending)

	if err != nil {
		return nil, err
	}

	return ids, nil
}

// GetHangingSequenceIds tries to get hanging sequence ids
// Hanging sequences - with state=processing and not updated for ttl.
func (r *repoImpl) GetHangingSequenceIds(ttl time.Duration, excluding []int64) ([]int64, error) {
	var ids []int64

	var err error
	if len(excluding) > 0 {
		_, err = r.Conn.Query(&ids, "select s.id from sequences s where s.state=?0 and s.updated_at < NOW() - interval '?1 seconds' and s.id not in (?2) order by s.id asc", StateProcessing, ttl.Seconds(), pg.In(excluding))
	} else {
		_, err = r.Conn.Query(&ids, "select s.id from sequences s where s.state=?0 and s.updated_at < NOW() - interval '?1 seconds' order by s.id asc", StateProcessing, ttl.Seconds())
	}

	if err != nil {
		return nil, err
	}

	return ids, nil
}

func (r *repoImpl) CreateSequence(txs []string) (int64, error) {
	sequenceID := int64(0)

	err := r.Conn.RunInTransaction(func(tr *pg.Tx) error {
		_, err := tr.QueryOne(&sequenceID, "insert into sequences(state) values(?0) returning id;", StatePending)
		if err != nil {
			return err

		}

		for i, tx := range txs {
			_, err := tr.Exec("insert into sequences_txs(sequence_id, state, position_in_sequence, tx) values(?0, ?1, ?2, ?3);", sequenceID, TransactionStatePending, i, tx)
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

func (r *repoImpl) SetSequenceStateByID(sequenceID int64, newState State) error {
	_, err := r.Conn.Exec("update sequences set state=?1, updated_at=NOW() where id=?0", sequenceID, newState)
	return err
}

func (r *repoImpl) SetSequenceErrorStateByID(sequenceID int64, errorMessage string, errorCode uint16) error {
	_, err := r.Conn.Exec("update sequences set state=?0, error_message=?1, error_code=?2, updated_at=NOW() where id=?3", StateError, errorMessage, errorCode, sequenceID)
	return err
}

func (r *repoImpl) SetSequenceTxID(sequenceID int64, positionInSequence int16, txID string) error {
	_, err := r.Conn.Exec("update sequences_txs set tx_id=?0, updated_at=NOW() where sequence_id=?1 and position_in_sequence=?2", txID, sequenceID, positionInSequence)
	return err
}

func (r *repoImpl) SetSequenceTxState(sequenceID int64, positionInSequence int16, newState TransactionState) error {
	_, err := r.Conn.Exec("update sequences_txs set state=?0, updated_at=NOW() where sequence_id=?1 and position_in_sequence=?2", newState, sequenceID, positionInSequence)
	return err
}

func (r *repoImpl) SetSequenceTxConfirmedState(sequenceID int64, positionInSequence int16, height int32) error {
	_, err := r.Conn.Exec("update sequences_txs set state=?0, height=?1, updated_at=NOW() where sequence_id=?2 and position_in_sequence=?3", TransactionStateConfirmed, height, sequenceID, positionInSequence)
	return err
}

func (r *repoImpl) SetSequenceTxsStateAfter(sequenceID int64, txID string, newState TransactionState) error {
	_, err := r.Conn.Exec("update sequences_txs set state=?0, updated_at=NOW() where sequence_id=?1 and position_in_sequence>=(select position_in_sequence from sequences_txs where sequence_id=?1 and tx_id=?2)", newState, sequenceID, txID)
	return err
}

func (r *repoImpl) SetSequenceTxErrorMessage(sequenceID int64, positionInSequence int16, errorMessage string) error {
	_, err := r.Conn.Exec("update sequences_txs set error_message=?0, updated_at=NOW() where sequence_id=?1 and position_in_sequence=?2", errorMessage, sequenceID, positionInSequence)
	return err
}

func (r *repoImpl) ResetSequenceTxErrorMessage(sequenceID int64, positionInSequence int16) error {
	_, err := r.Conn.Exec("update sequences_txs set error_message=null, updated_at=NOW() where sequence_id=?0 and position_in_sequence=?1", sequenceID, positionInSequence)
	return err
}
