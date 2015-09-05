package postgres

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/flynn/flynn/Godeps/_workspace/src/github.com/jackc/pgx"
	"github.com/flynn/flynn/appliance/postgresql/state"
	"github.com/flynn/flynn/discoverd/client"
	"github.com/flynn/flynn/pkg/dialer"
	"github.com/flynn/flynn/pkg/shutdown"
)

const (
	UniqueViolation = "23505"
	CheckViolation  = "23514"
)

type Conf struct {
	Service  string
	User     string
	Password string
	Database string
}

func Wait(conf *Conf, afterConn func(*pgx.Conn) error) *DB {
	// If no configuration is provided create the default Conf
	if conf == nil {
		conf = &Conf{
			Service:  os.Getenv("FLYNN_POSTGRES"),
			User:     os.Getenv("PGUSER"),
			Password: os.Getenv("PGPASSWORD"),
			Database: os.Getenv("PGDATABASE"),
		}
	}
	events := make(chan *discoverd.Event)
	stream, err := discoverd.NewService(conf.Service).Watch(events)
	if err != nil {
		shutdown.Fatal(err)
	}
	// wait for service meta that has sync or singleton primary
	for e := range events {
		if e.Kind&discoverd.EventKindServiceMeta == 0 || e.ServiceMeta == nil || len(e.ServiceMeta.Data) == 0 {
			continue
		}
		state := &state.State{}
		json.Unmarshal(e.ServiceMeta.Data, state)
		if state.Singleton || state.Sync != nil {
			break
		}
	}
	stream.Close()
	// TODO(titanous): handle discoverd disconnection

	db, err := Open(conf, afterConn)
	if err != nil {
		panic(err)
	}
	for {
		var readonly string
		// wait until read-write transactions are allowed
		if err := db.QueryRow("SHOW default_transaction_read_only").Scan(&readonly); err != nil || readonly == "on" {
			time.Sleep(100 * time.Millisecond)
			// TODO(titanous): add max wait here
			continue
		}
		return db
	}
}

func Open(conf *Conf, afterConn func(*pgx.Conn) error) (*DB, error) {
	connConfig := pgx.ConnConfig{
		Host:     fmt.Sprintf("leader.%s.discoverd", conf.Service),
		User:     conf.User,
		Database: conf.Database,
		Password: conf.Password,
		Dial:     dialer.Retry.Dial,
	}
	connPool, err := pgx.NewConnPool(pgx.ConnPoolConfig{
		ConnConfig:   connConfig,
		AfterConnect: afterConn,
	})
	db := &DB{connPool, conf}
	return db, err
}

func New(connPool *pgx.ConnPool, conf *Conf) *DB {
	&DB{connPool, conf}
}

type DB struct {
	*pgx.ConnPool
	conf *Conf
}

func IsUniquenessError(err error, constraint string) bool {
	if e, ok := err.(pgx.PgError); ok && e.Code == UniqueViolation {
		return constraint == "" || constraint == e.ConstraintName
	}
	return false
}

type Scanner interface {
	Scan(...interface{}) error
}

func (db *DB) Begin() (*DBTx, error) {
	tx, err := db.ConnPool.Begin()
	return &DBTx{tx}, err
}

func (db *DB) QueryRow(query string, args ...interface{}) Scanner {
	return db.QueryRow(query, args...)
}

func (db *DB) Exec(query string, args ...interface{}) error {
	_, err := db.ConnPool.Exec(query, args...)
	return err
}

type DBTx struct{ *pgx.Tx }

func (tx *DBTx) QueryRow(query string, args ...interface{}) Scanner {
	return tx.Tx.QueryRow(query, args...)
}

func (tx *DBTx) Exec(query string, args ...interface{}) error {
	_, err := tx.Tx.Exec(query, args...)
	return err
}
