package raft

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"sync"

	_ "github.com/mattn/go-sqlite3"
)

type Storage interface {
	Set(key string, value []byte) error
	Get(key string) ([]byte, bool, error)
	HasData() (bool, error)
}

type Database struct {
	mu sync.Mutex
	db *sql.DB
}

func NewDatabase(clusterID, nodeID uint64) (*Database, error) {
	// Create the db directory if it doesn't exist
	clusterPath := fmt.Sprintf("db/cluster_%d", clusterID)
	if err := os.MkdirAll(clusterPath, os.ModePerm); err != nil {
		return nil, err
	}

	// Create a database file for each node in the cluster folder
	dbPath := fmt.Sprintf("%s/raft_node_%d.db", clusterPath, nodeID)
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, err
	}

	_, err = db.Exec("CREATE TABLE IF NOT EXISTS kv (key NUMBER PRIMARY KEY, value BLOB)")
	if err != nil {
		return nil, err
	}

	return &Database{db: db}, nil
}

func (db *Database) Get(key string) ([]byte, bool, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	var value []byte
	err := db.db.QueryRow("SELECT value FROM kv WHERE key = ?", key).Scan(&value)
	if err == sql.ErrNoRows {
		return nil, false, nil
	} else if err != nil {
		return nil, false, err
	}
	return value, true, nil
}

func (db *Database) Set(key string, value []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if key == "15" {
		fmt.Println("key: ", key, strings.TrimSpace(string(value)))
	}

	if strings.TrimSpace(string(value)) == "delete" {
		fmt.Println("deleting key: ", key, string(value))
		_, err := db.db.Exec("DELETE FROM kv WHERE key = ?", key)
		return err
	}

	_, err := db.db.Exec("INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?)", key, value)
	return err
}

func (db *Database) HasData() (bool) {
	db.mu.Lock()
	defer db.mu.Unlock()

	var count int
	err := db.db.QueryRow("SELECT COUNT(*) FROM kv").Scan(&count)
	if err != nil {
		return false
	}
	return count > 0
}
