package store

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/lib/pq"
)

// InitPostgres sets up PostgreSQL connection and runs automatic migrations
func (s *Store) InitPostgres(connStr string) error {
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return fmt.Errorf("failed to open postgres connection: %w", err)
	}

	// Ping database with retry loop to allow database to boot up in cluster
	var lastErr error
	for i := 0; i < 10; i++ {
		lastErr = db.Ping()
		if lastErr == nil {
			break
		}
		log.Printf("[store/postgres] waiting for database connection... error: %v", lastErr)
		time.Sleep(3 * time.Second)
	}
	if lastErr != nil {
		db.Close()
		return fmt.Errorf("postgres ping timeout: %w", lastErr)
	}

	// Create tables if they do not exist
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS disabled_namespaces (
			namespace VARCHAR(255) PRIMARY KEY,
			disabled BOOLEAN DEFAULT true,
			updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
		);
		CREATE TABLE IF NOT EXISTS configured_namespaces (
			namespace VARCHAR(255) PRIMARY KEY,
			configured BOOLEAN DEFAULT true,
			updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
		);
	`)
	if err != nil {
		db.Close()
		return fmt.Errorf("failed to execute migrations: %w", err)
	}

	s.db = db
	s.postgresEnabled = true
	log.Println("[store/postgres] connection initialized successfully")
	return nil
}
