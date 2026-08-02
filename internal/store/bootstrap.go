package store

import (
	"fmt"
	"log"
	"os"
	"strings"

	"golang.org/x/crypto/bcrypt"
)

const (
	infraBootstrapUsernameKey = "BOOTSTRAP_ADMIN_USERNAME"
	infraBootstrapHashKey     = "BOOTSTRAP_ADMIN_PASSWORD_HASH"
)

// BootstrapAdminCreds reads local admin identity from env (Vault-injected).
// Prefers BOOTSTRAP_ADMIN_* (ASPM-style) then ADMIN_*.
func BootstrapAdminCreds() (username, password, email string) {
	username = firstEnv(
		os.Getenv("BOOTSTRAP_ADMIN_USERNAME"),
		os.Getenv("ADMIN_USERNAME"),
	)
	password = firstEnv(
		os.Getenv("BOOTSTRAP_ADMIN_PASSWORD"),
		os.Getenv("ADMIN_PASSWORD"),
	)
	email = firstEnv(
		os.Getenv("BOOTSTRAP_ADMIN_EMAIL"),
		os.Getenv("ADMIN_EMAIL"),
		"admin@crnet.local",
	)
	return strings.TrimSpace(username), password, strings.TrimSpace(email)
}

func firstEnv(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

// BootstrapAdminResult describes EnsureBootstrapAdmin work.
type BootstrapAdminResult struct {
	Created         bool
	PasswordUpdated bool
	RoleUpdated     bool
}

// EnsureBootstrapAdmin creates/updates the Vault-configured local admin, same
// idea as ASPM EnsureBootstrapAdmin: password and admin role are re-applied
// whenever config (Vault env) differs from stored state.
func (s *Store) EnsureBootstrapAdmin() (BootstrapAdminResult, error) {
	username, password, email := BootstrapAdminCreds()
	var result BootstrapAdminResult
	if username == "" || password == "" {
		return result, nil
	}

	displayName := "Local Administrator"
	if !s.postgresEnabled {
		return result, nil
	}

	existing := s.getUserPermissionFromDB(username)
	if existing == nil {
		perm := &UserPermission{
			Username:    username,
			DisplayName: displayName,
			Email:       email,
			Role:        "admin",
			Namespaces:  []string{"*"},
			Template:    "bootstrap",
		}
		if err := s.SaveUserPermission(perm); err != nil {
			return result, fmt.Errorf("bootstrap admin permission: %w", err)
		}
		result.Created = true
	} else {
		needsRole := existing.Role != "admin"
		needsNS := !hasWildcardNamespace(existing.Namespaces)
		if needsRole || needsNS || existing.Email != email {
			existing.Role = "admin"
			existing.Namespaces = []string{"*"}
			existing.DisplayName = displayName
			existing.Email = email
			existing.Template = "bootstrap"
			if err := s.SaveUserPermission(existing); err != nil {
				return result, fmt.Errorf("bootstrap admin permission sync: %w", err)
			}
			result.RoleUpdated = needsRole || needsNS
		}
	}

	updated, err := s.syncBootstrapPasswordHash(username, password)
	if err != nil {
		return result, err
	}
	result.PasswordUpdated = updated
	return result, nil
}

func hasWildcardNamespace(ns []string) bool {
	for _, n := range ns {
		if n == "*" {
			return true
		}
	}
	return false
}

func (s *Store) syncBootstrapPasswordHash(username, password string) (bool, error) {
	storedUser := s.GetInfraConfig(infraBootstrapUsernameKey, "")
	storedHash := s.GetInfraConfig(infraBootstrapHashKey, "")

	needUpdate := storedUser != username || storedHash == ""
	if !needUpdate {
		if err := bcrypt.CompareHashAndPassword([]byte(storedHash), []byte(password)); err != nil {
			needUpdate = true
		}
	}
	if !needUpdate {
		return false, nil
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return false, fmt.Errorf("bootstrap admin hash: %w", err)
	}
	if err := s.SaveInfraConfig(infraBootstrapUsernameKey, username); err != nil {
		return false, fmt.Errorf("bootstrap admin username save: %w", err)
	}
	if err := s.SaveInfraConfig(infraBootstrapHashKey, string(hash)); err != nil {
		return false, fmt.Errorf("bootstrap admin password save: %w", err)
	}
	log.Printf("[auth] synced bootstrap admin password from Vault/config for user %q", username)
	return true, nil
}

// VerifyBootstrapAdminPassword checks the local admin password.
// Prefers the synced Postgres hash; falls back to env when Postgres is down.
func (s *Store) VerifyBootstrapAdminPassword(username, password string) bool {
	wantUser, wantPass, _ := BootstrapAdminCreds()
	if wantUser == "" || wantPass == "" {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(username), wantUser) {
		return false
	}

	if s.postgresEnabled {
		storedUser := s.GetInfraConfig(infraBootstrapUsernameKey, "")
		storedHash := s.GetInfraConfig(infraBootstrapHashKey, "")
		if storedHash != "" && (storedUser == "" || strings.EqualFold(storedUser, username)) {
			if err := bcrypt.CompareHashAndPassword([]byte(storedHash), []byte(password)); err == nil {
				return true
			}
			// Vault/config may have changed since last sync — accept env and re-sync.
			if password == wantPass {
				_, _ = s.syncBootstrapPasswordHash(wantUser, wantPass)
				return true
			}
			return false
		}
	}

	return password == wantPass
}
