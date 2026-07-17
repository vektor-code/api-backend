package store

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// UserPermission controls what an authenticated user can see.
// Namespaces ["*"] (or empty) means unrestricted visibility.
type UserPermission struct {
	Username    string    `json:"username"`
	DisplayName string    `json:"displayName"`
	Email       string    `json:"email"`
	Role        string    `json:"role"`       // "admin" | "viewer"
	Namespaces  []string  `json:"namespaces"` // allowed namespaces; ["*"] = all
	Template    string    `json:"template"`   // template this user was created from (informational)
	LastLogin   time.Time `json:"lastLogin"`
	UpdatedAt   time.Time `json:"updatedAt"`
}

// PermissionTemplate is a reusable permission preset (SonarQube-style).
// The template marked IsDefault is applied to LDAP users on first login.
type PermissionTemplate struct {
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Role        string   `json:"role"`
	Namespaces  []string `json:"namespaces"`
	IsDefault   bool     `json:"isDefault"`
}

var (
	permCacheMu   sync.RWMutex
	permCache     map[string]*UserPermission
	permCacheTime time.Time
)

const permCacheTTL = 15 * time.Second

// initPermissionTables creates the permission tables (called from InitPostgres).
func (s *Store) initPermissionTables() error {
	_, err := s.db.Exec(`
		CREATE TABLE IF NOT EXISTS user_permissions (
			username VARCHAR(255) PRIMARY KEY,
			display_name TEXT DEFAULT '',
			email TEXT DEFAULT '',
			role VARCHAR(32) DEFAULT 'viewer',
			namespaces TEXT DEFAULT '["*"]',
			template VARCHAR(255) DEFAULT '',
			last_login TIMESTAMP WITH TIME ZONE,
			updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
		);
		CREATE TABLE IF NOT EXISTS permission_templates (
			name VARCHAR(255) PRIMARY KEY,
			description TEXT DEFAULT '',
			role VARCHAR(32) DEFAULT 'viewer',
			namespaces TEXT DEFAULT '["*"]',
			is_default BOOLEAN DEFAULT false,
			updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
		);
	`)
	return err
}

func marshalNamespaces(ns []string) string {
	// An empty list is meaningful: it means NO namespace access. Never coerce
	// it to "*" — that would silently grant full visibility.
	if ns == nil {
		ns = []string{}
	}
	data, err := json.Marshal(ns)
	if err != nil {
		return `[]`
	}
	return string(data)
}

func unmarshalNamespaces(raw string) []string {
	var ns []string
	if err := json.Unmarshal([]byte(raw), &ns); err != nil {
		// Unparseable legacy value: fall back to unrestricted rather than
		// accidentally locking an existing user out.
		return []string{"*"}
	}
	if ns == nil {
		ns = []string{}
	}
	return ns
}

// ListUserPermissions returns all known users with their permissions.
func (s *Store) ListUserPermissions() ([]*UserPermission, error) {
	if !s.postgresEnabled {
		return []*UserPermission{}, nil
	}
	rows, err := s.db.Query(`SELECT username, display_name, email, role, namespaces, template,
		COALESCE(last_login, to_timestamp(0)), updated_at FROM user_permissions ORDER BY username`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var list []*UserPermission
	for rows.Next() {
		var u UserPermission
		var nsRaw string
		if err := rows.Scan(&u.Username, &u.DisplayName, &u.Email, &u.Role, &nsRaw, &u.Template, &u.LastLogin, &u.UpdatedAt); err != nil {
			continue
		}
		u.Namespaces = unmarshalNamespaces(nsRaw)
		list = append(list, &u)
	}
	return list, nil
}

// SaveUserPermission upserts a user's role and namespace visibility.
func (s *Store) SaveUserPermission(u *UserPermission) error {
	if !s.postgresEnabled {
		return fmt.Errorf("postgres is not enabled")
	}
	if u.Role != "admin" {
		u.Role = "viewer"
	}
	_, err := s.db.Exec(`
		INSERT INTO user_permissions (username, display_name, email, role, namespaces, template, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, CURRENT_TIMESTAMP)
		ON CONFLICT (username) DO UPDATE SET
			role = $4, namespaces = $5, template = $6, updated_at = CURRENT_TIMESTAMP,
			display_name = CASE WHEN $2 != '' THEN $2 ELSE user_permissions.display_name END,
			email = CASE WHEN $3 != '' THEN $3 ELSE user_permissions.email END
	`, u.Username, u.DisplayName, u.Email, u.Role, marshalNamespaces(u.Namespaces), u.Template)
	s.invalidatePermCache()
	return err
}

// DeleteUserPermission removes a user's permission record (falls back to defaults on next login).
func (s *Store) DeleteUserPermission(username string) error {
	if !s.postgresEnabled {
		return fmt.Errorf("postgres is not enabled")
	}
	_, err := s.db.Exec(`DELETE FROM user_permissions WHERE username = $1`, username)
	s.invalidatePermCache()
	return err
}

// RecordUserLogin is called on every successful LDAP login. New users get the
// default permission template when one exists; otherwise they start with NO
// namespace access (approval-based model — an admin must grant visibility).
// Existing users just refresh metadata. Returns the effective permission.
func (s *Store) RecordUserLogin(username, displayName, email string, ldapAdmin bool) *UserPermission {
	fallback := &UserPermission{
		Username:    username,
		DisplayName: displayName,
		Email:       email,
		Role:        "viewer",
		Namespaces:  []string{}, // no access until granted
	}
	if ldapAdmin {
		fallback.Role = "admin"
		fallback.Namespaces = []string{"*"}
	}
	if !s.postgresEnabled {
		// Without a database there is no permission management — stay open.
		fallback.Namespaces = []string{"*"}
		return fallback
	}

	existing := s.getUserPermissionFromDB(username)
	if existing == nil {
		// First login: apply the default template when one exists.
		perm := fallback
		if tpl := s.getDefaultTemplate(); tpl != nil {
			perm = &UserPermission{
				Username:    username,
				DisplayName: displayName,
				Email:       email,
				Role:        tpl.Role,
				Namespaces:  tpl.Namespaces,
				Template:    tpl.Name,
			}
			// LDAP admin group always wins over a template role.
			if ldapAdmin {
				perm.Role = "admin"
			}
		}
		_, _ = s.db.Exec(`
			INSERT INTO user_permissions (username, display_name, email, role, namespaces, template, last_login, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
			ON CONFLICT (username) DO NOTHING
		`, username, displayName, email, perm.Role, marshalNamespaces(perm.Namespaces), perm.Template)
		s.invalidatePermCache()
		return perm
	}

	_, _ = s.db.Exec(`UPDATE user_permissions SET last_login = CURRENT_TIMESTAMP,
		display_name = CASE WHEN $2 != '' THEN $2 ELSE display_name END,
		email = CASE WHEN $3 != '' THEN $3 ELSE email END
		WHERE username = $1`, username, displayName, email)

	// LDAP admin group membership grants admin regardless of stored role.
	if ldapAdmin && existing.Role != "admin" {
		existing.Role = "admin"
	}
	return existing
}

func (s *Store) getUserPermissionFromDB(username string) *UserPermission {
	var u UserPermission
	var nsRaw string
	err := s.db.QueryRow(`SELECT username, display_name, email, role, namespaces, template,
		COALESCE(last_login, to_timestamp(0)), updated_at FROM user_permissions WHERE username = $1`, username).
		Scan(&u.Username, &u.DisplayName, &u.Email, &u.Role, &nsRaw, &u.Template, &u.LastLogin, &u.UpdatedAt)
	if err != nil {
		return nil
	}
	u.Namespaces = unmarshalNamespaces(nsRaw)
	return &u
}

// GetCachedUserPermission returns the permission for a user from a short-TTL
// cache, so per-request enforcement never hammers Postgres. Returns nil when
// the user has no record (= unrestricted, backward compatible).
func (s *Store) GetCachedUserPermission(username string) *UserPermission {
	if !s.postgresEnabled {
		return nil
	}

	permCacheMu.RLock()
	fresh := time.Since(permCacheTime) < permCacheTTL && permCache != nil
	var cached *UserPermission
	if fresh {
		cached = permCache[username]
	}
	permCacheMu.RUnlock()
	if fresh {
		return cached
	}

	list, err := s.ListUserPermissions()
	if err != nil {
		return nil
	}
	m := make(map[string]*UserPermission, len(list))
	for _, u := range list {
		m[u.Username] = u
	}
	permCacheMu.Lock()
	permCache = m
	permCacheTime = time.Now()
	permCacheMu.Unlock()
	return m[username]
}

func (s *Store) invalidatePermCache() {
	permCacheMu.Lock()
	permCacheTime = time.Time{}
	permCacheMu.Unlock()
}

// ListPermissionTemplates returns all templates.
func (s *Store) ListPermissionTemplates() ([]*PermissionTemplate, error) {
	if !s.postgresEnabled {
		return []*PermissionTemplate{}, nil
	}
	rows, err := s.db.Query(`SELECT name, description, role, namespaces, is_default FROM permission_templates ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var list []*PermissionTemplate
	for rows.Next() {
		var t PermissionTemplate
		var nsRaw string
		if err := rows.Scan(&t.Name, &t.Description, &t.Role, &nsRaw, &t.IsDefault); err != nil {
			continue
		}
		t.Namespaces = unmarshalNamespaces(nsRaw)
		list = append(list, &t)
	}
	return list, nil
}

func (s *Store) getDefaultTemplate() *PermissionTemplate {
	var t PermissionTemplate
	var nsRaw string
	err := s.db.QueryRow(`SELECT name, description, role, namespaces, is_default FROM permission_templates WHERE is_default = true LIMIT 1`).
		Scan(&t.Name, &t.Description, &t.Role, &nsRaw, &t.IsDefault)
	if err != nil {
		return nil
	}
	t.Namespaces = unmarshalNamespaces(nsRaw)
	return &t
}

// SavePermissionTemplate upserts a template; when marked default it unsets
// the default flag on every other template.
func (s *Store) SavePermissionTemplate(t *PermissionTemplate) error {
	if !s.postgresEnabled {
		return fmt.Errorf("postgres is not enabled")
	}
	if t.Role != "admin" {
		t.Role = "viewer"
	}
	if t.IsDefault {
		_, _ = s.db.Exec(`UPDATE permission_templates SET is_default = false WHERE name != $1`, t.Name)
	}
	_, err := s.db.Exec(`
		INSERT INTO permission_templates (name, description, role, namespaces, is_default, updated_at)
		VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP)
		ON CONFLICT (name) DO UPDATE SET description = $2, role = $3, namespaces = $4, is_default = $5, updated_at = CURRENT_TIMESTAMP
	`, t.Name, t.Description, t.Role, marshalNamespaces(t.Namespaces), t.IsDefault)
	return err
}

// DeletePermissionTemplate removes a template (users keep their materialized permissions).
func (s *Store) DeletePermissionTemplate(name string) error {
	if !s.postgresEnabled {
		return fmt.Errorf("postgres is not enabled")
	}
	_, err := s.db.Exec(`DELETE FROM permission_templates WHERE name = $1`, name)
	return err
}
