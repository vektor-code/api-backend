package api

import (
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/go-ldap/ldap/v3"
	"github.com/gofiber/fiber/v2"
	"github.com/golang-jwt/jwt/v5"
	"github.com/kubetrace/api-backend/internal/store"
	"github.com/kubetrace/api-backend/internal/vaultenv"
)

type LoginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
	Mode     string `json:"mode"` // "local" or "ldap"
}

type LDAPUser struct {
	DN          string
	DisplayName string
	Email       string
	IsAdmin     bool
}

func getJWTSecret() ([]byte, error) {
	secret := os.Getenv("JWT_SECRET")
	if secret == "" {
		return nil, fmt.Errorf("JWT_SECRET is required")
	}
	return []byte(secret), nil
}

func accessTokenTTL() time.Duration {
	// Short-lived access tokens; clients renew via POST /api/auth/refresh.
	minutes := 60
	if raw := strings.TrimSpace(os.Getenv("JWT_TTL_MINUTES")); raw != "" {
		var parsed int
		if _, err := fmt.Sscanf(raw, "%d", &parsed); err == nil && parsed > 0 {
			minutes = parsed
		}
	}
	if minutes < 5 {
		minutes = 5
	}
	if minutes > 24*60 {
		minutes = 24 * 60
	}
	return time.Duration(minutes) * time.Minute
}

func issueAccessToken(username, displayName, email, role string) (string, error) {
	secret, err := getJWTSecret()
	if err != nil {
		return "", err
	}
	now := time.Now()
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub":   username,
		"name":  displayName,
		"email": email,
		"role":  role,
		"iat":   now.Unix(),
		"exp":   now.Add(accessTokenTTL()).Unix(),
	})
	return token.SignedString(secret)
}

func parseBearerToken(authHeader string) (string, error) {
	parts := strings.Split(authHeader, " ")
	if len(parts) != 2 || strings.ToLower(parts[0]) != "bearer" || parts[1] == "" {
		return "", fmt.Errorf("invalid authorization header format")
	}
	return parts[1], nil
}

func parseAccessToken(tokenString string, allowExpiredWithin time.Duration) (*jwt.Token, jwt.MapClaims, error) {
	secret, err := getJWTSecret()
	if err != nil {
		return nil, nil, err
	}

	var parser *jwt.Parser
	if allowExpiredWithin > 0 {
		parser = jwt.NewParser(
			jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}),
			jwt.WithoutClaimsValidation(),
		)
	} else {
		parser = jwt.NewParser(jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}))
	}

	claims := jwt.MapClaims{}
	token, err := parser.ParseWithClaims(tokenString, claims, func(t *jwt.Token) (interface{}, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
		}
		return secret, nil
	})
	if err != nil || token == nil || !token.Valid {
		return nil, nil, fmt.Errorf("invalid token")
	}

	sub, _ := claims["sub"].(string)
	if strings.TrimSpace(sub) == "" {
		return nil, nil, fmt.Errorf("invalid token subject")
	}

	if allowExpiredWithin > 0 {
		expRaw, ok := claims["exp"]
		if !ok {
			return nil, nil, fmt.Errorf("invalid token expiry")
		}
		var expUnix int64
		switch v := expRaw.(type) {
		case float64:
			expUnix = int64(v)
		case int64:
			expUnix = v
		default:
			return nil, nil, fmt.Errorf("invalid token expiry")
		}
		if expiredFor := time.Since(time.Unix(expUnix, 0)); expiredFor > allowExpiredWithin {
			return nil, nil, fmt.Errorf("token expired")
		}
	}

	return token, claims, nil
}

func claimString(claims jwt.MapClaims, key string) string {
	if v, ok := claims[key].(string); ok {
		return v
	}
	return ""
}

// LoginHandler handles local and LDAP authentication
func (h *Handler) LoginHandler(c *fiber.Ctx) error {
	var req LoginRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Invalid request body"})
	}

	req.Username = strings.TrimSpace(req.Username)
	if req.Username == "" || req.Password == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Username and password are required"})
	}

	var displayName string
	var email string
	role := "user"

	if req.Mode == "ldap" {
		ldapEnabled := h.store.GetInfraConfig("LDAP_ENABLED", os.Getenv("LDAP_ENABLED"))
		if ldapEnabled == "" {
			ldapEnabled = "false"
		}
		if ldapEnabled != "true" {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "LDAP authentication is disabled"})
		}
		user, err := loginLDAP(h.store, req.Username, req.Password)
		if err != nil {
			log.Printf("[auth] LDAP login failure for user '%s': %v", req.Username, err)
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "LDAP authentication failed: invalid credentials or connection error"})
		}
		displayName = user.DisplayName
		if displayName == "" {
			displayName = req.Username
		}
		email = user.Email
		if user.IsAdmin {
			role = "admin"
		}
		// Apply stored permissions (or the default template on first login).
		if perm := h.store.RecordUserLogin(req.Username, displayName, email, user.IsAdmin); perm != nil {
			role = perm.Role
		}
	} else {
		// Local bootstrap admin — credentials from Vault/env; synced into
		// Postgres like ASPM EnsureBootstrapAdmin (password + admin role).
		if err := vaultenv.Load(); err != nil {
			log.Printf("[auth] vault reload: %v", err)
		}
		adminUser, _, adminEmail := store.BootstrapAdminCreds()
		if adminUser == "" {
			log.Printf("[auth] ADMIN_USERNAME/BOOTSTRAP_ADMIN_USERNAME not configured")
			return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{"error": "local admin authentication is not configured"})
		}
		if !h.store.VerifyBootstrapAdminPassword(req.Username, req.Password) {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "Invalid local admin credentials"})
		}
		if _, err := h.store.EnsureBootstrapAdmin(); err != nil {
			log.Printf("[auth] bootstrap admin sync: %v", err)
		}
		displayName = "Local Administrator"
		email = adminEmail
		role = "admin"
		if perm := h.store.RecordUserLogin(req.Username, displayName, email, true); perm != nil {
			role = "admin"
		}
	}

	// Issue JWT access token (renew via /api/auth/refresh).
	tokenString, err := issueAccessToken(req.Username, displayName, email, role)
	if err != nil {
		if strings.Contains(err.Error(), "JWT_SECRET") {
			log.Printf("[auth] %v", err)
			return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{"error": "authentication is not configured"})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "Could not generate authentication token"})
	}

	return c.JSON(fiber.Map{
		"token":      tokenString,
		"expires_in": int(accessTokenTTL().Seconds()),
		"user": fiber.Map{
			"username":    req.Username,
			"displayName": displayName,
			"email":       email,
			"role":        role,
		},
	})
}

// GetMeHandler returns current authenticated user details
func (h *Handler) GetMeHandler(c *fiber.Ctx) error {
	userClaims := c.Locals("user").(*jwt.Token).Claims.(jwt.MapClaims)
	return c.JSON(fiber.Map{
		"username":    userClaims["sub"],
		"displayName": userClaims["name"],
		"email":       userClaims["email"],
		"role":        userClaims["role"],
	})
}

type LookupRequest struct {
	Username string `json:"username"`
	Mode     string `json:"mode"` // "local" or "ldap"
}

// LookupAccountHandler checks whether a username can proceed to password entry.
func (h *Handler) LookupAccountHandler(c *fiber.Ctx) error {
	var req LookupRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Invalid request body"})
	}
	username := strings.TrimSpace(req.Username)
	if username == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Username is required"})
	}

	mode := strings.ToLower(strings.TrimSpace(req.Mode))
	if mode == "" {
		mode = "local"
	}

	if mode == "ldap" {
		ldapEnabled := h.store.GetInfraConfig("LDAP_ENABLED", os.Getenv("LDAP_ENABLED"))
		if ldapEnabled == "" {
			ldapEnabled = "false"
		}
		if ldapEnabled != "true" {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "LDAP authentication is disabled"})
		}
		// LDAP directories are validated at bind time; allow password step when LDAP is on.
		return c.JSON(fiber.Map{"exists": true})
	}

	if err := vaultenv.Load(); err != nil {
		log.Printf("[auth] vault reload on lookup: %v", err)
	}
	adminUser, _, _ := store.BootstrapAdminCreds()
	if adminUser != "" && strings.EqualFold(username, adminUser) {
		return c.JSON(fiber.Map{"exists": true})
	}
	if perm := h.store.GetCachedUserPermission(username); perm != nil {
		return c.JSON(fiber.Map{"exists": true})
	}
	return c.JSON(fiber.Map{"exists": false})
}

// RefreshHandler renews an access token. Accepts a token that expired within
// the last 24h so brief offline periods do not force a full login.
func (h *Handler) RefreshHandler(c *fiber.Ctx) error {
	tokenString, err := parseBearerToken(c.Get("Authorization"))
	if err != nil {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "Missing authorization token"})
	}

	_, claims, err := parseAccessToken(tokenString, 24*time.Hour)
	if err != nil {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "Invalid or expired authorization token"})
	}

	username := claimString(claims, "sub")
	displayName := claimString(claims, "name")
	email := claimString(claims, "email")
	role := claimString(claims, "role")
	if role == "" {
		role = "user"
	}

	if perm := h.store.GetCachedUserPermission(username); perm != nil {
		if perm.DisplayName != "" {
			displayName = perm.DisplayName
		}
		if perm.Email != "" {
			email = perm.Email
		}
		if perm.Role != "" {
			role = perm.Role
		}
	}
	if displayName == "" {
		displayName = username
	}

	tokenString, err = issueAccessToken(username, displayName, email, role)
	if err != nil {
		if strings.Contains(err.Error(), "JWT_SECRET") {
			log.Printf("[auth] refresh: %v", err)
			return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{"error": "authentication is not configured"})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "Could not refresh authentication token"})
	}

	return c.JSON(fiber.Map{
		"token":      tokenString,
		"expires_in": int(accessTokenTTL().Seconds()),
		"user": fiber.Map{
			"username":    username,
			"displayName": displayName,
			"email":       email,
			"role":        role,
		},
	})
}

// AuthMiddleware validates JWT Bearer tokens
func AuthMiddleware() fiber.Handler {
	return func(c *fiber.Ctx) error {
		// Bypass auth for non-API, health, ingestion, and public auth routes
		path := c.Path()
		if path == "/api/auth/login" || path == "/api/auth/lookup" || path == "/api/auth/refresh" || path == "/api/health" || path == "/health" || path == "/ready" || path == "/v1/traces" || path == "/api/ingest" {
			return c.Next()
		}

		authHeader := c.Get("Authorization")
		if authHeader == "" {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "Missing authorization token"})
		}

		tokenString, err := parseBearerToken(authHeader)
		if err != nil {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "Invalid authorization header format"})
		}

		token, _, err := parseAccessToken(tokenString, 0)
		if err != nil {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "Invalid or expired authorization token"})
		}

		c.Locals("user", token)
		return c.Next()
	}
}

func loginLDAP(s *store.Store, username, password string) (*LDAPUser, error) {
	ldapURL := s.GetInfraConfig("LDAP_URL", os.Getenv("LDAP_URL"))
	bindDN := s.GetInfraConfig("LDAP_BIND_DN", os.Getenv("LDAP_BIND_DN"))
	bindPassword := s.GetInfraConfig("LDAP_BIND_PASSWORD", os.Getenv("LDAP_BIND_PASSWORD"))
	userBaseDN := s.GetInfraConfig("LDAP_USER_BASE_DN", os.Getenv("LDAP_USER_BASE_DN"))
	userFilterTemplate := s.GetInfraConfig("LDAP_USER_FILTER", os.Getenv("LDAP_USER_FILTER"))
	nameAttr := s.GetInfraConfig("LDAP_USER_NAME_ATTR", os.Getenv("LDAP_USER_NAME_ATTR"))
	emailAttr := s.GetInfraConfig("LDAP_USER_EMAIL_ATTR", os.Getenv("LDAP_USER_EMAIL_ATTR"))

	if ldapURL == "" {
		return nil, fmt.Errorf("LDAP_URL is not configured")
	}
	if nameAttr == "" {
		nameAttr = "displayName"
	}
	if emailAttr == "" {
		emailAttr = "mail"
	}

	// 1. Dial LDAP
	l, err := ldap.DialURL(ldapURL)
	if err != nil {
		return nil, fmt.Errorf("ldap dial: %w", err)
	}
	defer l.Close()

	// 2. Bind as Service User
	err = l.Bind(bindDN, bindPassword)
	if err != nil {
		return nil, fmt.Errorf("ldap service bind: %w", err)
	}

	// 3. Search for User DN
	userFilter := strings.ReplaceAll(userFilterTemplate, "{login}", username)
	searchRequest := ldap.NewSearchRequest(
		userBaseDN,
		ldap.ScopeWholeSubtree, ldap.NeverDerefAliases, 0, 0, false,
		userFilter,
		[]string{"dn", nameAttr, emailAttr, "memberOf"},
		nil,
	)

	sr, err := l.Search(searchRequest)
	if err != nil {
		return nil, fmt.Errorf("ldap search: %w", err)
	}

	if len(sr.Entries) != 1 {
		return nil, fmt.Errorf("user not found or multiple users found")
	}

	userDN := sr.Entries[0].DN
	displayName := sr.Entries[0].GetAttributeValue(nameAttr)
	email := sr.Entries[0].GetAttributeValue(emailAttr)

	// Fetch groups to authorize CN=TRACE_ADM membership
	groups := sr.Entries[0].GetAttributeValues("memberOf")
	isAdmin := false
	for _, g := range groups {
		gLower := strings.ToLower(g)
		if strings.Contains(gLower, "cn=trace_adm") || strings.Contains(gLower, "trace_adm") {
			isAdmin = true
			break
		}
	}
	// Fallback check: username CN matches trace_adm
	if strings.ToLower(username) == "trace_adm" {
		isAdmin = true
	}

	// 4. Bind as the User to authenticate password
	err = l.Bind(userDN, password)
	if err != nil {
		return nil, fmt.Errorf("ldap user bind (invalid credentials): %w", err)
	}

	return &LDAPUser{
		DN:          userDN,
		DisplayName: displayName,
		Email:       email,
		IsAdmin:     isAdmin,
	}, nil
}
