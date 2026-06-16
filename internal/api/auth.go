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
}

func getJWTSecret() []byte {
	secret := os.Getenv("JWT_SECRET")
	if secret == "" {
		secret = "super-secret-key-2026"
	}
	return []byte(secret)
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

	if req.Mode == "ldap" {
		if os.Getenv("LDAP_ENABLED") == "false" {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "LDAP authentication is disabled"})
		}
		user, err := loginLDAP(req.Username, req.Password)
		if err != nil {
			log.Printf("[auth] LDAP login failure for user '%s': %v", req.Username, err)
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "LDAP authentication failed: invalid credentials or connection error"})
		}
		displayName = user.DisplayName
		if displayName == "" {
			displayName = req.Username
		}
		email = user.Email
	} else {
		// Local Admin auth
		adminUser := os.Getenv("ADMIN_USERNAME")
		adminPass := os.Getenv("ADMIN_PASSWORD")
		if adminUser == "" {
			adminUser = "admin"
		}
		if adminPass == "" {
			adminPass = "tracesvc-admin-pass-2026"
		}

		if req.Username != adminUser || req.Password != adminPass {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "Invalid local admin credentials"})
		}
		displayName = "Local Administrator"
		email = "admin@kubetrace.local"
	}

	// Issue JWT token
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub":   req.Username,
		"name":  displayName,
		"email": email,
		"role":  "admin",
		"exp":   time.Now().Add(24 * time.Hour).Unix(),
	})

	tokenString, err := token.SignedString(getJWTSecret())
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "Could not generate authentication token"})
	}

	return c.JSON(fiber.Map{
		"token": tokenString,
		"user": fiber.Map{
			"username":    req.Username,
			"displayName": displayName,
			"email":       email,
			"role":        "admin",
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

// AuthMiddleware validates JWT Bearer tokens
func AuthMiddleware() fiber.Handler {
	return func(c *fiber.Ctx) error {
		// Bypass auth for non-API, health, ingestion, and websockets, or login route itself
		path := c.Path()
		if path == "/api/auth/login" || path == "/api/health" || path == "/health" || path == "/ready" || path == "/v1/traces" || path == "/api/ingest" {
			return c.Next()
		}

		// Read Auth header
		authHeader := c.Get("Authorization")
		if authHeader == "" {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "Missing authorization token"})
		}

		parts := strings.Split(authHeader, " ")
		if len(parts) != 2 || strings.ToLower(parts[0]) != "bearer" {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "Invalid authorization header format"})
		}

		tokenString := parts[1]
		token, err := jwt.Parse(tokenString, func(t *jwt.Token) (interface{}, error) {
			if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
			}
			return getJWTSecret(), nil
		})

		if err != nil || !token.Valid {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "Invalid or expired authorization token"})
		}

		c.Locals("user", token)
		return c.Next()
	}
}

func loginLDAP(username, password string) (*LDAPUser, error) {
	ldapURL := os.Getenv("LDAP_URL")
	bindDN := os.Getenv("LDAP_BIND_DN")
	bindPassword := os.Getenv("LDAP_BIND_PASSWORD")
	userBaseDN := os.Getenv("LDAP_USER_BASE_DN")
	userFilterTemplate := os.Getenv("LDAP_USER_FILTER")
	nameAttr := os.Getenv("LDAP_USER_NAME_ATTR")
	emailAttr := os.Getenv("LDAP_USER_EMAIL_ATTR")

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
		[]string{"dn", nameAttr, emailAttr},
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

	// 4. Bind as the User to authenticate password
	err = l.Bind(userDN, password)
	if err != nil {
		return nil, fmt.Errorf("ldap user bind (invalid credentials): %w", err)
	}

	return &LDAPUser{
		DN:          userDN,
		DisplayName: displayName,
		Email:       email,
	}, nil
}
