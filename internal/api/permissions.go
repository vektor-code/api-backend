package api

import (
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/golang-jwt/jwt/v5"
	"github.com/kubetrace/api-backend/internal/store"
)

// requireAdmin returns an error response when the caller is not an admin.
func requireAdmin(c *fiber.Ctx) error {
	userClaims, ok := c.Locals("user").(*jwt.Token)
	if ok {
		claims, ok := userClaims.Claims.(jwt.MapClaims)
		if ok && claims["role"] != "admin" {
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{"error": "Forbidden: admin access required"})
		}
	}
	return nil
}

// allowedNamespaces returns the set of namespaces the caller may see.
// nil means unrestricted (admins, local users, or users without a record).
func (h *Handler) allowedNamespaces(c *fiber.Ctx) map[string]bool {
	userClaims, ok := c.Locals("user").(*jwt.Token)
	if !ok {
		return nil
	}
	claims, ok := userClaims.Claims.(jwt.MapClaims)
	if !ok {
		return nil
	}
	if claims["role"] == "admin" {
		return nil
	}
	username, _ := claims["sub"].(string)
	if username == "" {
		return nil
	}

	perm := h.store.GetCachedUserPermission(username)
	if perm == nil {
		return nil
	}
	allowed := make(map[string]bool, len(perm.Namespaces))
	for _, ns := range perm.Namespaces {
		if ns == "*" {
			return nil
		}
		allowed[strings.ToLower(ns)] = true
	}
	return allowed
}

func nsAllowed(allowed map[string]bool, ns string) bool {
	if allowed == nil {
		return true
	}
	return allowed[strings.ToLower(ns)]
}

// GET /api/metrics/timeseries?namespace=&minutes=60
func (h *Handler) GetTimeseries(c *fiber.Ctx) error {
	namespace := c.Query("namespace", "")
	minutes := c.QueryInt("minutes", 60)
	if minutes < 10 {
		minutes = 10
	}
	if minutes > 1440 {
		minutes = 1440
	}

	allowed := h.allowedNamespaces(c)
	if namespace != "" && (h.store.IsNamespaceDisabled(namespace) || !nsAllowed(allowed, namespace)) {
		return c.JSON(&store.TimeseriesData{WindowMinutes: minutes})
	}

	var allowedList []string
	if allowed != nil {
		for ns := range allowed {
			allowedList = append(allowedList, ns)
		}
		if allowedList == nil {
			allowedList = []string{}
		}
	}

	data, err := h.store.GetTimeseries(namespace, allowedList, minutes)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	return c.JSON(data)
}

// GET /api/admin/users
func (h *Handler) GetUsers(c *fiber.Ctx) error {
	if err := requireAdmin(c); err != nil {
		return err
	}
	users, err := h.store.ListUserPermissions()
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	return c.JSON(fiber.Map{"users": users})
}

// PUT /api/admin/users/:username
func (h *Handler) SaveUser(c *fiber.Ctx) error {
	if err := requireAdmin(c); err != nil {
		return err
	}
	var u store.UserPermission
	if err := c.BodyParser(&u); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "invalid request body"})
	}
	u.Username = c.Params("username")
	if u.Username == "" {
		return c.Status(400).JSON(fiber.Map{"error": "username is required"})
	}
	if err := h.store.SaveUserPermission(&u); err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	return c.JSON(fiber.Map{"success": true})
}

// DELETE /api/admin/users/:username
func (h *Handler) DeleteUser(c *fiber.Ctx) error {
	if err := requireAdmin(c); err != nil {
		return err
	}
	if err := h.store.DeleteUserPermission(c.Params("username")); err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	return c.JSON(fiber.Map{"success": true})
}

// GET /api/admin/permission-templates
func (h *Handler) GetPermissionTemplates(c *fiber.Ctx) error {
	if err := requireAdmin(c); err != nil {
		return err
	}
	templates, err := h.store.ListPermissionTemplates()
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	return c.JSON(fiber.Map{"templates": templates})
}

// POST /api/admin/permission-templates
func (h *Handler) SavePermissionTemplate(c *fiber.Ctx) error {
	if err := requireAdmin(c); err != nil {
		return err
	}
	var t store.PermissionTemplate
	if err := c.BodyParser(&t); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "invalid request body"})
	}
	if strings.TrimSpace(t.Name) == "" {
		return c.Status(400).JSON(fiber.Map{"error": "template name is required"})
	}
	if err := h.store.SavePermissionTemplate(&t); err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	return c.JSON(fiber.Map{"success": true})
}

// DELETE /api/admin/permission-templates/:name
func (h *Handler) DeletePermissionTemplate(c *fiber.Ctx) error {
	if err := requireAdmin(c); err != nil {
		return err
	}
	if err := h.store.DeletePermissionTemplate(c.Params("name")); err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	return c.JSON(fiber.Map{"success": true})
}
