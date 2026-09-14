package api

import (
	"github.com/gofiber/fiber/v2"
	"github.com/kubetrace/api-backend/internal/alerts"
)

func (h *Handler) alertStore() *alerts.MemoryStore {
	if h.alerts == nil || h.alerts.Store == nil {
		return nil
	}
	return h.alerts.Store
}

func (h *Handler) ListAlertRules(c *fiber.Ctx) error {
	s := h.alertStore()
	if s == nil {
		return c.JSON(fiber.Map{"rules": []alerts.Rule{}})
	}
	return c.JSON(fiber.Map{"rules": s.ListRules()})
}

func (h *Handler) UpsertAlertRule(c *fiber.Ctx) error {
	s := h.alertStore()
	if s == nil {
		return c.Status(503).JSON(fiber.Map{"error": "alerting not configured"})
	}
	var rule alerts.Rule
	if err := c.BodyParser(&rule); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "invalid rule body"})
	}
	if rule.Name == "" || rule.Metric == "" {
		return c.Status(400).JSON(fiber.Map{"error": "name and metric are required"})
	}
	saved, err := s.UpsertRule(rule)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	return c.JSON(saved)
}

func (h *Handler) DeleteAlertRule(c *fiber.Ctx) error {
	s := h.alertStore()
	if s == nil {
		return c.Status(503).JSON(fiber.Map{"error": "alerting not configured"})
	}
	if err := s.DeleteRule(c.Params("id")); err != nil {
		return c.Status(404).JSON(fiber.Map{"error": err.Error()})
	}
	return c.SendStatus(fiber.StatusNoContent)
}

func (h *Handler) ListAlertChannels(c *fiber.Ctx) error {
	s := h.alertStore()
	if s == nil {
		return c.JSON(fiber.Map{"channels": []alerts.Channel{}})
	}
	return c.JSON(fiber.Map{"channels": s.ListChannels()})
}

func (h *Handler) UpsertAlertChannel(c *fiber.Ctx) error {
	s := h.alertStore()
	if s == nil {
		return c.Status(503).JSON(fiber.Map{"error": "alerting not configured"})
	}
	var ch alerts.Channel
	if err := c.BodyParser(&ch); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "invalid channel body"})
	}
	if ch.Name == "" || ch.Target == "" {
		return c.Status(400).JSON(fiber.Map{"error": "name and target are required"})
	}
	saved, err := s.UpsertChannel(ch)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	return c.JSON(saved)
}

func (h *Handler) DeleteAlertChannel(c *fiber.Ctx) error {
	s := h.alertStore()
	if s == nil {
		return c.Status(503).JSON(fiber.Map{"error": "alerting not configured"})
	}
	if err := s.DeleteChannel(c.Params("id")); err != nil {
		return c.Status(404).JSON(fiber.Map{"error": err.Error()})
	}
	return c.SendStatus(fiber.StatusNoContent)
}

func (h *Handler) ListActiveAlerts(c *fiber.Ctx) error {
	s := h.alertStore()
	if s == nil {
		return c.JSON(fiber.Map{"alerts": []alerts.ActiveAlert{}})
	}
	return c.JSON(fiber.Map{"alerts": s.ListActive()})
}

func (h *Handler) EvaluateAlertsNow(c *fiber.Ctx) error {
	if h.alerts == nil {
		return c.Status(503).JSON(fiber.Map{"error": "alerting not configured"})
	}
	firings := h.alerts.EvaluateOnce(c.Context())
	return c.JSON(fiber.Map{"alerts": firings, "count": len(firings)})
}
