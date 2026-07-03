// Package client is a minimal typed HTTP client for the Meridian
// management API (`/api/v2`). It covers exactly the surface the Terraform
// provider needs: warehouses, roles, grants, webhooks, and search.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Client talks to one Meridian server.
type Client struct {
	endpoint string
	token    string
	http     *http.Client
}

// New builds a client for the server at endpoint (e.g.
// "http://localhost:8181"). token may be empty when the server runs with
// authentication disabled.
func New(endpoint, token string) *Client {
	return &Client{
		endpoint: strings.TrimRight(endpoint, "/"),
		token:    token,
		http:     &http.Client{Timeout: 30 * time.Second},
	}
}

// APIError is the server's error envelope
// (`{"error": {"message", "type", "code"}}`) surfaced as a Go error.
type APIError struct {
	Status  int
	Type    string
	Message string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("meridian API error %d (%s): %s", e.Status, e.Type, e.Message)
}

// IsNotFound reports whether err is an APIError with HTTP status 404.
func IsNotFound(err error) bool {
	apiErr, ok := err.(*APIError)
	return ok && apiErr.Status == http.StatusNotFound
}

type errorEnvelope struct {
	Error struct {
		Message string `json:"message"`
		Type    string `json:"type"`
		Code    int    `json:"code"`
	} `json:"error"`
}

// do performs one request. A non-nil body is JSON-encoded; a non-nil out
// receives the decoded JSON response body.
func (c *Client) do(ctx context.Context, method, path string, body, out any) error {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("encoding request body: %w", err)
		}
		reader = bytes.NewReader(encoded)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.endpoint+path, reader)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("calling %s %s: %w", method, path, err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("reading response of %s %s: %w", method, path, err)
	}
	if resp.StatusCode >= 400 {
		var envelope errorEnvelope
		if json.Unmarshal(raw, &envelope) == nil && envelope.Error.Message != "" {
			return &APIError{
				Status:  resp.StatusCode,
				Type:    envelope.Error.Type,
				Message: envelope.Error.Message,
			}
		}
		return &APIError{
			Status:  resp.StatusCode,
			Type:    http.StatusText(resp.StatusCode),
			Message: strings.TrimSpace(string(raw)),
		}
	}
	// A 2xx with an empty body (e.g. 204 No Content) leaves out untouched
	// rather than failing to decode zero bytes.
	if out != nil && len(bytes.TrimSpace(raw)) > 0 {
		if err := json.Unmarshal(raw, out); err != nil {
			return fmt.Errorf("decoding response of %s %s: %w", method, path, err)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Warehouses
// ---------------------------------------------------------------------------

// Warehouse mirrors the management API's warehouse rendering. Secret
// storage-option values are redacted to "***" by the server.
type Warehouse struct {
	ID             string            `json:"id"`
	Name           string            `json:"name"`
	StorageRoot    string            `json:"storage_root"`
	StorageOptions map[string]string `json:"storage_options"`
	CreatedAt      string            `json:"created_at"`
	UpdatedAt      string            `json:"updated_at"`
}

// CreateWarehouseRequest is the body of POST /api/v2/warehouses.
type CreateWarehouseRequest struct {
	Name           string            `json:"name"`
	StorageRoot    string            `json:"storage_root"`
	StorageOptions map[string]string `json:"storage_options,omitempty"`
}

// CreateWarehouse registers a warehouse.
func (c *Client) CreateWarehouse(ctx context.Context, req CreateWarehouseRequest) (*Warehouse, error) {
	var out Warehouse
	if err := c.do(ctx, http.MethodPost, "/api/v2/warehouses", req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ListWarehouses lists all registered warehouses.
func (c *Client) ListWarehouses(ctx context.Context) ([]Warehouse, error) {
	var out struct {
		Warehouses []Warehouse `json:"warehouses"`
	}
	if err := c.do(ctx, http.MethodGet, "/api/v2/warehouses", nil, &out); err != nil {
		return nil, err
	}
	return out.Warehouses, nil
}

// GetWarehouseByName finds one warehouse by name; a missing warehouse is a
// 404 APIError. (The management API has no per-name GET; this filters the
// listing.)
func (c *Client) GetWarehouseByName(ctx context.Context, name string) (*Warehouse, error) {
	warehouses, err := c.ListWarehouses(ctx)
	if err != nil {
		return nil, err
	}
	for i := range warehouses {
		if warehouses[i].Name == name {
			return &warehouses[i], nil
		}
	}
	return nil, &APIError{
		Status:  http.StatusNotFound,
		Type:    "NoSuchWarehouseException",
		Message: fmt.Sprintf("warehouse %q does not exist", name),
	}
}

// DeleteWarehouse deletes an empty warehouse by name.
func (c *Client) DeleteWarehouse(ctx context.Context, name string) error {
	return c.do(ctx, http.MethodDelete, "/api/v2/warehouses/"+url.PathEscape(name), nil, nil)
}

// ---------------------------------------------------------------------------
// Roles
// ---------------------------------------------------------------------------

// Role mirrors the management API's role rendering.
type Role struct {
	ID          string  `json:"id"`
	Name        string  `json:"name"`
	Description *string `json:"description"`
	BuiltIn     bool    `json:"built_in"`
	CreatedAt   string  `json:"created_at"`
}

// CreateRoleRequest is the body of POST /api/v2/roles.
type CreateRoleRequest struct {
	Name        string  `json:"name"`
	Description *string `json:"description,omitempty"`
}

// CreateRole creates a role.
func (c *Client) CreateRole(ctx context.Context, req CreateRoleRequest) (*Role, error) {
	var out Role
	if err := c.do(ctx, http.MethodPost, "/api/v2/roles", req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ListRoles lists all roles.
func (c *Client) ListRoles(ctx context.Context) ([]Role, error) {
	var out struct {
		Roles []Role `json:"roles"`
	}
	if err := c.do(ctx, http.MethodGet, "/api/v2/roles", nil, &out); err != nil {
		return nil, err
	}
	return out.Roles, nil
}

// GetRoleByName finds one role by name; a missing role is a 404 APIError.
func (c *Client) GetRoleByName(ctx context.Context, name string) (*Role, error) {
	roles, err := c.ListRoles(ctx)
	if err != nil {
		return nil, err
	}
	for i := range roles {
		if roles[i].Name == name {
			return &roles[i], nil
		}
	}
	return nil, &APIError{
		Status:  http.StatusNotFound,
		Type:    "NotFoundException",
		Message: fmt.Sprintf("role %q does not exist", name),
	}
}

// DeleteRole deletes a non-built-in role by name (its bindings and grants
// go with it).
func (c *Client) DeleteRole(ctx context.Context, name string) error {
	return c.do(ctx, http.MethodDelete, "/api/v2/roles/"+url.PathEscape(name), nil, nil)
}

// ---------------------------------------------------------------------------
// Grants
// ---------------------------------------------------------------------------

// GrantSecurable addresses the securable a grant attaches to, by name.
type GrantSecurable struct {
	Type      string   `json:"type"`
	Warehouse string   `json:"warehouse"`
	Namespace []string `json:"namespace,omitempty"`
	Table     *string  `json:"table,omitempty"`
	View      *string  `json:"view,omitempty"`
}

// CreateGrantRequest is the body of POST /api/v2/grants. Exactly one of
// Role and PrincipalID selects the grantee.
type CreateGrantRequest struct {
	Privilege   string         `json:"privilege"`
	Role        *string        `json:"role,omitempty"`
	PrincipalID *string        `json:"principal_id,omitempty"`
	Securable   GrantSecurable `json:"securable"`
}

// Grant mirrors the management API's grant rendering. The securable comes
// back as a (type, ULID) pair, not by name.
type Grant struct {
	ID            string  `json:"id"`
	Privilege     string  `json:"privilege"`
	Role          *string `json:"role"`
	PrincipalID   *string `json:"principal_id"`
	SecurableType string  `json:"securable_type"`
	SecurableID   string  `json:"securable_id"`
	GrantedBy     string  `json:"granted_by"`
	CreatedAt     string  `json:"created_at"`
}

// CreateGrant creates a grant.
func (c *Client) CreateGrant(ctx context.Context, req CreateGrantRequest) (*Grant, error) {
	var out Grant
	if err := c.do(ctx, http.MethodPost, "/api/v2/grants", req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ListGrants lists all grants.
func (c *Client) ListGrants(ctx context.Context) ([]Grant, error) {
	var out struct {
		Grants []Grant `json:"grants"`
	}
	if err := c.do(ctx, http.MethodGet, "/api/v2/grants", nil, &out); err != nil {
		return nil, err
	}
	return out.Grants, nil
}

// GetGrant finds one grant by id; a missing grant is a 404 APIError. (The
// management API has no per-id GET; this filters the listing.)
func (c *Client) GetGrant(ctx context.Context, id string) (*Grant, error) {
	grants, err := c.ListGrants(ctx)
	if err != nil {
		return nil, err
	}
	for i := range grants {
		if grants[i].ID == id {
			return &grants[i], nil
		}
	}
	return nil, &APIError{
		Status:  http.StatusNotFound,
		Type:    "NotFoundException",
		Message: fmt.Sprintf("grant %q does not exist", id),
	}
}

// DeleteGrant deletes a grant by id.
func (c *Client) DeleteGrant(ctx context.Context, id string) error {
	return c.do(ctx, http.MethodDelete, "/api/v2/grants/"+url.PathEscape(id), nil, nil)
}

// ---------------------------------------------------------------------------
// Webhooks
// ---------------------------------------------------------------------------

// Webhook mirrors the management API's webhook rendering. The signing
// secret is write-only and never appears here.
type Webhook struct {
	ID         string   `json:"id"`
	URL        string   `json:"url"`
	EventTypes []string `json:"event_types"`
	CreatedAt  string   `json:"created_at"`
	UpdatedAt  string   `json:"updated_at"`
}

// CreateWebhookRequest is the body of POST /api/v2/webhooks.
type CreateWebhookRequest struct {
	URL        string   `json:"url"`
	EventTypes []string `json:"event_types,omitempty"`
	Secret     string   `json:"secret"`
}

// CreateWebhook registers a webhook endpoint.
func (c *Client) CreateWebhook(ctx context.Context, req CreateWebhookRequest) (*Webhook, error) {
	var out Webhook
	if err := c.do(ctx, http.MethodPost, "/api/v2/webhooks", req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// GetWebhook loads one webhook endpoint by id.
func (c *Client) GetWebhook(ctx context.Context, id string) (*Webhook, error) {
	var out Webhook
	if err := c.do(ctx, http.MethodGet, "/api/v2/webhooks/"+url.PathEscape(id), nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// DeleteWebhook deletes a webhook endpoint by id.
func (c *Client) DeleteWebhook(ctx context.Context, id string) error {
	return c.do(ctx, http.MethodDelete, "/api/v2/webhooks/"+url.PathEscape(id), nil, nil)
}

// ---------------------------------------------------------------------------
// Search
// ---------------------------------------------------------------------------

// SearchQuery holds the parameters of GET /api/v2/search.
type SearchQuery struct {
	Query     string
	Types     []string // table | view | namespace; empty = all
	Warehouse string   // restrict to one warehouse by name
	Namespace string   // dot-separated namespace path prefix
	Limit     int64    // 1–100; 0 = server default (20)
}

// SearchResult is one ranked hit.
type SearchResult struct {
	Type      string   `json:"type"`
	ID        string   `json:"id"`
	Name      string   `json:"name"`
	Namespace []string `json:"namespace"`
	Warehouse string   `json:"warehouse"`
	Rank      float64  `json:"rank"`
	Snippet   string   `json:"snippet"`
}

// SearchResponse is the body of GET /api/v2/search.
type SearchResponse struct {
	Results       []SearchResult `json:"results"`
	NextPageToken *string        `json:"next_page_token"`
}

// Search runs one ranked full-text search (single page).
func (c *Client) Search(ctx context.Context, q SearchQuery) (*SearchResponse, error) {
	params := url.Values{}
	params.Set("q", q.Query)
	if len(q.Types) > 0 {
		params.Set("type", strings.Join(q.Types, ","))
	}
	if q.Warehouse != "" {
		params.Set("warehouse", q.Warehouse)
	}
	if q.Namespace != "" {
		params.Set("namespace", q.Namespace)
	}
	if q.Limit > 0 {
		params.Set("limit", strconv.FormatInt(q.Limit, 10))
	}
	var out SearchResponse
	if err := c.do(ctx, http.MethodGet, "/api/v2/search?"+params.Encode(), nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}
