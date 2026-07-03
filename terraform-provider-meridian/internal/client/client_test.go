package client

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

// newTestClient returns a client pointed at ts with no token.
func newTestClient(ts *httptest.Server) *Client {
	return New(ts.URL, "")
}

func TestNewTrimsTrailingSlash(t *testing.T) {
	c := New("http://example.com/", "tok")
	if c.endpoint != "http://example.com" {
		t.Fatalf("endpoint = %q, want %q", c.endpoint, "http://example.com")
	}
	if c.token != "tok" {
		t.Fatalf("token = %q, want %q", c.token, "tok")
	}
}

func TestCreateWarehouse(t *testing.T) {
	var gotBody CreateWarehouseRequest
	var gotAuth string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/v2/warehouses" {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		gotAuth = r.Header.Get("Authorization")
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(Warehouse{ID: "01WH", Name: gotBody.Name, StorageRoot: gotBody.StorageRoot})
	}))
	defer ts.Close()

	c := New(ts.URL, "secret-token")
	wh, err := c.CreateWarehouse(context.Background(), CreateWarehouseRequest{
		Name:           "prod",
		StorageRoot:    "s3://bucket/prefix",
		StorageOptions: map[string]string{"region": "us-east-1"},
	})
	if err != nil {
		t.Fatalf("CreateWarehouse: %v", err)
	}
	if wh.ID != "01WH" || wh.Name != "prod" {
		t.Fatalf("warehouse = %+v", wh)
	}
	if gotBody.StorageRoot != "s3://bucket/prefix" {
		t.Fatalf("storage_root = %q", gotBody.StorageRoot)
	}
	if gotBody.StorageOptions["region"] != "us-east-1" {
		t.Fatalf("storage_options = %v", gotBody.StorageOptions)
	}
	if gotAuth != "Bearer secret-token" {
		t.Fatalf("Authorization = %q, want %q", gotAuth, "Bearer secret-token")
	}
}

func TestGetWarehouseByName(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"warehouses": []Warehouse{
				{ID: "01A", Name: "alpha"},
				{ID: "01B", Name: "beta"},
			},
		})
	}))
	defer ts.Close()
	c := newTestClient(ts)

	wh, err := c.GetWarehouseByName(context.Background(), "beta")
	if err != nil {
		t.Fatalf("GetWarehouseByName: %v", err)
	}
	if wh.ID != "01B" {
		t.Fatalf("id = %q, want 01B", wh.ID)
	}

	_, err = c.GetWarehouseByName(context.Background(), "missing")
	if !IsNotFound(err) {
		t.Fatalf("expected not-found error, got %v", err)
	}
}

func TestNoAuthHeaderWhenTokenEmpty(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "" {
			t.Errorf("Authorization = %q, want empty", got)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"warehouses": []Warehouse{}})
	}))
	defer ts.Close()
	if _, err := newTestClient(ts).ListWarehouses(context.Background()); err != nil {
		t.Fatalf("ListWarehouses: %v", err)
	}
}

func TestAPIErrorEnvelope(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error": map[string]any{
				"message": "warehouse already exists",
				"type":    "AlreadyExistsException",
				"code":    409,
			},
		})
	}))
	defer ts.Close()

	_, err := newTestClient(ts).CreateWarehouse(context.Background(), CreateWarehouseRequest{Name: "dup"})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	apiErr, ok := err.(*APIError)
	if !ok {
		t.Fatalf("error type = %T, want *APIError", err)
	}
	if apiErr.Status != http.StatusConflict {
		t.Fatalf("status = %d, want 409", apiErr.Status)
	}
	if apiErr.Type != "AlreadyExistsException" || apiErr.Message != "warehouse already exists" {
		t.Fatalf("api error = %+v", apiErr)
	}
	if IsNotFound(err) {
		t.Fatal("IsNotFound should be false for a 409")
	}
}

func TestAPIErrorNonEnvelopeBody(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("plain text boom"))
	}))
	defer ts.Close()

	_, err := newTestClient(ts).ListRoles(context.Background())
	apiErr, ok := err.(*APIError)
	if !ok {
		t.Fatalf("error type = %T, want *APIError", err)
	}
	if apiErr.Status != http.StatusInternalServerError {
		t.Fatalf("status = %d", apiErr.Status)
	}
	if apiErr.Message != "plain text boom" {
		t.Fatalf("message = %q", apiErr.Message)
	}
}

func TestCreateGrantBody(t *testing.T) {
	var got CreateGrantRequest
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&got)
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(Grant{ID: "01G", Privilege: got.Privilege, SecurableType: "warehouse", SecurableID: "01WH"})
	}))
	defer ts.Close()

	role := "analysts"
	g, err := newTestClient(ts).CreateGrant(context.Background(), CreateGrantRequest{
		Privilege: "READ",
		Role:      &role,
		Securable: GrantSecurable{Type: "warehouse", Warehouse: "prod"},
	})
	if err != nil {
		t.Fatalf("CreateGrant: %v", err)
	}
	if g.ID != "01G" || g.SecurableID != "01WH" {
		t.Fatalf("grant = %+v", g)
	}
	if got.Role == nil || *got.Role != "analysts" {
		t.Fatalf("role in body = %v", got.Role)
	}
	if got.PrincipalID != nil {
		t.Fatalf("principal_id should be omitted, got %v", got.PrincipalID)
	}
	if got.Securable.Warehouse != "prod" {
		t.Fatalf("securable = %+v", got.Securable)
	}
}

func TestGetGrantFiltersListing(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"grants": []Grant{{ID: "01X"}, {ID: "01Y"}},
		})
	}))
	defer ts.Close()
	c := newTestClient(ts)

	g, err := c.GetGrant(context.Background(), "01Y")
	if err != nil {
		t.Fatalf("GetGrant: %v", err)
	}
	if g.ID != "01Y" {
		t.Fatalf("id = %q", g.ID)
	}
	if _, err := c.GetGrant(context.Background(), "nope"); !IsNotFound(err) {
		t.Fatalf("expected not-found, got %v", err)
	}
}

func TestGetWebhookAndDelete(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v2/webhooks/01H":
			_ = json.NewEncoder(w).Encode(Webhook{ID: "01H", URL: "https://x/y", EventTypes: []string{"com.meridian.table.committed"}})
		case r.Method == http.MethodDelete && r.URL.Path == "/api/v2/webhooks/01H":
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
		}
	}))
	defer ts.Close()
	c := newTestClient(ts)

	wh, err := c.GetWebhook(context.Background(), "01H")
	if err != nil {
		t.Fatalf("GetWebhook: %v", err)
	}
	if wh.URL != "https://x/y" || len(wh.EventTypes) != 1 {
		t.Fatalf("webhook = %+v", wh)
	}
	if err := c.DeleteWebhook(context.Background(), "01H"); err != nil {
		t.Fatalf("DeleteWebhook: %v", err)
	}
}

func TestSearchQueryEncoding(t *testing.T) {
	var gotQuery string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		_ = json.NewEncoder(w).Encode(SearchResponse{Results: []SearchResult{{Type: "table", Name: "orders"}}})
	}))
	defer ts.Close()

	resp, err := newTestClient(ts).Search(context.Background(), SearchQuery{
		Query:     "orders",
		Types:     []string{"table", "view"},
		Warehouse: "prod",
		Namespace: "db.sales",
		Limit:     50,
	})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(resp.Results) != 1 || resp.Results[0].Name != "orders" {
		t.Fatalf("results = %+v", resp.Results)
	}
	values, _ := url.ParseQuery(gotQuery)
	if values.Get("q") != "orders" {
		t.Errorf("q = %q", values.Get("q"))
	}
	if values.Get("type") != "table,view" {
		t.Errorf("type = %q", values.Get("type"))
	}
	if values.Get("warehouse") != "prod" {
		t.Errorf("warehouse = %q", values.Get("warehouse"))
	}
	if values.Get("namespace") != "db.sales" {
		t.Errorf("namespace = %q", values.Get("namespace"))
	}
	if values.Get("limit") != "50" {
		t.Errorf("limit = %q", values.Get("limit"))
	}
}

func TestEmptyBodyIsNotADecodeError(t *testing.T) {
	// A 200 with an empty body must not error when out != nil: the decode
	// is skipped, out is left as-is. (Guards against 204/empty responses on
	// endpoints the provider reads.)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	var out Warehouse
	if err := newTestClient(ts).do(context.Background(), http.MethodGet, "/whatever", nil, &out); err != nil {
		t.Fatalf("empty body should not error, got: %v", err)
	}
	if out.ID != "" {
		t.Fatalf("out should be untouched, got %+v", out)
	}
}

func TestDeleteWarehousePathEscape(t *testing.T) {
	var gotPath string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.EscapedPath()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer ts.Close()
	if err := newTestClient(ts).DeleteWarehouse(context.Background(), "we ird/name"); err != nil {
		t.Fatalf("DeleteWarehouse: %v", err)
	}
	if gotPath != "/api/v2/warehouses/we%20ird%2Fname" {
		t.Fatalf("escaped path = %q", gotPath)
	}
}
