package api

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/auth"
	"github.com/sirrobot01/synod/internal/store/postgres"
	"golang.org/x/time/rate"
)

const operatorPassword = "correct horse battery"
const bootstrapToken = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func identityServer(t *testing.T) (*Server, *postgres.Store, uuid.UUID) {
	t.Helper()
	store, _, _ := apiTestStore(t)
	admin, err := auth.NewAdminToken(bootstrapToken, "bootstrap-admin")
	if err != nil {
		t.Fatal(err)
	}
	installation := uuid.New()
	server, err := NewServer(store, admin, installation, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	server.PublicOrigin = "https://synod.example.com"
	return server, store, installation
}

// call runs one request and returns its recorder.
func call(t *testing.T, server *Server, method, path, body string, prepare ...func(*http.Request)) *httptest.ResponseRecorder {
	t.Helper()
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, path, reader)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	for _, apply := range prepare {
		apply(req)
	}
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)
	return rec
}

func withBootstrap(r *http.Request)   { r.Header.Set("Authorization", "Bearer "+bootstrapToken) }
func sameOriginFetch(r *http.Request) { r.Header.Set("Sec-Fetch-Site", "same-origin") }

func fromAddress(address string) func(*http.Request) {
	return func(r *http.Request) { r.RemoteAddr = address }
}

func withCookie(value string) func(*http.Request) {
	return func(r *http.Request) { r.AddCookie(&http.Cookie{Name: SessionCookie, Value: value}) }
}

// sessionCookie signs in and returns the cookie value that the response set.
func sessionCookie(t *testing.T, server *Server, username, password string) string {
	t.Helper()
	rec := call(t, server, "POST", "/v1/sessions", `{"username":"`+username+`","password":"`+password+`"}`, sameOriginFetch)
	if rec.Code != 201 {
		t.Fatalf("sign-in: %d %s", rec.Code, rec.Body)
	}
	for _, cookie := range rec.Result().Cookies() {
		if cookie.Name == SessionCookie {
			if !cookie.HttpOnly || !cookie.Secure || cookie.SameSite != http.SameSiteStrictMode || cookie.Path != "/" || cookie.Domain != "" {
				t.Fatalf("session cookie attributes: %+v", cookie)
			}
			return cookie.Value
		}
	}
	t.Fatal("sign-in set no session cookie")
	return ""
}

// createOperator uses the bootstrap credential, which is how the first account appears.
func createOperator(t *testing.T, server *Server, username string) uuid.UUID {
	t.Helper()
	rec := call(t, server, "POST", "/v1/users", `{"username":"`+username+`","password":"`+operatorPassword+`"}`, withBootstrap)
	if rec.Code != 201 {
		t.Fatalf("create user: %d %s", rec.Code, rec.Body)
	}
	var user postgres.User
	if err := json.Unmarshal(rec.Body.Bytes(), &user); err != nil {
		t.Fatal(err)
	}
	return user.ID
}

func TestBootstrapCredentialCreatesTheFirstOperator(t *testing.T) {
	server, store, installation := identityServer(t)
	if total, err := store.CountUsers(t.Context(), installation); err != nil || total != 0 {
		t.Fatalf("a new installation has %d accounts: %v", total, err)
	}
	createOperator(t, server, "operator")
	if total, err := store.CountUsers(t.Context(), installation); err != nil || total != 1 {
		t.Fatalf("accounts=%d: %v", total, err)
	}
	// Creating an account needs a credential.
	if rec := call(t, server, "POST", "/v1/users", `{"username":"other","password":"`+operatorPassword+`"}`); rec.Code != 401 {
		t.Fatalf("unauthenticated create: %d", rec.Code)
	}
	// A short password is accepted, so it is not in this table. The server applies
	// no minimum length. The upper bound stays, because it caps the hashing work
	// that one request can demand.
	for name, body := range map[string]string{
		"repeated username":  `{"username":"operator","password":"` + operatorPassword + `"}`,
		"oversized password": `{"username":"other","password":"` + strings.Repeat("a", auth.MaxPasswordBytes+1) + `"}`,
		"empty username":     `{"username":"","password":"` + operatorPassword + `"}`,
		"upper case":         `{"username":"Operator!","password":"` + operatorPassword + `"}`,
	} {
		rec := call(t, server, "POST", "/v1/users", body, withBootstrap)
		if rec.Code != 400 && rec.Code != 409 {
			t.Fatalf("%s: %d %s", name, rec.Code, rec.Body)
		}
	}
}

// The API applies no minimum password length. An operator chooses the strength of
// a password, so a short one must create an account and must sign in with it.
func TestShortPasswordCreatesAnAccountAndSignsIn(t *testing.T) {
	server, _, _ := identityServer(t)
	rec := call(t, server, "POST", "/v1/users", `{"username":"operator","password":"pw"}`, withBootstrap)
	if rec.Code != 201 {
		t.Fatalf("create with a short password: %d %s", rec.Code, rec.Body)
	}
	if cookie := sessionCookie(t, server, "operator", "pw"); cookie == "" {
		t.Fatal("a short password did not sign in")
	}
	// A short password still has to be the right one.
	if rec := call(t, server, "POST", "/v1/sessions", `{"username":"operator","password":"pX"}`, sameOriginFetch); rec.Code != 401 {
		t.Fatalf("a wrong short password answered %d", rec.Code)
	}
}

func TestSignInIssuesASessionAndSignOutEndsIt(t *testing.T) {
	server, _, _ := identityServer(t)
	createOperator(t, server, "operator")
	cookie := sessionCookie(t, server, "operator", operatorPassword)
	rec := call(t, server, "GET", "/v1/sessions/current", "", withCookie(cookie))
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"operator"`) {
		t.Fatalf("current session: %d %s", rec.Code, rec.Body)
	}
	// Two sign-ins never share an identity.
	if other := sessionCookie(t, server, "operator", operatorPassword); other == cookie {
		t.Fatal("two sign-ins share one session value")
	}
	if rec := call(t, server, "DELETE", "/v1/sessions/current", "", withCookie(cookie), sameOriginFetch); rec.Code != 204 {
		t.Fatalf("sign-out: %d %s", rec.Code, rec.Body)
	}
	rec = call(t, server, "GET", "/v1/sessions/current", "", withCookie(cookie))
	if rec.Code != 401 {
		t.Fatalf("a revoked session still answered: %d", rec.Code)
	}
	// A refused cookie is cleared so the browser stops presenting it.
	cleared := false
	for _, set := range rec.Result().Cookies() {
		if set.Name == SessionCookie && set.MaxAge < 0 {
			cleared = true
		}
	}
	if !cleared {
		t.Fatal("a refused session cookie was not cleared")
	}
}

func TestSignInRefusesWeakCredentials(t *testing.T) {
	server, _, _ := identityServer(t)
	createOperator(t, server, "operator")
	for name, body := range map[string]string{
		"wrong password":   `{"username":"operator","password":"wrong password here"}`,
		"unknown username": `{"username":"nobody","password":"` + operatorPassword + `"}`,
		"empty body":       `{}`,
	} {
		rec := call(t, server, "POST", "/v1/sessions", body, sameOriginFetch)
		if rec.Code != 401 {
			t.Fatalf("%s: %d %s", name, rec.Code, rec.Body)
		}
		if strings.Contains(rec.Body.String(), "operator") || strings.Contains(rec.Body.String(), "nobody") {
			t.Fatalf("%s: the answer names the account", name)
		}
	}
	// A garbage cookie is refused without reaching a handler.
	if rec := call(t, server, "GET", "/v1/sessions/current", "", withCookie("not-a-session")); rec.Code != 401 {
		t.Fatalf("garbage cookie: %d", rec.Code)
	}
}

func TestDisabledAccountIsForbiddenNotUnauthenticated(t *testing.T) {
	server, _, _ := identityServer(t)
	id := createOperator(t, server, "operator")
	cookie := sessionCookie(t, server, "operator", operatorPassword)
	if rec := call(t, server, "POST", "/v1/users/"+id.String()+"/actions/disable", "", withBootstrap); rec.Code != 204 {
		t.Fatalf("disable: %d %s", rec.Code, rec.Body)
	}
	// A correct password for a disabled account is a refusal, not a bad credential.
	rec := call(t, server, "POST", "/v1/sessions", `{"username":"operator","password":"`+operatorPassword+`"}`, sameOriginFetch)
	if rec.Code != 403 || !strings.Contains(rec.Body.String(), "account_disabled") {
		t.Fatalf("disabled sign-in: %d %s", rec.Code, rec.Body)
	}
	// Disabling ended the live session as well.
	if rec := call(t, server, "GET", "/v1/sessions/current", "", withCookie(cookie)); rec.Code != 401 {
		t.Fatalf("a disabled account kept its session: %d", rec.Code)
	}
}

// A cookie is ambient, so an unsafe method must prove its origin.
func TestCookieRequestNeedsAnOriginProof(t *testing.T) {
	server, _, _ := identityServer(t)
	createOperator(t, server, "operator")
	cookie := sessionCookie(t, server, "operator", operatorPassword)
	// No proof at all.
	rec := call(t, server, "DELETE", "/v1/sessions/current", "", withCookie(cookie))
	if rec.Code != 403 || !strings.Contains(rec.Body.String(), "cross_origin") {
		t.Fatalf("a cookie request changed state with no origin proof: %d %s", rec.Code, rec.Body)
	}
	// Another site's origin is not a proof.
	rec = call(t, server, "DELETE", "/v1/sessions/current", "", withCookie(cookie), func(r *http.Request) {
		r.Header.Set("Origin", "https://attacker.example.com")
		r.Header.Set("Sec-Fetch-Site", "cross-site")
	})
	if rec.Code != 403 {
		t.Fatalf("a cross-site origin was accepted: %d %s", rec.Code, rec.Body)
	}
	// The configured origin is a proof.
	rec = call(t, server, "DELETE", "/v1/sessions/current", "", withCookie(cookie), func(r *http.Request) {
		r.Header.Set("Origin", server.PublicOrigin)
	})
	if rec.Code != 204 {
		t.Fatalf("the configured origin was refused: %d %s", rec.Code, rec.Body)
	}
	// A safe method needs no proof.
	fresh := sessionCookie(t, server, "operator", operatorPassword)
	if rec := call(t, server, "GET", "/v1/sessions/current", "", withCookie(fresh)); rec.Code != 200 {
		t.Fatalf("a safe cookie request was refused: %d", rec.Code)
	}
}

// A bearer credential is not ambient, so it needs no origin proof.
func TestBearerRequestNeedsNoOriginProof(t *testing.T) {
	server, _, _ := identityServer(t)
	rec := call(t, server, "POST", "/v1/tokens", `{"name":"deploy pipeline"}`, withBootstrap)
	if rec.Code != 201 {
		t.Fatalf("token create with no origin header: %d %s", rec.Code, rec.Body)
	}
	var created struct {
		ID    uuid.UUID `json:"id"`
		Token string    `json:"token"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(created.Token, "synod_") {
		t.Fatalf("token %q", created.Token)
	}
	// The minted token authenticates, and it too needs no origin proof.
	present := func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+created.Token) }
	if rec := call(t, server, "GET", "/v1/tokens", "", present); rec.Code != 200 {
		t.Fatalf("token authentication: %d %s", rec.Code, rec.Body)
	}
	// A listing never repeats the secret.
	list := call(t, server, "GET", "/v1/tokens", "", withBootstrap)
	if strings.Contains(list.Body.String(), created.Token) {
		t.Fatal("a token listing repeated the secret")
	}
	if rec := call(t, server, "POST", "/v1/tokens/"+created.ID.String()+"/actions/revoke", "", withBootstrap); rec.Code != 204 {
		t.Fatalf("revoke: %d %s", rec.Code, rec.Body)
	}
	if rec := call(t, server, "GET", "/v1/tokens", "", present); rec.Code != 401 {
		t.Fatalf("a revoked token still answered: %d", rec.Code)
	}
}

func TestPasswordChangeEndsOtherSessionsOverTheAPI(t *testing.T) {
	server, _, _ := identityServer(t)
	id := createOperator(t, server, "operator")
	keep := sessionCookie(t, server, "operator", operatorPassword)
	other := sessionCookie(t, server, "operator", operatorPassword)
	// A session alone is not enough. Self-service re-authenticates first.
	weak := call(t, server, "POST", "/v1/users/"+id.String()+"/actions/set-password", `{"password":"a different password"}`, withCookie(keep), sameOriginFetch)
	if weak.Code != 403 || !strings.Contains(weak.Body.String(), "reauthentication_required") {
		t.Fatalf("a stolen session changed a password: %d %s", weak.Code, weak.Body)
	}
	rec := call(t, server, "POST", "/v1/users/"+id.String()+"/actions/set-password",
		`{"current_password":"`+operatorPassword+`","password":"a different password"}`, withCookie(keep), sameOriginFetch)
	if rec.Code != 204 {
		t.Fatalf("set password: %d %s", rec.Code, rec.Body)
	}
	if rec := call(t, server, "GET", "/v1/sessions/current", "", withCookie(other)); rec.Code != 401 {
		t.Fatalf("another session survived a password change: %d", rec.Code)
	}
	if rec := call(t, server, "GET", "/v1/sessions/current", "", withCookie(keep)); rec.Code != 200 {
		t.Fatalf("the caller lost its own session: %d", rec.Code)
	}
	if rec := call(t, server, "POST", "/v1/sessions", `{"username":"operator","password":"a different password"}`, sameOriginFetch); rec.Code != 201 {
		t.Fatalf("the new password was refused: %d %s", rec.Code, rec.Body)
	}
}

// Sign-in changes state and sets a cookie, so a cross-site form post must not work.
func TestSignInProvesItsOrigin(t *testing.T) {
	server, _, _ := identityServer(t)
	createOperator(t, server, "operator")
	body := `{"username":"operator","password":"` + operatorPassword + `"}`
	rec := call(t, server, "POST", "/v1/sessions", body)
	if rec.Code != 403 || !strings.Contains(rec.Body.String(), "cross_origin") {
		t.Fatalf("a sign-in with no origin proof: %d %s", rec.Code, rec.Body)
	}
	rec = call(t, server, "POST", "/v1/sessions", body, func(r *http.Request) {
		r.Header.Set("Origin", "https://attacker.example.com")
		r.Header.Set("Sec-Fetch-Site", "cross-site")
	})
	if rec.Code != 403 {
		t.Fatalf("a cross-site sign-in: %d %s", rec.Code, rec.Body)
	}
	if rec := call(t, server, "POST", "/v1/sessions", body, func(r *http.Request) { r.Header.Set("Origin", server.PublicOrigin) }); rec.Code != 201 {
		t.Fatalf("the configured origin was refused: %d %s", rec.Code, rec.Body)
	}
}

// A guesser must spend its own budget, not one that every account shares.
// The keyed budget is stricter than the shared one, so it must bite first,
// and exhausting one account must leave another account usable.
func TestSignInBudgetIsKeyed(t *testing.T) {
	server, _, _ := identityServer(t)
	// Password checks can exceed the refill interval on a loaded host.
	server.signInAttempts = newAttemptLimiter(0, 5, 4096)
	createOperator(t, server, "victim")
	createOperator(t, server, "other")
	wrong := `{"username":"victim","password":"wrong password here"}`
	limited := false
	for range 6 {
		if call(t, server, "POST", "/v1/sessions", wrong, sameOriginFetch, fromAddress("198.51.100.7:4000")).Code == 429 {
			limited = true
			break
		}
	}
	if !limited {
		t.Fatal("guesses against one account spent no account budget")
	}
	// The same address is now spent for every account, which stops one guesser.
	if rec := call(t, server, "POST", "/v1/sessions", `{"username":"other","password":"`+operatorPassword+`"}`, sameOriginFetch, fromAddress("198.51.100.7:4000")); rec.Code != 429 {
		t.Fatalf("a spent address kept signing in: %d %s", rec.Code, rec.Body)
	}
	// Another address still reaches the untouched account, so one victim
	// cannot lock the installation out for everybody.
	if rec := call(t, server, "POST", "/v1/sessions", `{"username":"other","password":"`+operatorPassword+`"}`, sameOriginFetch, fromAddress("203.0.113.9:4000")); rec.Code != 201 {
		t.Fatalf("one account exhausted another account's budget: %d %s", rec.Code, rec.Body)
	}
}

// Sign-in carries its own budget, so a password guesser cannot spend the shared one.
func TestSignInHasItsOwnRateBudget(t *testing.T) {
	server, _, _ := identityServer(t)
	createOperator(t, server, "operator")
	server.signInLimit = rate.NewLimiter(0, 0)
	rec := call(t, server, "POST", "/v1/sessions", `{"username":"operator","password":"`+operatorPassword+`"}`, sameOriginFetch)
	if rec.Code != 429 || rec.Header().Get("Retry-After") != "1" {
		t.Fatalf("sign-in limit: %d", rec.Code)
	}
	// The shared budget still serves every other route.
	if rec := call(t, server, "GET", "/v1/tokens", "", withBootstrap); rec.Code != 200 {
		t.Fatalf("the sign-in limit reached another route: %d", rec.Code)
	}
}
