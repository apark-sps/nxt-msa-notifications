package middleware_test

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"

	"nxt-msa-notifications/internal/adapter/middleware"
)

// buildToken constructs a mock JWT with a given payload map.
// The header and signature segments are dummy values — matching the decode-only contract.
func buildToken(payload map[string]string) string {
	header, _ := json.Marshal(map[string]string{"alg": "RS256", "typ": "JWT"})
	body, _ := json.Marshal(payload)

	h := base64.RawURLEncoding.EncodeToString(header)
	b := base64.RawURLEncoding.EncodeToString(body)
	sig := base64.RawURLEncoding.EncodeToString([]byte("dummy-signature"))

	return strings.Join([]string{h, b, sig}, ".")
}

func TestDecodeJWT_ValidToken_ReturnsCorrectClaims(t *testing.T) {
	token := buildToken(map[string]string{
		"jti":                "session-001",
		"custom:iduser":      "USERS0001",
		"custom:role":        "ROLE_ADMIN",
		"custom:hierarchyId": "1",
		"given_name":         "John",
		"family_name":        "Doe",
	})

	claims, err := middleware.DecodeJWT(token)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if claims.UserID != "USERS0001" {
		t.Errorf("UserID: got %q, want %q", claims.UserID, "USERS0001")
	}
	if claims.JTI != "session-001" {
		t.Errorf("JTI: got %q, want %q", claims.JTI, "session-001")
	}
	if claims.RoleID != "ROLE_ADMIN" {
		t.Errorf("RoleID: got %q, want %q", claims.RoleID, "ROLE_ADMIN")
	}
}

func TestDecodeJWT_WithBearerPrefix_StripsPrefix(t *testing.T) {
	token := buildToken(map[string]string{
		"jti":           "session-001",
		"custom:iduser": "USERS0001",
	})

	claims, err := middleware.DecodeJWT("Bearer " + token)
	if err != nil {
		t.Fatalf("expected no error with Bearer prefix, got: %v", err)
	}
	if claims.UserID != "USERS0001" {
		t.Errorf("UserID: got %q, want %q", claims.UserID, "USERS0001")
	}
}

func TestDecodeJWT_MissingUserID_ReturnsError(t *testing.T) {
	token := buildToken(map[string]string{
		"jti": "session-001",
		// custom:iduser intentionally omitted
	})

	_, err := middleware.DecodeJWT(token)
	if err == nil {
		t.Error("expected error when custom:iduser is missing, got nil")
	}
}

func TestDecodeJWT_MissingJTI_ReturnsError(t *testing.T) {
	token := buildToken(map[string]string{
		"custom:iduser": "USERS0001",
		// jti intentionally omitted
	})

	_, err := middleware.DecodeJWT(token)
	if err == nil {
		t.Error("expected error when jti is missing, got nil")
	}
}

func TestDecodeJWT_MalformedToken_ReturnsError(t *testing.T) {
	_, err := middleware.DecodeJWT("not-a-jwt")
	if err == nil {
		t.Error("expected error for malformed token, got nil")
	}
}

func TestDecodeJWT_EmptyToken_ReturnsError(t *testing.T) {
	_, err := middleware.DecodeJWT("")
	if err == nil {
		t.Error("expected error for empty token string, got nil")
	}
}

func TestCognitoClaims_DisplayName_FullName(t *testing.T) {
	claims := &middleware.CognitoClaims{GivenName: "John", FamilyName: "Doe"}
	if got := claims.DisplayName(); got != "John Doe" {
		t.Errorf("DisplayName: got %q, want %q", got, "John Doe")
	}
}

func TestCognitoClaims_DisplayName_OnlyGiven(t *testing.T) {
	claims := &middleware.CognitoClaims{GivenName: "John"}
	if got := claims.DisplayName(); got != "John" {
		t.Errorf("DisplayName (given only): got %q, want %q", got, "John")
	}
}

func TestCognitoClaims_DisplayName_OnlyFamily(t *testing.T) {
	claims := &middleware.CognitoClaims{FamilyName: "Doe"}
	if got := claims.DisplayName(); got != "Doe" {
		t.Errorf("DisplayName (family only): got %q, want %q", got, "Doe")
	}
}
