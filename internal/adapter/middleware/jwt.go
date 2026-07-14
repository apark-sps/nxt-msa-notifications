package middleware

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
)

// CognitoClaims maps the exact JWT claim names used in nxt-msa-commons.
// Source: JwtAuthenticationFilter.java lines 30-50
// The filter performs JWT.decode() only — no signature verification.
// The API Gateway / AWS Cognito validates the token before it reaches this service.
type CognitoClaims struct {
	JTI         string `json:"jti"`                // → CONTEXT_SESSION_ID
	UserID      string `json:"custom:iduser"`      // → CONTEXT_USER_ID (USERS####)
	RoleID      string `json:"custom:role"`        // → CONTEXT_ROLE_ID
	HierarchyID string `json:"custom:hierarchyId"` // → CONTEXT_HIERARCHY_ID
	GivenName   string `json:"given_name"`         // → part of CONTEXT_USER_NAME
	FamilyName  string `json:"family_name"`        // → part of CONTEXT_USER_NAME
	Email       string `json:"email"`              // available but not extracted by Java filter
}

// DisplayName assembles a full name matching JwtAuthenticationFilter.processClaims() line 174.
func (c *CognitoClaims) DisplayName() string {
	given := strings.TrimSpace(c.GivenName)
	family := strings.TrimSpace(c.FamilyName)
	if given == "" {
		return family
	}
	if family == "" {
		return given
	}
	return given + " " + family
}

// DecodeJWT performs decode-only JWT parsing without signature verification.
// This matches JwtAuthenticationFilter.processClaims() which calls JWT.decode(token)
// from com.auth0:java-jwt — no Algorithm, no public key, no HMAC secret.
//
// Validity check mirrors JwtAuthenticationFilter.isInvalidRequest():
// rejects the token if jti or custom:iduser is blank.
func DecodeJWT(tokenString string) (*CognitoClaims, error) {
	tokenString = strings.TrimPrefix(tokenString, "Bearer ")

	parts := strings.Split(tokenString, ".")
	if len(parts) != 3 {
		return nil, errors.New("invalid token: expected 3 parts")
	}

	// Decode the payload segment (index 1) — no signature check on segment 2
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, errors.New("invalid token: payload base64 decode failed")
	}

	var claims CognitoClaims
	if err := json.Unmarshal(payload, &claims); err != nil {
		return nil, errors.New("invalid token: payload JSON parse failed")
	}

	// Mirror Java filter: reject if sessionId (jti) or userId is blank
	if strings.TrimSpace(claims.JTI) == "" || strings.TrimSpace(claims.UserID) == "" {
		return nil, errors.New("invalid token: missing jti or custom:iduser claim")
	}

	return &claims, nil
}
