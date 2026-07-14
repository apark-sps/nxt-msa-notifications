package domain_test

import (
	"testing"

	"nxt-msa-notifications/internal/domain"
)

func TestGenerateID_IsDeterministic(t *testing.T) {
	eventID := "evt-abc-123"
	userID := "USERS0001"

	id1 := domain.GenerateID(eventID, userID)
	id2 := domain.GenerateID(eventID, userID)

	if id1 != id2 {
		t.Errorf("GenerateID must be deterministic: got %q and %q for the same inputs", id1, id2)
	}
}

func TestGenerateID_DifferentEventIDs_ProduceDifferentIDs(t *testing.T) {
	userID := "USERS0001"

	id1 := domain.GenerateID("evt-001", userID)
	id2 := domain.GenerateID("evt-002", userID)

	if id1 == id2 {
		t.Error("GenerateID must produce different IDs for different eventIDs")
	}
}

func TestGenerateID_DifferentUserIDs_ProduceDifferentIDs(t *testing.T) {
	eventID := "evt-abc-123"

	id1 := domain.GenerateID(eventID, "USERS0001")
	id2 := domain.GenerateID(eventID, "USERS0002")

	if id1 == id2 {
		t.Error("GenerateID must produce different IDs for different userIDs")
	}
}

func TestGenerateID_IsValidUUIDFormat(t *testing.T) {
	id := domain.GenerateID("evt-abc-123", "USERS0001")

	// A standard UUID is 36 characters: 8-4-4-4-12 with hyphens
	if len(id) != 36 {
		t.Errorf("GenerateID must return a standard 36-char UUID, got len=%d: %q", len(id), id)
	}
}

func TestDeliveryStatus_Constants(t *testing.T) {
	tests := []struct {
		status   domain.DeliveryStatus
		expected string
	}{
		{domain.StatusPending, "pending"},
		{domain.StatusDelivered, "delivered"},
		{domain.StatusRead, "read"},
		{domain.StatusFailed, "failed"},
	}

	for _, tc := range tests {
		if string(tc.status) != tc.expected {
			t.Errorf("DeliveryStatus constant mismatch: got %q, want %q", tc.status, tc.expected)
		}
	}
}
