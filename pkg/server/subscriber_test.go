package server

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseMaxSize(t *testing.T) {
	stringTests := []struct {
		name     string
		maxSize  string
		expected uint32
	}{
		{
			name:     "zero",
			maxSize:  "0",
			expected: 0,
		},
		{
			name:     "empty",
			maxSize:  "",
			expected: 0,
		},
		{
			name:     "invalid",
			maxSize:  "nope",
			expected: 0,
		},
		{
			name:     "valid",
			maxSize:  "1000000",
			expected: 1000000,
		},
		{
			name:     "uint32max",
			maxSize:  "4294967295",
			expected: 4294967295,
		},
		{
			name:     "uint32max_plus1",
			maxSize:  "4294967296",
			expected: 0,
		},
	}

	for _, tt := range stringTests {
		t.Run(fmt.Sprintf("string %s", tt.name), func(t *testing.T) {
			got := ParseMaxSize(tt.maxSize)
			if got != tt.expected {
				t.Errorf("expected max size to be %d, got %d", tt.expected, got)
			}
		})
	}

	intTests := []struct {
		name     string
		maxSize  int
		expected uint32
	}{
		{
			name:     "zero",
			maxSize:  0,
			expected: 0,
		},
		{
			name:     "uint32max",
			maxSize:  4294967295,
			expected: 4294967295,
		},
		{
			name:     "uint32max_plus1",
			maxSize:  4294967296,
			expected: 0,
		},
	}

	for _, tt := range intTests {
		t.Run(fmt.Sprintf("int %s", tt.name), func(t *testing.T) {
			got := ParseMaxSize(tt.maxSize)
			if got != tt.expected {
				t.Errorf("expected max size to be %d, got %d", tt.expected, got)
			}
		})
	}
}

func TestParseSubscriberOptions(t *testing.T) {
	testCases := []struct {
		name     string
		data     []byte
		expected SubscriberOptionsUpdatePayload
	}{
		{
			name: "empty",
			data: []byte(`{}`),
			expected: SubscriberOptionsUpdatePayload{
				WantedCollections: nil,
				WantedDIDs:        nil,
				MaxSize:           0,
			},
		},
		{
			name: "collection",
			data: []byte(`{"wantedCollections":["foo"]}`),
			expected: SubscriberOptionsUpdatePayload{
				WantedCollections: []string{"foo"},
				WantedDIDs:        nil,
				MaxSize:           0,
			},
		},
		{
			name: "small",
			data: []byte(`{"maxSize":1000}`),
			expected: SubscriberOptionsUpdatePayload{
				WantedCollections: nil,
				WantedDIDs:        nil,
				MaxSize:           1000,
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			var subOptsUpdate SubscriberOptionsUpdatePayload
			if err := json.Unmarshal(testCase.data, &subOptsUpdate); err != nil {
				t.Errorf("failed to unmarshal subscriber options update: %v", err)
			}
			assert.Equal(t, subOptsUpdate, testCase.expected)
		})
	}

}
