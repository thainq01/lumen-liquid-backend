package main

import "testing"

func TestLedgerFromCursor(t *testing.T) {
	tests := []struct {
		name      string
		cursor    string
		wantLedge uint32
		wantOK    bool
	}{
		{
			// Real cursor observed from a stuck empty-window poll: TOID high
			// 32 bits = ledger 3184477.
			name:      "empty window cursor advances ledger",
			cursor:    "0013677228864831487-4294967295",
			wantLedge: 3184477,
			wantOK:    true,
		},
		{
			name:      "toid without event suffix",
			cursor:    "0013677228864831487",
			wantLedge: 3184477,
			wantOK:    true,
		},
		{
			name:   "empty cursor",
			cursor: "",
			wantOK: false,
		},
		{
			name:   "non-numeric toid",
			cursor: "abc-1",
			wantOK: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := ledgerFromCursor(tt.cursor)
			if ok != tt.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tt.wantOK)
			}
			if ok && got != tt.wantLedge {
				t.Errorf("ledger = %d, want %d", got, tt.wantLedge)
			}
		})
	}
}
