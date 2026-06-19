package main

import "testing"

func TestCompressionConcurrency(t *testing.T) {
	tests := []struct {
		name               string
		threads            int
		defaultConcurrency int
		want               int
		wantOK             bool
	}{
		{
			name:               "default",
			threads:            0,
			defaultConcurrency: 8,
			want:               8,
			wantOK:             true,
		},
		{
			name:               "explicit",
			threads:            2,
			defaultConcurrency: 8,
			want:               2,
			wantOK:             true,
		},
		{
			name:               "negative",
			threads:            -1,
			defaultConcurrency: 8,
			wantOK:             false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := compressionConcurrency(tt.threads, tt.defaultConcurrency)
			if got != tt.want || ok != tt.wantOK {
				t.Fatalf("compressionConcurrency(%d, %d) = (%d, %t), want (%d, %t)",
					tt.threads, tt.defaultConcurrency, got, ok, tt.want, tt.wantOK)
			}
		})
	}
}

func TestParseChunkSizes(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		wantMin int
		wantAvg int
		wantMax int
		wantErr bool
	}{
		{
			name:    "explicit range",
			in:      "128:1024:8192",
			wantMin: 128 * 1024,
			wantAvg: 1024 * 1024,
			wantMax: 8192 * 1024,
		},
		{
			name:    "average shorthand",
			in:      "1024",
			wantMin: 256 * 1024,
			wantAvg: 1024 * 1024,
			wantMax: 4096 * 1024,
		},
		{
			name:    "average shorthand uses integer kb",
			in:      "5",
			wantMin: 1 * 1024,
			wantAvg: 5 * 1024,
			wantMax: 20 * 1024,
		},
		{
			name:    "wrong field count",
			in:      "128:1024",
			wantErr: true,
		},
		{
			name:    "invalid shorthand",
			in:      "nope",
			wantErr: true,
		},
		{
			name:    "invalid range",
			in:      "128:nope:8192",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotMin, gotAvg, gotMax, err := parseChunkSizes(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatal("parseChunkSizes returned nil error")
				}
				return
			}
			if err != nil {
				t.Fatalf("parseChunkSizes returned error: %v", err)
			}
			if gotMin != tt.wantMin || gotAvg != tt.wantAvg || gotMax != tt.wantMax {
				t.Fatalf("parseChunkSizes(%q) = (%d, %d, %d), want (%d, %d, %d)",
					tt.in, gotMin, gotAvg, gotMax, tt.wantMin, tt.wantAvg, tt.wantMax)
			}
		})
	}
}
