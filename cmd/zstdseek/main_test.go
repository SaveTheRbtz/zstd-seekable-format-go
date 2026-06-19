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
