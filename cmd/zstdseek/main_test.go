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

func TestResolveInputOutput(t *testing.T) {
	tests := []struct {
		name             string
		args             []string
		outputFlag       string
		verify           bool
		stdoutIsTerminal bool
		wantInput        string
		wantOutput       string
		wantErr          bool
	}{
		{
			name:       "defaults to stdin stdout",
			wantInput:  "-",
			wantOutput: "-",
		},
		{
			name:             "refuses implicit terminal stdout",
			stdoutIsTerminal: true,
			wantErr:          true,
		},
		{
			name:             "allows file output when stdout is terminal",
			outputFlag:       "out.zst",
			stdoutIsTerminal: true,
			wantInput:        "-",
			wantOutput:       "out.zst",
		},
		{
			name:       "preserves explicit input and output",
			args:       []string{"in.dat"},
			outputFlag: "out.zst",
			wantInput:  "in.dat",
			wantOutput: "out.zst",
		},
		{
			name:       "verify requires file output",
			outputFlag: "-",
			verify:     true,
			wantErr:    true,
		},
		{
			name:             "refuses explicit terminal stdout",
			args:             []string{"-"},
			outputFlag:       "-",
			stdoutIsTerminal: true,
			wantErr:          true,
		},
		{
			name:    "rejects multiple inputs",
			args:    []string{"one.dat", "two.dat"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotInput, gotOutput, err := resolveInputOutput(tt.args, tt.outputFlag, tt.verify, tt.stdoutIsTerminal)
			if tt.wantErr {
				if err == nil {
					t.Fatal("resolveInputOutput returned nil error")
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveInputOutput returned error: %v", err)
			}
			if gotInput != tt.wantInput || gotOutput != tt.wantOutput {
				t.Fatalf("resolveInputOutput returned (%q, %q), want (%q, %q)",
					gotInput, gotOutput, tt.wantInput, tt.wantOutput)
			}
		})
	}
}
