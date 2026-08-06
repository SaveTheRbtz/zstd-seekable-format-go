package main

import "testing"

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

func TestResolveInputOutput(t *testing.T) {
	tests := []struct {
		name             string
		args             []string
		inputFlag        string
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
			name:       "accepts positional input",
			args:       []string{"in.dat"},
			outputFlag: "out.zst",
			wantInput:  "in.dat",
			wantOutput: "out.zst",
		},
		{
			name:       "accepts input flag",
			inputFlag:  "in.dat",
			outputFlag: "out.zst",
			wantInput:  "in.dat",
			wantOutput: "out.zst",
		},
		{
			name:      "rejects input flag with positional input",
			args:      []string{"positional.dat"},
			inputFlag: "flag.dat",
			wantErr:   true,
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
			gotInput, gotOutput, err := resolveInputOutput(tt.args, tt.inputFlag, tt.outputFlag, tt.verify, tt.stdoutIsTerminal)
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
