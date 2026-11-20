package main

import (
	"sync/atomic"
	"testing"
	"time"
)

type atomicCounter = atomic.Int64

func TestConfigDefaults(t *testing.T) {
	cfg, threshold, openFor, err := loadConfig()
	if err != nil || cfg.interval != 30*time.Second || cfg.llmTimeout != 3*time.Second || threshold != 3 || openFor != 30*time.Second || cfg.history != 30 {
		t.Fatalf("got %+v %d %s %v", cfg, threshold, openFor, err)
	}
}

func TestConfigRejectsBadValues(t *testing.T) {
	t.Setenv("LLM_TIMEOUT_MS", "-5")

	if _, _, _, err := loadConfig(); err == nil {
		t.Fatal("a negative timeout must be refused")
	}

	t.Setenv("LLM_TIMEOUT_MS", "")
	t.Setenv("ANOMALY_HISTORY_BUCKETS", "2")

	if _, _, _, err := loadConfig(); err == nil {
		t.Fatal("too little history must be refused")
	}
}

func TestLLMIsOnlyBuiltWhenAIIsEnabledWithARealKey(t *testing.T) {
	cases := []struct {
		name, enabled, url, key string
		want                    bool
	}{
		{"off", "", "http://x", "k", false},
		{"false", "false", "http://x", "k", false},
		{"no key", "true", "http://x", "", false},
		{"placeholder key", "true", "http://x", "change-me-llm-key", false},
		{"no url", "true", "", "k", false},
		{"on", "true", "http://x", "k", true},
	}

	for _, c := range cases {
		t.Setenv("AI_ENABLED", c.enabled)
		t.Setenv("LLM_API_URL", c.url)
		t.Setenv("LLM_API_KEY", c.key)

		if got := newLLM(testConfig()) != nil; got != c.want {
			t.Errorf("%s: llm built = %v, want %v", c.name, got, c.want)
		}
	}
}
