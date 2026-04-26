package payload

import (
	"fmt"
	"strings"
	"sync"
)

type Redactor interface {
	RedactArgs(args []interface{}) string
}

// DebugRedactor logs raw argument values without any redaction.
// WARNING: This redactor is UNSAFE for production use — it exposes all job
// arguments including passwords, tokens, and PII. Use only for local debugging.
type DebugRedactor struct{}

func (DebugRedactor) RedactArgs(args []interface{}) string {
	return fmt.Sprintf("%v", args)
}

// NoopRedactor is an alias for DebugRedactor retained for backward compatibility.
// Deprecated: Use MaskingRedactor (default) or FieldMaskingRedactor in production.
// This redactor exposes all job arguments in logs without any redaction.
type NoopRedactor = DebugRedactor

type MaskingRedactor struct{}

func (MaskingRedactor) RedactArgs(args []interface{}) string {
	if len(args) == 0 {
		return "[]"
	}
	return fmt.Sprintf("[REDACTED x%d]", len(args))
}

type FieldMaskingRedactor struct {
	Keys []string
}

func (f *FieldMaskingRedactor) RedactArgs(args []interface{}) string {
	parts := make([]string, len(args))
	for index, arg := range args {
		if argMap, ok := arg.(map[string]interface{}); ok {
			masked := make(map[string]interface{})
			for key, value := range argMap {
				isSensitive := false
				for _, sensitiveKey := range f.Keys {
					if strings.EqualFold(key, sensitiveKey) {
						isSensitive = true
						break
					}
				}
				if isSensitive {
					masked[key] = "[REDACTED]"
				} else {
					masked[key] = value
				}
			}
			parts[index] = fmt.Sprintf("%v", masked)
		} else {
			// Non-map arguments are redacted to prevent leaking bare string secrets.
			parts[index] = "[REDACTED non-map arg]"
		}
	}
	return fmt.Sprintf("%v", parts)
}

var DefaultRedactor Redactor = MaskingRedactor{}

var (
	defaultRedactor Redactor = DefaultRedactor
	redactorMu      sync.RWMutex
)

func SetDefaultRedactor(r Redactor) {
	redactorMu.Lock()
	defer redactorMu.Unlock()
	if r == nil {
		defaultRedactor = DefaultRedactor
		return
	}
	defaultRedactor = r
}

func GetDefaultRedactor() Redactor {
	redactorMu.RLock()
	defer redactorMu.RUnlock()
	if defaultRedactor == nil {
		return DefaultRedactor
	}
	return defaultRedactor
}
