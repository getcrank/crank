package payload

import (
	"fmt"
	"regexp"
	"sync"
)

// validQueueName restricts queue names to alphanumeric, underscore, and hyphen, 1-128 chars.
var validQueueName = regexp.MustCompile(`^[A-Za-z0-9_\-]{1,128}$`)

// ValidateQueueName returns an error if the queue name contains invalid characters
// or exceeds the maximum length. Queue names must match [A-Za-z0-9_-]{1,128}.
func ValidateQueueName(name string) error {
	if !validQueueName.MatchString(name) {
		return fmt.Errorf("invalid queue name %q: must match [A-Za-z0-9_-]{1,128}", name)
	}
	return nil
}

// ValidateWorkerClass returns an error if the worker class is empty.
func ValidateWorkerClass(class string) error {
	if class == "" {
		return fmt.Errorf("worker class must not be empty")
	}
	return nil
}

type Validator interface {
	Validate(job *Job) error
}

type ValidatorFunc func(job *Job) error

func (f ValidatorFunc) Validate(job *Job) error {
	return f(job)
}

func MaxArgsCount(maxArgs int) Validator {
	return ValidatorFunc(func(job *Job) error {
		if len(job.Args) > maxArgs {
			return fmt.Errorf("job args count %d exceeds max %d", len(job.Args), maxArgs)
		}
		return nil
	})
}

func ClassAllowlist(classes map[string]bool) Validator {
	return ValidatorFunc(func(job *Job) error {
		if !classes[job.Class] {
			return fmt.Errorf("job class '%s' not in allowlist", job.Class)
		}
		return nil
	})
}

func ClassPattern(pattern *regexp.Regexp) Validator {
	return ValidatorFunc(func(job *Job) error {
		if !pattern.MatchString(job.Class) {
			return fmt.Errorf("job class '%s' does not match allowed pattern", job.Class)
		}
		return nil
	})
}

func MaxPayloadSize(maxBytes int) Validator {
	return ValidatorFunc(func(job *Job) error {
		data, err := job.ToJSON()
		if err != nil {
			return err
		}
		if len(data) > maxBytes {
			return fmt.Errorf("job payload size %d exceeds max %d bytes", len(data), maxBytes)
		}
		return nil
	})
}

type ChainValidator []Validator

func (c ChainValidator) Validate(job *Job) error {
	for _, validator := range c {
		if err := validator.Validate(job); err != nil {
			return err
		}
	}
	return nil
}

var (
	defaultValidator Validator
	validatorMu      sync.RWMutex
)

func SetDefaultValidator(validator Validator) {
	validatorMu.Lock()
	defer validatorMu.Unlock()
	defaultValidator = validator
}

func GetDefaultValidator() Validator {
	validatorMu.RLock()
	defer validatorMu.RUnlock()
	return defaultValidator
}
