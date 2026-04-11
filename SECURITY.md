# Security

This document covers security concerns and mitigations you should be aware of when using Crank in production.

## Broker credentials

Broker URLs often contain passwords (e.g. `redis://:secret@host:6379/0`). Protect them accordingly:

- Pass URLs via environment variables (`REDIS_URL`, `NATS_URL`) rather than checking them into config files.
- Restrict file permissions on any config file that contains a broker URL.
- Crank does **not** redact credentials from error messages. Do not expose SDK errors to end users.

## Encrypt connections with TLS

Unencrypted broker connections expose job data and credentials on the wire.

- Use `rediss://` URLs or `WithTLS(true)` to enable TLS. Crank enforces TLS 1.2 minimum.
- **Never** use `WithTLSInsecureSkipVerify(true)` in production — it disables certificate verification and exposes you to man-in-the-middle attacks. It exists only for local development with self-signed certs.

## Redact sensitive job arguments

When a job fails, Crank logs its arguments. If those arguments contain passwords, tokens, or PII, they will appear in your logs unless you configure a redactor.

The default `MaskingRedactor` replaces all arguments with `[REDACTED]`. If you need to see non-sensitive args, use `FieldMaskingRedactor` to mask only specific keys:

```go
crank.SetRedactor(crank.NewFieldMaskingRedactor([]string{"password", "token", "ssn"}))
```

**Do not** use `NoopRedactor` unless you are certain no job will ever carry sensitive data.

## Validate jobs before execution

Without validation, any job class name and payload can be enqueued and executed. In shared or multi-tenant environments this is a risk.

Use `SetValidator()` to restrict what the engine will accept. Validation runs before every job execution:

```go
crank.SetValidator(crank.ChainValidator{
    crank.SafeClassPattern(),           // only [A-Za-z0-9_]
    crank.MaxArgsCount(20),
    crank.MaxPayloadSize(1 << 20),      // 1 MB
})
```

- `ClassAllowlist` or `ClassPattern` prevents execution of unexpected job classes.
- `MaxArgsCount` and `MaxPayloadSize` prevent oversized payloads from consuming memory.

## Do not use user input as queue names

Queue names become Redis keys (e.g. `queue:<name>`). Accepting user-controlled queue names can lead to key-space pollution, very long keys, or injection of special characters. Use fixed, hardcoded queue names.

## Protect config file paths

If you use `QuickStart(configPath)`, do not pass untrusted user input as the path. Crank rejects `..` traversals and resolves symlinks, but the safest approach is to hardcode or tightly control the config path.

YAML config files are parsed as trusted input. Never load config from an untrusted source.

## Custom broker responsibilities

When providing a custom broker via `WithCustomBroker()`, you are responsible for its security properties — TLS, authentication, connection timeouts, and access control. Crank cannot enforce these on an implementation it does not control.

## Do not use the test engine in production

`NewTestEngine()` uses an in-memory broker with no authentication, persistence, or encryption. It is intended for tests only.

## Reporting vulnerabilities

If you discover a security issue, please report it privately rather than in a public issue.

**Maintainer:** ogwurujohnson@gmail.com
