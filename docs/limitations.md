# Known limitations

- **Config reloads**: FrankenPHP's extension worker registration does not
  survive a Caddy config reload; see [UPSTREAM-ISSUES.md](UPSTREAM-ISSUES.md).
  Restart FrankenPHP to apply a new configuration.
- A connection is closed with 1001 when its configuration is replaced.
