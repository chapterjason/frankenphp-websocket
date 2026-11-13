# Upstream issues

## FrankenPHP: extension workers break on config reload

Found with FrankenPHP v1.13.5 and Caddy v2.11.9.

`caddy.RegisterWorkers()` appends to a process-wide options list in
FrankenPHP's `caddy/app.go`. Only `FrankenPHPApp.Stop()` clears that list,
and Caddy stops the old config only after the new one has started.

- Registering in `Provision` (documented for configurable extensions): the
  first reload fails with `two global workers cannot have the same filename`,
  because the old registration is still in the list. The failed reload never
  stops the old app, so every later reload fails the same way.
- Registering once in `init()` (documented static registration): the first
  reload works, then the old app's `Stop()` clears the list, and after the
  second reload the worker is gone (`server is not registered`).

Worker lifecycle across reloads is FrankenPHP's responsibility, not this
extension's. The extension registers in `Provision` as documented and does not
work around this bug.
