# API Stability

Bunshin exposes a Go-native API from the `github.com/xargin/bunshin` package. The public surface is organized around typed configs, constructor functions, and sentinel errors that work with `errors.Is`.

## Stable Entry Points

The API contract test covers the main exported constructors and config types:

- publication and subscription constructors;
- embeddable and external media-driver entry points;
- IPC ring and driver IPC constructors;
- archive, archive control, replay merge, and replication entry points;
- cluster node, learner, replication, election, and backup entry points;
- idle strategy, flow-control, driver threading, and channel URI helpers.

The test also references the exported sentinel errors so accidental renames or removals fail at compile time.

## Compatibility Policy

Within the Bunshin-native API, prefer additive changes. Existing exported constructors, config fields, and sentinel errors should remain source-compatible unless the project intentionally makes a major-version break.

Bunshin does not promise Aeron Java/C/C++ API compatibility. The compatibility boundary is documented in `docs/compatibility.md`.
