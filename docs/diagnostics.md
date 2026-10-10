# Diagnostics

Connection lifecycle logs are enabled by default and written as JSON to stderr. Follow `trace_id` through admission, upgrades, authentication, first-message timing, pairing, and closure. `pair_tag` connects the client's and host's separate requests; `endpoint_tag` connects requests to registration events. Close summaries include durations, close codes, and completed message and byte counts. Payloads, raw identifiers, credentials, and remote close-message contents are excluded.

To investigate a host, read its `topHosts[].traceTag` from private relay metrics, or run `supacode-relay trace-tag ENDPOINT_ID` with its full endpoint ID. Filter the relevant service's journal using that tag:

```sh
journalctl -u supacode-relay --since "10 minutes ago" -o cat |
  jq -R 'fromjson? | select(.endpoint_tag == "HOST_TRACE_TAG")'
```

An `endpoint not found` rejection identifies a host that is offline; a pending pair closed for `pair timeout` identifies a host that never accepted, for example because it was at capacity; an active pair's peer failure and close summary identify where forwarding ended. These logs observe the outer relay connection. The encrypted application's own HTTP responses and errors remain opaque.
