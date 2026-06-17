// Package logtail is the one engine for reading Google Cloud Logging entries:
// fetching recent history, streaming live via gRPC, converting protobuf entries,
// and the default label-prefixed formatter. Cloud Run, VM, and Dataflow tailing
// all sit on top of it — they each build their own filter strings and (for
// Dataflow) their own formatter, but the fetch/stream/convert machinery lives
// here once instead of being copied per resource type.
package logtail

import (
	"net/http"
	"net/url"

	"cloud.google.com/go/logging"
	logpb "cloud.google.com/go/logging/apiv2/loggingpb"
)

// ProtoToEntry converts a protobuf LogEntry (from the gRPC tail stream) into the
// higher-level logging.Entry used by the formatters. It copies every field any
// caller needs — text/json/proto payloads, HTTP request, resource, labels — so
// it can back both Cloud Run and Dataflow without per-package variants.
func ProtoToEntry(entry *logpb.LogEntry) *logging.Entry {
	var payload interface{}
	switch p := entry.Payload.(type) {
	case *logpb.LogEntry_TextPayload:
		payload = p.TextPayload
	case *logpb.LogEntry_JsonPayload:
		if p.JsonPayload != nil {
			payload = p.JsonPayload.AsMap()
		}
	case *logpb.LogEntry_ProtoPayload:
		payload = p.ProtoPayload
	}

	var httpRequest *logging.HTTPRequest
	if req := entry.GetHttpRequest(); req != nil {
		httpRequest = &logging.HTTPRequest{
			Status: int(req.GetStatus()),
		}
		if req.GetRequestMethod() != "" || req.GetRequestUrl() != "" {
			httpRequest.Request = &http.Request{Method: req.GetRequestMethod()}
			if req.GetRequestUrl() != "" {
				if u, err := url.Parse(req.GetRequestUrl()); err == nil {
					httpRequest.Request.URL = u
				}
			}
		}
		if req.GetLatency() != nil {
			httpRequest.Latency = req.GetLatency().AsDuration()
		}
	}

	e := &logging.Entry{
		Timestamp:   entry.Timestamp.AsTime(),
		Severity:    logging.Severity(entry.Severity),
		Payload:     payload,
		HTTPRequest: httpRequest,
		Labels:      entry.Labels,
	}
	if entry.Resource != nil {
		e.Resource = entry.Resource
	}
	return e
}
