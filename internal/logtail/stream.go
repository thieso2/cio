package logtail

import (
	"context"
	"fmt"
	"os"
	"time"

	"cloud.google.com/go/logging"
	logpb "cloud.google.com/go/logging/apiv2/loggingpb"
	"golang.org/x/oauth2/google"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/oauth"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Stream tails the given filters live via the Cloud Logging gRPC TailLogEntries
// API and invokes onEntry for each converted entry, tagged with the index of the
// filter it matched. It prints a "Streaming logs..." notice to stderr and blocks
// until ctx is cancelled (returning nil) or an unrecoverable error occurs.
//
// One filter opens a single stream with transient-error retry. Multiple filters
// open one stream each and merge their entries; this is how Dataflow tags the
// J/W/S streams it reads concurrently.
func Stream(ctx context.Context, projectID string, filters []string, onEntry func(e *logging.Entry, filterIdx int)) error {
	client, conn, err := dial(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Build one request per filter.
	reqs := make([]*logpb.TailLogEntriesRequest, len(filters))
	for i, filter := range filters {
		reqs[i] = &logpb.TailLogEntriesRequest{
			ResourceNames: []string{fmt.Sprintf("projects/%s", projectID)},
			Filter:        filter,
			BufferWindow:  durationpb.New(2 * time.Second),
		}
	}

	fmt.Fprintf(os.Stderr, "Streaming logs... (Ctrl+C to stop)\n")

	if len(reqs) == 1 {
		return streamOne(ctx, client, reqs[0], func(e *logging.Entry) { onEntry(e, 0) })
	}
	return streamMany(ctx, client, reqs, onEntry)
}

// dial creates an authenticated gRPC client for the Logging v2 API.
func dial(ctx context.Context) (logpb.LoggingServiceV2Client, *grpc.ClientConn, error) {
	tokenSource, err := google.DefaultTokenSource(ctx, "https://www.googleapis.com/auth/logging.read")
	if err != nil {
		return nil, nil, fmt.Errorf("getting credentials: %w", err)
	}
	conn, err := grpc.NewClient(
		"logging.googleapis.com:443",
		grpc.WithTransportCredentials(credentials.NewClientTLSFromCert(nil, "")),
		grpc.WithPerRPCCredentials(oauth.TokenSource{TokenSource: tokenSource}),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("creating gRPC connection: %w", err)
	}
	return logpb.NewLoggingServiceV2Client(conn), conn, nil
}

// streamOne reads a single tail stream, reconnecting on transient errors.
func streamOne(ctx context.Context, client logpb.LoggingServiceV2Client, req *logpb.TailLogEntriesRequest, onEntry func(*logging.Entry)) error {
	stream, err := client.TailLogEntries(ctx)
	if err != nil {
		return fmt.Errorf("creating tail stream: %w", err)
	}
	if err := stream.Send(req); err != nil {
		return fmt.Errorf("sending tail request: %w", err)
	}

	const maxRetries = 2
	for {
		resp, err := stream.Recv()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			// Retry on transient errors (connection reset, unavailable, etc.)
			reconnected := false
			for attempt := 1; attempt <= maxRetries; attempt++ {
				time.Sleep(time.Duration(attempt) * time.Second)
				if ctx.Err() != nil {
					return nil
				}
				newStream, sendErr := client.TailLogEntries(ctx)
				if sendErr != nil {
					continue
				}
				if sendErr = newStream.Send(req); sendErr != nil {
					continue
				}
				stream = newStream
				reconnected = true
				break
			}
			if !reconnected {
				return fmt.Errorf("receiving log entries: %w", err)
			}
			continue
		}
		for _, entry := range resp.Entries {
			onEntry(ProtoToEntry(entry))
		}
	}
}

// streamMany reads one stream per request in its own goroutine and merges their
// entries through a channel, preserving each entry's filter index.
func streamMany(ctx context.Context, client logpb.LoggingServiceV2Client, reqs []*logpb.TailLogEntriesRequest, onEntry func(e *logging.Entry, filterIdx int)) error {
	type taggedProto struct {
		entry *logpb.LogEntry
		idx   int
	}
	ch := make(chan taggedProto, 100)
	errCh := make(chan error, len(reqs))

	for i, req := range reqs {
		stream, err := client.TailLogEntries(ctx)
		if err != nil {
			return fmt.Errorf("creating tail stream: %w", err)
		}
		if err := stream.Send(req); err != nil {
			return fmt.Errorf("sending tail request: %w", err)
		}
		go func(idx int, s logpb.LoggingServiceV2_TailLogEntriesClient) {
			for {
				resp, err := s.Recv()
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					errCh <- fmt.Errorf("receiving log entries: %w", err)
					return
				}
				for _, entry := range resp.Entries {
					ch <- taggedProto{entry: entry, idx: idx}
				}
			}
		}(i, stream)
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-errCh:
			return err
		case tp := <-ch:
			onEntry(ProtoToEntry(tp.entry), tp.idx)
		}
	}
}
