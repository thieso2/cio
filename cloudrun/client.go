package cloudrun

import (
	"context"

	run "cloud.google.com/go/run/apiv2"
	"github.com/thieso2/cio/apilog"
	"github.com/thieso2/cio/gclient"
)

// Singleton providers for each Cloud Run v2 client.
var (
	services    gclient.Provider[*run.ServicesClient]
	jobs        gclient.Provider[*run.JobsClient]
	executions  gclient.Provider[*run.ExecutionsClient]
	workerPools gclient.Provider[*run.WorkerPoolsClient]
)

// GetServicesClient returns the singleton Cloud Run v2 ServicesClient.
func GetServicesClient(ctx context.Context) (*run.ServicesClient, error) {
	return services.Get(ctx, func(ctx context.Context) (*run.ServicesClient, error) {
		apilog.Logf("[CloudRun] NewServicesClient()")
		return run.NewServicesClient(ctx)
	})
}

// GetJobsClient returns the singleton Cloud Run v2 JobsClient.
func GetJobsClient(ctx context.Context) (*run.JobsClient, error) {
	return jobs.Get(ctx, func(ctx context.Context) (*run.JobsClient, error) {
		apilog.Logf("[CloudRun] NewJobsClient()")
		return run.NewJobsClient(ctx)
	})
}

// GetExecutionsClient returns the singleton Cloud Run v2 ExecutionsClient.
func GetExecutionsClient(ctx context.Context) (*run.ExecutionsClient, error) {
	return executions.Get(ctx, func(ctx context.Context) (*run.ExecutionsClient, error) {
		apilog.Logf("[CloudRun] NewExecutionsClient()")
		return run.NewExecutionsClient(ctx)
	})
}

// GetWorkerPoolsClient returns the singleton Cloud Run v2 WorkerPoolsClient.
func GetWorkerPoolsClient(ctx context.Context) (*run.WorkerPoolsClient, error) {
	return workerPools.Get(ctx, func(ctx context.Context) (*run.WorkerPoolsClient, error) {
		apilog.Logf("[CloudRun] NewWorkerPoolsClient()")
		return run.NewWorkerPoolsClient(ctx)
	})
}

// Close closes all Cloud Run clients.
func Close() {
	_ = services.Close(func(c *run.ServicesClient) error { return c.Close() })
	_ = jobs.Close(func(c *run.JobsClient) error { return c.Close() })
	_ = executions.Close(func(c *run.ExecutionsClient) error { return c.Close() })
	_ = workerPools.Close(func(c *run.WorkerPoolsClient) error { return c.Close() })
}
