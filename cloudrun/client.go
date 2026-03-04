package cloudrun

import (
	"context"
	"sync"

	run "cloud.google.com/go/run/apiv2"
	"github.com/thieso2/cio/apilog"
)

var (
	servicesOnce   sync.Once
	servicesClient *run.ServicesClient
	servicesErr    error

	jobsOnce   sync.Once
	jobsClient *run.JobsClient
	jobsErr    error

	executionsOnce   sync.Once
	executionsClient *run.ExecutionsClient
	executionsErr    error

	workerPoolsOnce   sync.Once
	workerPoolsClient *run.WorkerPoolsClient
	workerPoolsErr    error
)

// GetServicesClient returns the singleton Cloud Run v2 ServicesClient.
func GetServicesClient(ctx context.Context) (*run.ServicesClient, error) {
	servicesOnce.Do(func() {
		apilog.Logf("[CloudRun] NewServicesClient()")
		servicesClient, servicesErr = run.NewServicesClient(ctx)
	})
	return servicesClient, servicesErr
}

// GetJobsClient returns the singleton Cloud Run v2 JobsClient.
func GetJobsClient(ctx context.Context) (*run.JobsClient, error) {
	jobsOnce.Do(func() {
		apilog.Logf("[CloudRun] NewJobsClient()")
		jobsClient, jobsErr = run.NewJobsClient(ctx)
	})
	return jobsClient, jobsErr
}

// GetExecutionsClient returns the singleton Cloud Run v2 ExecutionsClient.
func GetExecutionsClient(ctx context.Context) (*run.ExecutionsClient, error) {
	executionsOnce.Do(func() {
		apilog.Logf("[CloudRun] NewExecutionsClient()")
		executionsClient, executionsErr = run.NewExecutionsClient(ctx)
	})
	return executionsClient, executionsErr
}

// GetWorkerPoolsClient returns the singleton Cloud Run v2 WorkerPoolsClient.
func GetWorkerPoolsClient(ctx context.Context) (*run.WorkerPoolsClient, error) {
	workerPoolsOnce.Do(func() {
		apilog.Logf("[CloudRun] NewWorkerPoolsClient()")
		workerPoolsClient, workerPoolsErr = run.NewWorkerPoolsClient(ctx)
	})
	return workerPoolsClient, workerPoolsErr
}

// Close closes all Cloud Run clients.
func Close() {
	if servicesClient != nil {
		servicesClient.Close()
	}
	if jobsClient != nil {
		jobsClient.Close()
	}
	if executionsClient != nil {
		executionsClient.Close()
	}
	if workerPoolsClient != nil {
		workerPoolsClient.Close()
	}
}
