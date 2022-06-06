package cronext

import "time"

type Job struct {
	Project  string
	Job      string
	Start    time.Time
	Finished time.Time
	Duration time.Duration
	Message  string
}

type JobHistory interface {
	ToHistory(job Job) error
	GetLastRuntime(project, job string) (time.Time, error)
}
