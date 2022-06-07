package cronext

import (
	"time"

	"github.com/robfig/cron/v3"
)

type Job struct {
	Job      string
	Start    time.Time
	Finished time.Time
	Duration time.Duration
}

type JobHistory interface {
	ToHistory(job Job) error
	GetLastRuntime(job string) (time.Time, error)
}

func WithHistory(logger cron.Logger, job string, jobHistory JobHistory) cron.JobWrapper {
	return func(j cron.Job) cron.Job {
		return cron.FuncJob(
			func() {
				task := Job{
					Job:   job,
					Start: time.Now(),
				}
				defer func() {
					done := time.Now()
					task.Duration = time.Since(task.Start)
					task.Finished = done
					if jobHistory != nil {
						jobHistory.ToHistory(task)
					}
				}()
				j.Run()
			})
	}
}
