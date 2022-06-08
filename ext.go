package cronext

import (
	"sync"
	"time"

	"github.com/go-logr/zapr"
	"github.com/robfig/cron/v3"
	"go.uber.org/zap"
)

type Job struct {
	Job       string
	Scheduler string
	Logger    *zap.Logger
	Hs        JobHistoryService
	cr        *cron.Cron
}

type JobHistory struct {
	Job      string
	Start    time.Time
	Finished time.Time
	Duration time.Duration
}

type JobHistoryService interface {
	ToHistory(job JobHistory) error
	GetLastRuntime(job string) (time.Time, error)
}

type RamHistoryService struct {
	Logger *zap.Logger
	cache  sync.Map
}

func (a *RamHistoryService) ToHistory(job JobHistory) error {
	a.cache.Store(job.Job, job.Finished)
	a.Logger.Debug("updated lastRuntime", zap.String("job", job.Job), zap.Time("time", job.Finished))
	return nil
}

func (a *RamHistoryService) GetLastRuntime(job string) (time.Time, error) {
	if ojb, ok := a.cache.Load(job); ok {
		last := ojb.(time.Time)
		return last, nil
	}
	return time.Time{}, nil
}

func (jb *Job) PrintNextRuntime() {
	if jb.cr == nil {
		return
	}
	entries := jb.cr.Entries()
	nextRuntime := entries[0].Next

	jb.Logger.Info("next run time", zap.String("job", jb.Job), zap.Time("next", nextRuntime))
}

func (jb *Job) Schedule(cmd func()) error {
	if jb.Hs == nil {
		jb.Hs = &RamHistoryService{Logger: jb.Logger}
	}

	log := zapr.NewLogger(jb.Logger)

	cr := cron.New(cron.WithChain(cron.Recover(log), cron.SkipIfStillRunning(log), jb.WithHistory()))
	_, err := cr.AddFunc(jb.Scheduler, cmd)
	if err != nil {
		jb.Logger.Error("schedule job failed.", zap.Error(err))
		return err
	}

	jb.cr = cr

	cr.Start()

	jb.PrintNextRuntime()

	return nil
}

func (jb *Job) WithHistory() cron.JobWrapper {
	return func(j cron.Job) cron.Job {
		return cron.FuncJob(
			func() {
				task := JobHistory{
					Job:   jb.Job,
					Start: time.Now(),
				}
				defer func() {
					done := time.Now()
					task.Duration = time.Since(task.Start)
					task.Finished = done
					// if jobHistory != nil {
					jb.Hs.ToHistory(task)
					// }
				}()
				j.Run()

				jb.PrintNextRuntime()
			})
	}
}
