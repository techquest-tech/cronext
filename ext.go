package cronext

import (
	"sync"
	"time"

	"github.com/asaskevich/EventBus"
	"github.com/go-logr/zapr"
	"github.com/robfig/cron/v3"
	"go.uber.org/zap"
)

const (
	// EventJobStarted  = "event.job.started"
	// EventJobDone     = "event.job.done"
	// EventJobFailed   = "event.job.failed"
	EventJobFinished = "event.job.finished"
)

type Job struct {
	Job       string
	Scheduler string
	Logger    *zap.Logger
	Hs        JobHistoryService
	cr        *cron.Cron
	Bus       EventBus.Bus
}

type JobHistory struct {
	Job      string
	Start    time.Time
	Finished time.Time
	Duration time.Duration
	Succeed  bool
	Message  string
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
	if job.Succeed {
		a.cache.Store(job.Job, job.Finished)
		a.Logger.Debug("updated lastRuntime", zap.String("job", job.Job), zap.Time("time", job.Finished))
	} else {
		a.Logger.Debug("Job failed. cache won't be updated.", zap.String("job", job.Job), zap.Time("time", job.Finished))
	}

	return nil
}

func (a *RamHistoryService) GetLastRuntime(job string) (time.Time, error) {
	if ojb, ok := a.cache.Load(job); ok {
		last := ojb.(time.Time)
		return last, nil
	}
	return time.Time{}, nil
}

func (jb *Job) PrintNextRuntime(msg string) {
	if jb.cr == nil {
		return
	}
	entries := jb.cr.Entries()
	nextRuntime := entries[0].Next

	jb.Logger.Info(msg+" next run time", zap.String("job", jb.Job), zap.Time("next", nextRuntime))
}

func (jb *Job) Schedule(cmd func()) error {
	if jb.Hs == nil {
		jb.Hs = &RamHistoryService{Logger: jb.Logger}
	}

	log := zapr.NewLogger(jb.Logger)

	cr := cron.New(cron.WithChain(cron.SkipIfStillRunning(log), jb.WithHistory()))
	_, err := cr.AddFunc(jb.Scheduler, cmd)
	if err != nil {
		jb.Logger.Error("schedule job failed.", zap.Error(err))
		return err
	}

	jb.cr = cr

	cr.Start()

	jb.PrintNextRuntime("job scheduled.")

	return nil
}

func (jb *Job) WithHistory() cron.JobWrapper {
	return func(j cron.Job) cron.Job {
		return cron.FuncJob(
			func() {
				jb.Logger.Debug("mark job started", zap.String("job", jb.Job))
				task := JobHistory{
					Job:     jb.Job,
					Start:   time.Now(),
					Succeed: true,
				}
				defer func() {
					if r := recover(); r != nil {
						if err, ok := r.(error); ok {
							task.Message = err.Error()
							if jb.Bus != nil {
								jb.Bus.Publish("event.error", err)
							}
						}
						task.Succeed = false
						jb.Logger.Error("recover from panic", zap.Any("panic", r), zap.String("job", task.Job))
					}

					jb.PrintNextRuntime("job finshed.")

					done := time.Now()
					task.Duration = time.Since(task.Start)
					task.Finished = done
					jb.Logger.Debug("mark job end", zap.String("job", jb.Job), zap.Duration("duration", task.Duration))
					if jb.Bus != nil {
						jb.Bus.Publish(EventJobFinished, task)
					}
					// if jobHistory != nil {
					go jb.Hs.ToHistory(task)
					// }
				}()

				j.Run()
			})
	}
}
