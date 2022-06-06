package cronext

import (
	"github.com/go-logr/zapr"
	"github.com/robfig/cron/v3"
	"go.uber.org/zap"
)

func CreateSchedule(jobname, schedule string, cmd func(), logger *zap.Logger) (*cron.Cron, error) {
	l := zapr.NewLogger(logger)

	cr := cron.New(cron.WithChain(cron.Recover(l), cron.SkipIfStillRunning(l)))
	_, err := cr.AddFunc(schedule, cmd)
	if err != nil {
		return nil, err
	}
	cr.Start()

	entries := cr.Entries()
	nextRuntime := entries[0].Next

	logger.Debug("cron job scheduled", zap.String("schedule", schedule), zap.Time("next", nextRuntime))

	return cr, nil
}
