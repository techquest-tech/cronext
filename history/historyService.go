package history

import (
	"errors"
	"time"

	"github.com/spf13/viper"
	ext "github.com/techquest-tech/cronext"
	"github.com/techquest-tech/gin-shared/pkg/ginshared"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

func InitHistoryService(db *gorm.DB, logger *zap.Logger, settingKey string) (ext.JobHistory, error) {
	service := &HistoryService{
		DB:     db,
		Logger: logger,
	}
	if settingKey != "" {
		settings := viper.Sub(settingKey)
		if settings != nil {
			settings.Unmarshal(service)
		}
	}
	if viper.GetBool(ginshared.KeyInitDB) {
		service.DB.AutoMigrate(&JobDetails{})
		logger.Info("Job Details table created or updated done")
	}

	return service, nil
}

type JobDetails struct {
	gorm.Model
	ext.Job
}

type HistoryService struct {
	DB     *gorm.DB
	Logger *zap.Logger
	cache  map[string]time.Time
}

func (hs *HistoryService) ToHistory(job ext.Job) error {
	// n :=time.Now()
	defer func() {
		hs.cache[job.Job] = job.Finished
		hs.Logger.Debug("updated lastRuntime", zap.String("job", job.Job))
	}()

	entity := JobDetails{
		Job: job,
	}
	err := hs.DB.Save(&entity).Error
	if err != nil {
		hs.Logger.Error("save history failed.", zap.Error(err))
		return err
	}
	hs.Logger.Info("save history done.")

	return nil
}

func (hs *HistoryService) GetLastRuntime(job string) (time.Time, error) {
	if l, ok := hs.cache[job]; ok {
		return l, nil
	}

	lastJob := &JobDetails{}

	err := hs.DB.First(lastJob, " job = ?", job).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			hs.Logger.Debug("job should not run before, return 0")
			return time.Time{}, nil
		}
		hs.Logger.Error("query last job failed.", zap.Error(err))
		return time.Time{}, err
	}

	hs.cache[job] = lastJob.Finished
	hs.Logger.Debug("updated last time", zap.Time("job last run time", lastJob.Finished), zap.String("job", job))

	return lastJob.Finished, nil
}
