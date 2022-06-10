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

func InitHistoryService(db *gorm.DB, logger *zap.Logger) (ext.JobHistoryService, error) {
	service := &OrmJobHistoryService{
		DB:     db,
		Logger: logger,
	}
	if viper.GetBool(ginshared.KeyInitDB) {
		service.DB.AutoMigrate(&CronjobHistory{})
		logger.Info("Job Details table created or updated done")
	}

	return service, nil
}

type CronjobHistory struct {
	gorm.Model
	ext.JobHistory
}

type OrmJobHistoryService struct {
	DB     *gorm.DB
	Logger *zap.Logger
}

func (hs *OrmJobHistoryService) ToHistory(job ext.JobHistory) error {
	entity := CronjobHistory{
		JobHistory: job,
	}
	err := hs.DB.Save(&entity).Error
	if err != nil {
		hs.Logger.Error("save history failed.", zap.Error(err))
		return err
	}
	hs.Logger.Debug("save history done.")

	return nil
}

func (hs *OrmJobHistoryService) GetLastRuntime(job string) (time.Time, error) {
	lastJob := &CronjobHistory{}

	err := hs.DB.First(lastJob, " job = ?", job).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			hs.Logger.Debug("job should not run before, return 0")
			return time.Time{}, nil
		}
		hs.Logger.Error("query last job failed.", zap.Error(err))
		return time.Time{}, err
	}

	return lastJob.Finished, nil
}
