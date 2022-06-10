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
		// cache: &ext.RamHistoryService{
		// 	Logger: logger,
		// },
	}
	// if settingKey != "" {
	// 	settings := viper.Sub(settingKey)
	// 	if settings != nil {
	// 		settings.Unmarshal(service)
	// 	}
	// }
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
	// cache  *ext.RamHistoryService
}

func (hs *OrmJobHistoryService) ToHistory(job ext.JobHistory) error {
	// defer func() {
	// 	hs.cache.ToHistory(job)
	// }()

	entity := CronjobHistory{
		JobHistory: job,
	}
	err := hs.DB.Save(&entity).Error
	if err != nil {
		hs.Logger.Error("save history failed.", zap.Error(err))
		return err
	}
	hs.Logger.Info("save history done.")

	return nil
}

func (hs *OrmJobHistoryService) GetLastRuntime(job string) (time.Time, error) {
	// result, err := hs.cache.GetLastRuntime(job)
	// if err == nil && !result.IsZero() {
	// 	return result, nil
	// }

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
