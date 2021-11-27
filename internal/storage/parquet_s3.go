package storage

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/xitongsys/parquet-go-source/s3v2"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"
)

type DateHourKey struct {
	Date string
	Hour int
}

type Rotatable interface {
	GetDateHourKey() DateHourKey
	UpdateFields() error
}

type ParquetS3FileWriter struct {
	api             s3v2.S3API
	bucket          string
	prefix          string
	writer          *writer.ParquetWriter
	s3File          source.ParquetFile
	lock            sync.Mutex
	timeOpened      time.Time
	timeOfLastWrite time.Time
	currentSize     int64
	sampleObj       interface{}
	key             DateHourKey
}

func NewParquetS3FileWriter(api s3v2.S3API, bucket string, prefix string, key DateHourKey, sampleObj interface{}) (*ParquetS3FileWriter, error) {
	// create new writers
	filename := fmt.Sprintf("%s/event_date=%s/event_hour=%v/%s.parquet", prefix, key.Date, key.Hour, uuid.New().String())
	s3File, err := s3v2.NewS3FileWriterWithClient(context.TODO(), api, bucket, filename, nil)
	if err != nil {
		return nil, err
	}

	writer, err := writer.NewParquetWriter(s3File, sampleObj, 4)
	if err != nil {
		return nil, err
	}

	return &ParquetS3FileWriter{
		api:             api,
		bucket:          bucket,
		prefix:          prefix,
		s3File:          s3File,
		writer:          writer,
		timeOpened:      time.Now(),
		timeOfLastWrite: time.Now(),
		currentSize:     0,
		sampleObj:       sampleObj,
		key:             key,
	}, nil
}

func (w *ParquetS3FileWriter) Write(obj interface{}) error {
	w.lock.Lock()
	defer w.lock.Unlock()

	err := w.writer.Write(obj)
	if err != nil {
		return err
	}

	w.timeOfLastWrite = time.Now()
	w.currentSize += w.writer.ObjSize

	return nil
}

func (w *ParquetS3FileWriter) Close() error {
	w.lock.Lock()
	defer w.lock.Unlock()

	w.close()

	return nil
}

// close function without lock to avoid deadlock
func (w *ParquetS3FileWriter) close() error {
	err := w.writer.WriteStop()
	if err != nil {
		return err
	}
	err = w.s3File.Close()
	if err != nil {
		return err
	}

	logrus.WithFields(logrus.Fields{
		"prefix": w.prefix,
		"date":   w.key.Date,
		"hour":   w.key.Hour,
	}).Info("closed S3 parquet file")

	return nil
}

func (w *ParquetS3FileWriter) RotateFile() error {
	w.lock.Lock()
	defer w.lock.Unlock()

	err := w.close()
	if err != nil {
		return err
	}

	filename := fmt.Sprintf("%s/event_date=%s/event_hour=%v/%s.parquet", w.prefix, w.key.Date, w.key.Hour, uuid.New().String())
	s3File, err := s3v2.NewS3FileWriterWithClient(context.TODO(), w.api, w.bucket, filename, nil)
	if err != nil {
		return err
	}

	writer, err := writer.NewParquetWriter(s3File, w.sampleObj, 4)
	if err != nil {
		return err
	}

	w.s3File = s3File
	w.writer = writer

	w.timeOpened = time.Now()
	w.timeOfLastWrite = time.Now()
	w.currentSize = 0

	logrus.WithFields(logrus.Fields{
		"new_filename": filename,
		"prefix":       w.prefix,
		"date":         w.key.Date,
		"hour":         w.key.Hour,
	}).Info("rotated S3 parquet file")

	return nil
}

type RotatingWriter struct {
	openWriters        map[DateHourKey]*ParquetS3FileWriter
	lock               sync.Mutex
	api                s3v2.S3API
	bucket             string
	prefix             string
	fileTimeoutMinutes int
	fileMaxAgeMinutes  int
	fileMaxSizeBytes   int64
}

func NewRotatingWriter(api s3v2.S3API, bucket string, prefix string, fileTimeoutMinutes, fileMaxAgeMinutes int, fileMaxSizeBytes int64) *RotatingWriter {
	writer := &RotatingWriter{
		openWriters:        make(map[DateHourKey]*ParquetS3FileWriter),
		api:                api,
		bucket:             bucket,
		prefix:             prefix,
		fileTimeoutMinutes: fileTimeoutMinutes,
		fileMaxAgeMinutes:  fileMaxAgeMinutes,
		fileMaxSizeBytes:   fileMaxSizeBytes,
	}

	// schedule cleaning of file writers
	go func() {
		ticker := time.NewTicker(time.Minute)
		for range ticker.C {
			err := writer.CleanFiles()
			if err != nil {
				logrus.WithFields(logrus.Fields{
					"error":  err,
					"prefix": writer.prefix,
				}).Error("error cleaning S3 parquet files")
			}
		}
	}()

	return writer
}

func (r *RotatingWriter) Write(obj Rotatable) error {
	if obj == nil {
		return fmt.Errorf("obj is nil")
	}

	r.lock.Lock()
	defer r.lock.Unlock()

	key := obj.GetDateHourKey()
	if _, ok := r.openWriters[key]; !ok {
		writer, err := NewParquetS3FileWriter(r.api, r.bucket, r.prefix, key, obj)
		if err != nil {
			return err
		}
		r.openWriters[key] = writer
	}

	if r.openWriters[key].currentSize > r.fileMaxSizeBytes {
		err := r.openWriters[key].RotateFile()
		if err != nil {
			return err
		}
		logrus.WithFields(logrus.Fields{
			"prefix": r.prefix,
			"key":    key,
		}).Info("rotated S3 parquet file due to max filesize reached")
	}

	return r.openWriters[key].Write(obj)
}

func (r *RotatingWriter) CleanFiles() error {
	r.lock.Lock()
	defer r.lock.Unlock()

	for key, writer := range r.openWriters {
		if time.Since(writer.timeOfLastWrite) > time.Minute*time.Duration(r.fileTimeoutMinutes) {
			err := writer.Close()
			if err != nil {
				return err
			}
			delete(r.openWriters, key)
			logrus.WithFields(logrus.Fields{
				"prefix": writer.prefix,
				"key":    key,
			}).Info("closed S3 parquet file due to time-of-last-write")
		} else if time.Since(writer.timeOpened) > time.Minute*time.Duration(r.fileMaxAgeMinutes) {
			err := writer.Close()
			if err != nil {
				return err
			}
			delete(r.openWriters, key)
			logrus.WithFields(logrus.Fields{
				"prefix": writer.prefix,
				"key":    key,
			}).Info("closed S3 parquet file due to time-since-opened")
		}
	}

	return nil
}

func (r *RotatingWriter) Close() error {
	r.lock.Lock()
	defer r.lock.Unlock()

	for _, writer := range r.openWriters {
		err := writer.Close()
		if err != nil {
			return err
		}
	}

	return nil
}
