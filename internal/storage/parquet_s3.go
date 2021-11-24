package storage

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/xitongsys/parquet-go-source/s3v2"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"
)

type DateHourKey struct {
	Date string
	Hour string
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
	filename := fmt.Sprintf("%s/event_date=%s/event_hour=%s/%s.parquet", prefix, key.Date, key.Hour, uuid.New().String())
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
	w.currentSize += w.writer.Size

	return nil
}

func (w *ParquetS3FileWriter) Close() error {
	w.lock.Lock()
	defer w.lock.Unlock()

	err := w.writer.WriteStop()
	if err != nil {
		return err
	}
	err = w.s3File.Close()
	if err != nil {
		return err
	}

	fmt.Println("Closed file ", w.prefix, w.key.Date, w.key.Hour)

	return nil
}

func (w *ParquetS3FileWriter) RotateFile() error {
	w.lock.Lock()
	defer w.lock.Unlock()

	err := w.Close()
	if err != nil {
		return err
	}

	filename := fmt.Sprintf("%s/event_date=%s/event_hour=%s/%s.parquet", w.prefix, w.key.Date, w.key.Hour, uuid.New().String())
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

	fmt.Println("rotated file ", filename)

	return nil
}

type RotatingWriter struct {
	openWriters map[DateHourKey]*ParquetS3FileWriter
	lock        sync.Mutex
	api         s3v2.S3API
	bucket      string
	prefix      string
}

func NewRotatingWriter(api s3v2.S3API, bucket string, prefix string) *RotatingWriter {
	writer := &RotatingWriter{
		openWriters: make(map[DateHourKey]*ParquetS3FileWriter),
		api:         api,
		bucket:      bucket,
		prefix:      prefix,
	}

	// schedule cleaning of file writers
	go func() {
		ticker := time.NewTicker(time.Minute)
		for range ticker.C {
			err := writer.CleanFiles()
			if err != nil {
				fmt.Printf("Error cleaning files: %s\n", err.Error())
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

	return r.openWriters[key].Write(obj)
}

func (r *RotatingWriter) CleanFiles() error {
	fmt.Println("cleaning files")
	r.lock.Lock()
	defer r.lock.Unlock()

	for key, writer := range r.openWriters {
		if time.Since(writer.timeOfLastWrite) > time.Minute*5 {
			err := writer.Close()
			if err != nil {
				return err
			}
			delete(r.openWriters, key)
		} else if time.Since(writer.timeOpened) > time.Minute*15 {
			err := writer.Close()
			if err != nil {
				return err
			}
			delete(r.openWriters, key)
		}
	}
	fmt.Println("cleaned files")

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
