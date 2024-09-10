package storage

import (
	"testing"

	"github.com/sciclon2/kafka-lag-go/pkg/config"
	"github.com/stretchr/testify/assert"
)

func TestInitializeStorage_UnsupportedStorageType(t *testing.T) {
	// Arrange: Create a config with an unsupported storage type
	mockConfig := &config.Config{
		Storage: struct {
			Type  string             `yaml:"type"`
			Redis config.RedisConfig `yaml:"redis"`
		}{
			Type: "unsupported", // Unsupported type
		},
	}

	// Act: Attempt to initialize storage with unsupported type

	storage, err := InitializeStorage(mockConfig)

	// Assert: Ensure the function returns an error for unsupported storage type
	assert.Error(t, err)
	assert.Nil(t, storage)
	assert.Contains(t, err.Error(), "unsupported storage type")
}
