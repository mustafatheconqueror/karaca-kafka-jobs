package config

import (
	"time"
)

type paramBuilder struct {
	fetchedTime time.Time
	provider    Provider
}
