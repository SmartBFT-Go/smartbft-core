// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package consensus

import (
	"testing"
	"time"

	"github.com/SmartBFT-Go/smartbft-core/internal/bft/mocks"
	bft "github.com/SmartBFT-Go/smartbft-core/pkg/api"
	"github.com/SmartBFT-Go/smartbft-core/pkg/metrics/disabled"
	"github.com/SmartBFT-Go/smartbft-core/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"
)

// returnsWithin runs f on its own goroutine and reports whether it returned in time.
// A blocked channel operation would otherwise sit there until the package timeout.
func returnsWithin(d time.Duration, f func()) bool {
	done := make(chan struct{})
	go func() {
		defer close(done)
		f()
	}()

	select {
	case <-done:
		return true
	case <-time.After(d):
		return false
	}
}

// stoppedConsensus returns a consensus whose run loop is gone: reconfigChan is unbuffered
// and nothing is left to read it, which is the state Deliver and Sync race with on shutdown.
func stoppedConsensus(t *testing.T) *Consensus {
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)

	c := &Consensus{
		Logger:       basicLog.Sugar(),
		Metrics:      bft.NewMetrics(&disabled.Provider{}),
		reconfigChan: make(chan types.Reconfig),
		stopChan:     make(chan struct{}),
	}
	close(c.stopChan)

	return c
}

func TestDeliverWithReconfigAfterStop(t *testing.T) {
	c := stoppedConsensus(t)

	app := &mocks.ApplicationMock{}
	app.On("Deliver", mock.Anything, mock.Anything).Return(types.Reconfig{InLatestDecision: true})
	c.Application = app

	// Deliver runs on the controller's run goroutine, which Stop waits for.
	assert.True(t, returnsWithin(10*time.Second, func() {
		c.Deliver(types.Proposal{}, nil)
	}), "Deliver did not return after consensus was stopped")
}

func TestSyncWithReconfigAfterStop(t *testing.T) {
	c := stoppedConsensus(t)

	synchronizer := &mocks.SynchronizerMock{}
	synchronizer.On("Sync").Return(types.SyncResponse{
		Reconfig: types.ReconfigSync{InReplicatedDecisions: true},
	})
	c.Synchronizer = synchronizer

	assert.True(t, returnsWithin(10*time.Second, func() {
		c.Sync()
	}), "Sync did not return after consensus was stopped")
}
