// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft_test

import (
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SmartBFT-Go/smartbft-core/internal/bft"
	"github.com/SmartBFT-Go/smartbft-core/internal/bft/mocks"
	"github.com/SmartBFT-Go/smartbft-core/pkg/api"
	"github.com/SmartBFT-Go/smartbft-core/pkg/metrics/disabled"
	"github.com/SmartBFT-Go/smartbft-core/pkg/types"
	"github.com/SmartBFT-Go/smartbft-core/pkg/wal"
	protos "github.com/SmartBFT-Go/smartbft-core/smartbftprotos"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

func TestControllerBasic(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()
	app := &mocks.ApplicationMock{}
	app.On("Deliver", mock.Anything, mock.Anything)
	batcher := &mocks.Batcher{}
	batcher.On("Close")
	pool := &mocks.RequestPool{}
	pool.On("Close")
	leaderMon := &mocks.LeaderMonitor{}
	leaderMon.On("ChangeRole", mock.Anything, mock.Anything, mock.Anything)
	leaderMon.On("Close")
	comm := &mocks.CommMock{}
	comm.On("SendConsensus", mock.Anything, mock.Anything)
	verifier := &mocks.VerifierMock{}
	verifier.On("VerificationSequence").Return(uint64(1))

	startedWG := sync.WaitGroup{}
	startedWG.Add(1)

	controller := &bft.Controller{
		Checkpoint:    &types.Checkpoint{},
		Batcher:       batcher,
		RequestPool:   pool,
		LeaderMonitor: leaderMon,
		ID:            1, // not the leader
		N:             4,
		NodesList:     []uint64{1, 2, 3, 4},
		Logger:        log,
		Application:   app,
		Comm:          comm,
		Verifier:      verifier,
		StartedWG:     &startedWG,
	}
	controller.Deliver = &bft.MutuallyExclusiveDeliver{C: controller}

	configureProposerBuilder(controller)

	controller.Start(1, 0, 0, false)

	leaderMon.On("ProcessMsg", uint64(2), heartbeat)
	controller.ProcessMessages(2, heartbeat)
	controller.ViewChanged(2, 1)
	controller.ViewChanged(3, 2)
	controller.AbortView(3)
	controller.AbortView(3)
	controller.Stop()
	controller.Stop()
}

func TestControllerLeaderBasic(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()
	batcher := &mocks.Batcher{}
	batcher.On("Close")
	batcher.On("Closed").Return(false)
	batcherChan := make(chan struct{})
	var once sync.Once
	batcher.On("NextBatch").Run(func(args mock.Arguments) {
		once.Do(func() {
			batcherChan <- struct{}{}
		})
	}).Return([][]byte{})
	pool := &mocks.RequestPool{}
	pool.On("Close")
	leaderMon := &mocks.LeaderMonitor{}
	leaderMon.On("ChangeRole", bft.Leader, mock.Anything, mock.Anything)
	leaderMon.On("Close")
	commMock := &mocks.CommMock{}
	commMock.On("SendConsensus", mock.Anything, mock.Anything)
	verifier := &mocks.VerifierMock{}
	verifier.On("VerificationSequence").Return(uint64(1))

	startedWG := sync.WaitGroup{}
	startedWG.Add(1)

	controller := &bft.Controller{
		InFlight:      &bft.InFlightData{},
		Checkpoint:    &types.Checkpoint{},
		RequestPool:   pool,
		LeaderMonitor: leaderMon,
		ID:            2, // the leader
		N:             4,
		NodesList:     []uint64{1, 2, 3, 4},
		Logger:        log,
		Batcher:       batcher,
		Comm:          commMock,
		Verifier:      verifier,
		StartedWG:     &startedWG,
	}
	controller.Deliver = &bft.MutuallyExclusiveDeliver{C: controller}

	configureProposerBuilder(controller)

	controller.Start(1, 0, 0, false)
	<-batcherChan
	controller.Stop()
	batcher.AssertCalled(t, "NextBatch")
}

func TestLeaderPropose(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()
	req := []byte{1}
	batcher := &mocks.Batcher{}
	batcher.On("Close")
	batcher.On("Closed").Return(false)
	batcher.On("NextBatch").Return([][]byte{req}).Once()
	batcher.On("NextBatch").Return([][]byte{req}).Once()
	batcher.On("PopRemainder").Return([][]byte{})
	batcher.On("BatchRemainder", mock.Anything)
	verifier := &mocks.VerifierMock{}
	verifier.On("VerifySignature", mock.Anything).Return(nil)
	verifier.On("VerifyRequest", req).Return(types.RequestInfo{}, nil)
	verifier.On("VerificationSequence").Return(uint64(1))
	verifier.On("VerifyProposal", mock.Anything, mock.Anything).Return(nil, nil)
	verifier.On("VerifyConsenterSig", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	verifier.On("AuxiliaryData", mock.Anything).Return(bft.MarshalOrPanic(&protos.PreparesFrom{
		Ids: []uint64{11, 23},
	}))
	assembler := &mocks.AssemblerMock{}
	assembler.On("AssembleProposal", mock.Anything, [][]byte{req}).Return(proposal, [][]byte{}).Once()
	secondProposal := proposal
	secondProposal.Metadata = bft.MarshalOrPanic(&protos.ViewMetadata{
		DecisionsInView: 1,
		LatestSequence:  1,
		ViewId:          1,
	})
	assembler.On("AssembleProposal", mock.Anything, [][]byte{req}).Return(secondProposal, [][]byte{}).Once()
	comm := &mocks.CommMock{}
	commWG := sync.WaitGroup{}
	comm.On("SendConsensus", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		commWG.Done()
	})
	comm.On("Nodes").Return([]uint64{11, 17, 23, 37})
	signer := &mocks.SignerMock{}
	signer.On("Sign", mock.Anything).Return(nil)
	signer.On("SignProposal", mock.Anything, mock.Anything).Return(&types.Signature{
		ID:    17,
		Value: []byte{4},
	})
	app := &mocks.ApplicationMock{}
	appWG := sync.WaitGroup{}
	app.On("Deliver", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		appWG.Done()
	}).Return(types.Reconfig{InLatestDecision: false})
	reqPool := &mocks.RequestPool{}
	reqPool.On("Prune", mock.Anything)
	reqPool.On("Close")
	leaderMon := &mocks.LeaderMonitor{}
	leaderMon.On("ChangeRole", bft.Leader, mock.Anything, mock.Anything)
	leaderMon.On("HeartbeatWasSent")
	leaderMon.On("Close")

	testDir, err := os.MkdirTemp("", "controller-unittest")
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)
	wal, err := wal.Create(log, testDir, nil)
	assert.NoError(t, err)

	synchronizer := &mocks.SynchronizerMock{}
	synchronizer.On("Sync").Run(func(args mock.Arguments) {}).Return(types.SyncResponse{Latest: types.Decision{
		Proposal:   types.Proposal{VerificationSequence: 0},
		Signatures: nil,
	}, Reconfig: types.ReconfigSync{InReplicatedDecisions: false}})

	collector := bft.StateCollector{
		SelfID:         11,
		N:              4,
		Logger:         log,
		CollectTimeout: 100 * time.Millisecond,
	}
	collector.Start()

	startedWG := sync.WaitGroup{}
	startedWG.Add(1)

	controller := &bft.Controller{
		InFlight:      &bft.InFlightData{},
		RequestPool:   reqPool,
		LeaderMonitor: leaderMon,
		WAL:           wal,
		ID:            17, // the leader
		N:             4,
		NodesList:     []uint64{11, 17, 23, 37},
		Logger:        log,
		Batcher:       batcher,
		Verifier:      verifier,
		Assembler:     assembler,
		Comm:          comm,
		Signer:        signer,
		Application:   app,
		Checkpoint:    &types.Checkpoint{},
		ViewChanger:   &bft.ViewChanger{},
		Synchronizer:  synchronizer,
		Collector:     &collector,
		StartedWG:     &startedWG,
		MetricsView:   api.NewMetricsView(&disabled.Provider{}),
	}
	controller.Deliver = &bft.MutuallyExclusiveDeliver{C: controller}

	configureProposerBuilder(controller)

	commWG.Add(9)
	controller.Start(1, 0, 0, true)
	commWG.Wait() // propose + state request

	commWG.Add(3)
	controller.ProcessMessages(23, prepare)
	controller.ProcessMessages(37, prepare)
	commWG.Wait()

	commit23 := proto.Clone(commit2).(*protos.Message)
	commit23Get := commit23.GetCommit()
	commit23Get.Signature.Signer = 23
	controller.ProcessMessages(23, commit23)
	commit37 := proto.Clone(commit3).(*protos.Message)
	commit37Get := commit37.GetCommit()
	commit37Get.Signature.Signer = 37
	appWG.Add(1)  // deliver
	commWG.Add(6) // next proposal
	controller.ProcessMessages(37, commit37)
	appWG.Wait()
	commWG.Wait()

	controller.Stop()
	app.AssertNumberOfCalls(t, "Deliver", 1)
	leaderMon.AssertCalled(t, "HeartbeatWasSent")

	// Ensure checkpoint was updated
	expected := &protos.Proposal{
		Header:               proposal.Header,
		Payload:              proposal.Payload,
		Metadata:             proposal.Metadata,
		VerificationSequence: uint64(proposal.VerificationSequence),
	}
	proposal, signatures := controller.Checkpoint.Get()
	assert.Equal(t, expected, proposal)
	signaturesBySigners := make(map[uint64]*protos.Signature)
	for _, sig := range signatures {
		signaturesBySigners[sig.Signer] = sig
	}
	assert.Equal(t, map[uint64]*protos.Signature{
		17: {Signer: 17, Value: []byte{4}},
		23: {Signer: 23, Value: []byte{4}},
		37: {Signer: 37, Value: []byte{4}},
	}, signaturesBySigners)

	controller.Stop()
	collector.Stop()
	wal.Close()
}

func TestViewChanged(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()
	req := []byte{1}
	batcher := &mocks.Batcher{}
	batcher.On("Close")
	batcher.On("Closed").Return(false)
	batcher.On("Reset")
	batcher.On("NextBatch").Return([][]byte{req})
	verifier := &mocks.VerifierMock{}
	verifier.On("VerifySignature", mock.Anything).Return(nil)
	verifier.On("VerificationSequence").Return(uint64(1))
	verifier.On("VerifyProposal", mock.Anything, mock.Anything).Return(nil, nil)

	secondProposal := proposal
	secondProposal.Metadata = bft.MarshalOrPanic(&protos.ViewMetadata{
		LatestSequence: 0,
		ViewId:         2,
	})

	assembler := &mocks.AssemblerMock{}
	assembler.On("AssembleProposal", mock.Anything, [][]byte{req}).Return(secondProposal, [][]byte{}).Once()
	comm := &mocks.CommMock{}
	commWG := sync.WaitGroup{}
	comm.On("SendConsensus", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		commWG.Done()
	})
	reqPool := &mocks.RequestPool{}
	reqPool.On("Prune", mock.Anything)
	reqPool.On("Close")
	leaderMon := &mocks.LeaderMonitor{}
	leaderMon.On("ChangeRole", bft.Follower, mock.Anything, mock.Anything)
	leaderMon.On("ChangeRole", bft.Leader, mock.Anything, mock.Anything)
	leaderMon.On("HeartbeatWasSent")
	leaderMon.On("Close")

	signer := &mocks.SignerMock{}
	signer.On("Sign", mock.Anything).Return(nil)

	testDir, err := os.MkdirTemp("", "controller-unittest")
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)
	wal, err := wal.Create(log, testDir, nil)
	assert.NoError(t, err)

	synchronizer := &mocks.SynchronizerMock{}
	synchronizer.On("Sync").Run(func(args mock.Arguments) {}).Return(types.SyncResponse{Latest: types.Decision{
		Proposal:   types.Proposal{VerificationSequence: 0},
		Signatures: nil,
	}, Reconfig: types.ReconfigSync{InReplicatedDecisions: false}})

	collector := bft.StateCollector{
		SelfID:         1,
		N:              4,
		Logger:         log,
		CollectTimeout: 100 * time.Millisecond,
	}
	collector.Start()

	startedWG := sync.WaitGroup{}
	startedWG.Add(1)

	controller := &bft.Controller{
		InFlight:      &bft.InFlightData{},
		Checkpoint:    &types.Checkpoint{},
		Signer:        signer,
		WAL:           wal,
		ID:            3, // the next leader
		N:             4,
		NodesList:     []uint64{1, 2, 3, 4},
		Logger:        log,
		Batcher:       batcher,
		Verifier:      verifier,
		Assembler:     assembler,
		Comm:          comm,
		RequestPool:   reqPool,
		LeaderMonitor: leaderMon,
		Synchronizer:  synchronizer,
		Collector:     &collector,
		StartedWG:     &startedWG,
	}
	controller.Deliver = &bft.MutuallyExclusiveDeliver{C: controller}

	configureProposerBuilder(controller)

	commWG.Add(3) // state request
	controller.Start(1, 0, 0, true)
	commWG.Wait()

	commWG.Add(6) // propose
	controller.ViewChanged(2, 0)
	commWG.Wait()
	batcher.AssertNumberOfCalls(t, "NextBatch", 1)
	assembler.AssertNumberOfCalls(t, "AssembleProposal", 1)
	comm.AssertNumberOfCalls(t, "SendConsensus", 9)
	leaderMon.AssertCalled(t, "HeartbeatWasSent")
	controller.Stop()
	collector.Stop()
	wal.Close()
}

func TestSyncPrevView(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()
	app := &mocks.ApplicationMock{}
	appWG := sync.WaitGroup{}
	app.On("Deliver", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		appWG.Done()
	}).Return(types.Reconfig{InLatestDecision: false})
	batcher := &mocks.Batcher{}
	batcher.On("Close")
	pool := &mocks.RequestPool{}
	pool.On("Close")
	pool.On("Prune", mock.Anything)
	leaderMon := &mocks.LeaderMonitor{}
	leaderMon.On("InjectArtificialHeartbeat", mock.Anything, mock.Anything)
	leaderMonWG := sync.WaitGroup{}
	leaderMon.On("ChangeRole", mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		leaderMonWG.Done()
	})
	leaderMon.On("Close")
	comm := &mocks.CommMock{}
	comm.On("SendConsensus", mock.Anything, mock.Anything)
	verifier := &mocks.VerifierMock{}
	verifier.On("VerifyProposal", mock.Anything, mock.Anything).Return(nil, nil)
	verifier.On("VerificationSequence").Return(uint64(1))
	verifier.On("VerifyConsenterSig", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	signer := &mocks.SignerMock{}
	signer.On("SignProposal", mock.Anything, mock.Anything).Return(&types.Signature{
		ID:    4,
		Value: []byte{4},
	})
	fd := &mocks.FailureDetector{}
	fd.On("Complain", mock.Anything, mock.Anything)
	synchronizer := &mocks.SynchronizerMock{}
	synchronizerWG := sync.WaitGroup{}
	synchronizer.On("Sync").Run(func(args mock.Arguments) {
		synchronizerWG.Done()
	}).Return(types.SyncResponse{Latest: types.Decision{
		Proposal: types.Proposal{
			Metadata: bft.MarshalOrPanic(&protos.ViewMetadata{
				LatestSequence: 0,
				ViewId:         0, // previous view number
			}),
			VerificationSequence: 1,
		},
		Signatures: nil,
	}, Reconfig: types.ReconfigSync{InReplicatedDecisions: false}})

	startedWG := sync.WaitGroup{}
	startedWG.Add(1)

	testDir, err := os.MkdirTemp("", "controller-unittest")
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)
	wal, err := wal.Create(log, testDir, nil)
	assert.NoError(t, err)

	collector := bft.StateCollector{
		SelfID:         0,
		N:              4,
		Logger:         log,
		CollectTimeout: 100 * time.Millisecond,
	}
	collector.Start()
	defer collector.Stop()

	controller := &bft.Controller{
		Collector:       &collector,
		InFlight:        &bft.InFlightData{},
		Batcher:         batcher,
		RequestPool:     pool,
		LeaderMonitor:   leaderMon,
		ID:              4, // not the leader
		N:               4,
		NodesList:       []uint64{1, 2, 3, 4},
		Logger:          log,
		Application:     app,
		Comm:            comm,
		ViewChanger:     &bft.ViewChanger{},
		Checkpoint:      &types.Checkpoint{},
		FailureDetector: fd,
		Synchronizer:    synchronizer,
		Verifier:        verifier,
		Signer:          signer,
		WAL:             wal,
		StartedWG:       &startedWG,
		MetricsView:     api.NewMetricsView(&disabled.Provider{}),
	}
	controller.Deliver = &bft.MutuallyExclusiveDeliver{C: controller}

	vs := configureProposerBuilder(controller)
	controller.ViewSequences = vs

	leaderMonWG.Add(1)
	controller.Start(1, 0, 0, false)
	leaderMonWG.Wait()

	appWG.Add(1)
	controller.ProcessMessages(2, prePrepare)
	controller.ProcessMessages(2, prepare)
	controller.ProcessMessages(3, prepare)
	controller.ProcessMessages(2, commit2)
	controller.ProcessMessages(3, commit3)

	appWG.Wait()
	app.AssertNumberOfCalls(t, "Deliver", 1)

	synchronizerWG.Add(1)
	leaderMonWG.Add(1)
	wrongViewMsg := proto.Clone(prePrepare).(*protos.Message)
	wrongViewMsgGet := wrongViewMsg.GetPrePrepare()
	wrongViewMsgGet.View = 2
	controller.ProcessMessages(2, wrongViewMsg)
	synchronizerWG.Wait()
	leaderMonWG.Wait() // wait for view to start before sending messages

	prePrepareNext := proto.Clone(prePrepare).(*protos.Message)
	prePrepareNextGet := prePrepareNext.GetPrePrepare()
	prePrepareNextGet.Seq = 1
	prePrepareNextGet.GetProposal().Metadata = bft.MarshalOrPanic(&protos.ViewMetadata{
		DecisionsInView: 1,
		LatestSequence:  1,
		ViewId:          1,
	})
	controller.ProcessMessages(2, prePrepareNext)

	nextProp := types.Proposal{
		Header:               prePrepareNextGet.Proposal.Header,
		Payload:              prePrepareNextGet.Proposal.Payload,
		Metadata:             prePrepareNextGet.Proposal.Metadata,
		VerificationSequence: 1,
	}
	prepareNext := proto.Clone(prepare).(*protos.Message)
	prepareNextGet := prepareNext.GetPrepare()
	prepareNextGet.Seq = 1
	prepareNextGet.Digest = nextProp.Digest()
	controller.ProcessMessages(2, prepareNext)
	controller.ProcessMessages(3, prepareNext)

	commit2Next := proto.Clone(commit2).(*protos.Message)
	commit2NextGet := commit2Next.GetCommit()
	commit2NextGet.Seq = 1
	commit2NextGet.Digest = nextProp.Digest()

	commit3Next := proto.Clone(commit3).(*protos.Message)
	commit3NextGet := commit3Next.GetCommit()
	commit3NextGet.Seq = 1
	commit3NextGet.Digest = nextProp.Digest()

	appWG.Add(1)
	controller.ProcessMessages(2, commit2Next)
	controller.ProcessMessages(3, commit3Next)

	appWG.Wait()
	app.AssertNumberOfCalls(t, "Deliver", 2)

	controller.Stop()
	wal.Close()
}

func TestControllerLeaderRequestHandling(t *testing.T) {
	for _, testCase := range []struct {
		description      string
		startViewNum     uint64
		verifyReqReturns error
		shouldEnqueue    bool
		shouldVerify     bool
		waitForLoggedMsg string
	}{
		{
			description:      "not the leader",
			startViewNum:     2,
			waitForLoggedMsg: "Got request from 3 but the leader is 2, dropping request",
		},
		{
			description:      "bad request",
			startViewNum:     1,
			verifyReqReturns: errors.New("unauthorized user"),
			waitForLoggedMsg: "unauthorized user",
			shouldVerify:     true,
		},
		{
			description:      "good request",
			shouldEnqueue:    true,
			startViewNum:     1,
			waitForLoggedMsg: "Got request from 3",
			shouldVerify:     true,
		},
	} {
		t.Run(testCase.description, func(t *testing.T) {
			var submittedToPool sync.WaitGroup

			basicLog, err := zap.NewDevelopment()
			assert.NoError(t, err)

			log := basicLog.Sugar()

			batcher := &mocks.Batcher{}
			batcher.On("Close")
			batcher.On("Closed").Return(false)
			batcher.On("Reset")
			batcher.On("NextBatch").Run(func(arguments mock.Arguments) {
				time.Sleep(time.Hour)
			})

			pool := &mocks.RequestPool{}
			pool.On("Close")
			leaderMon := &mocks.LeaderMonitor{}
			leaderMon.On("ChangeRole", bft.Follower, mock.Anything, mock.Anything)
			leaderMon.On("ChangeRole", bft.Leader, mock.Anything, mock.Anything)
			leaderMon.On("Close")
			if testCase.shouldEnqueue {
				submittedToPool.Add(1)
				pool.On("Submit", mock.Anything).Return(nil).Run(func(_ mock.Arguments) {
					submittedToPool.Done()
				})
			}

			commMock := &mocks.CommMock{}
			commMock.On("SendConsensus", mock.Anything, mock.Anything)

			verifier := &mocks.VerifierMock{}
			verifier.On("VerifyRequest", mock.Anything).Return(types.RequestInfo{}, testCase.verifyReqReturns)
			verifier.On("VerificationSequence").Return(uint64(0))

			synchronizer := &mocks.SynchronizerMock{}
			synchronizer.On("Sync").Run(func(args mock.Arguments) {}).Return(types.SyncResponse{Latest: types.Decision{
				Proposal:   types.Proposal{VerificationSequence: 0},
				Signatures: nil,
			}, Reconfig: types.ReconfigSync{InReplicatedDecisions: false}})

			collector := bft.StateCollector{
				SelfID:         0,
				N:              4,
				Logger:         log,
				CollectTimeout: 100 * time.Millisecond,
			}
			collector.Start()
			defer collector.Stop()

			startedWG := sync.WaitGroup{}
			startedWG.Add(1)

			controller := &bft.Controller{
				InFlight:      &bft.InFlightData{},
				Checkpoint:    &types.Checkpoint{},
				RequestPool:   pool,
				LeaderMonitor: leaderMon,
				ID:            1,
				N:             4,
				NodesList:     []uint64{0, 1, 2, 3},
				Logger:        log,
				Batcher:       batcher,
				Comm:          commMock,
				Verifier:      verifier,
				Synchronizer:  synchronizer,
				Collector:     &collector,
				StartedWG:     &startedWG,
			}
			controller.Deliver = &bft.MutuallyExclusiveDeliver{C: controller}

			configureProposerBuilder(controller)
			controller.Start(testCase.startViewNum, 0, 0, true)

			controller.HandleRequest(3, []byte{1, 2, 3})

			submittedToPool.Wait()

			if !testCase.shouldVerify {
				verifier.AssertNotCalled(t, "VerifyRequest", mock.Anything)
			}
		})
	}
}

func createView(c *bft.Controller, leader, proposalSequence, viewNum, decisionsInView uint64, quorumSize int, vs *atomic.Value) *bft.View {
	mn := &mocks.MembershipNotifierMock{}
	mn.On("MembershipChange").Return(false)
	return &bft.View{
		RetrieveCheckpoint: c.Checkpoint.Get,
		N:                  c.N,
		NodesList:          c.NodesList,
		LeaderID:           leader,
		SelfID:             c.ID,
		Quorum:             quorumSize,
		Number:             viewNum,
		Decider:            c,
		FailureDetector:    c.FailureDetector,
		Sync:               c,
		Logger:             c.Logger,
		Comm:               c,
		Verifier:           c.Verifier,
		Signer:             c.Signer,
		MembershipNotifier: mn,
		ProposalSequence:   proposalSequence,
		DecisionsInView:    decisionsInView,
		ViewSequences:      vs,
		State:              &bft.PersistedState{WAL: c.WAL, InFlightProposal: &bft.InFlightData{}},
		InMsgQSize:         int(c.N * 10),
		MetricsView:        api.NewMetricsView(&disabled.Provider{}),
		MetricsBlacklist:   api.NewMetricsBlacklist(&disabled.Provider{}),
	}
}

func configureProposerBuilder(controller *bft.Controller) *atomic.Value {
	pb := &mocks.ProposerBuilder{}
	vs := &atomic.Value{}
	pb.On("NewProposer", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(func(a uint64, b uint64, c uint64, d uint64, e int) bft.Proposer {
			return createView(controller, a, b, c, d, e, vs)
		}, bft.Phase(bft.COMMITTED))
	controller.ProposerBuilder = pb
	return vs
}

func TestSyncInform(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()
	req := []byte{1}
	batcher := &mocks.Batcher{}
	batcher.On("Close")
	batcher.On("Closed").Return(false)
	batcher.On("Reset")
	batcher.On("NextBatch").Return([][]byte{req})
	verifier := &mocks.VerifierMock{}
	verifier.On("VerifySignature", mock.Anything).Return(nil)
	verifier.On("VerificationSequence").Return(uint64(1))
	verifier.On("VerifyProposal", mock.Anything, mock.Anything).Return(nil, nil)
	verifier.On("AuxiliaryData", mock.Anything).Return(bft.MarshalOrPanic(&protos.PreparesFrom{
		Ids: []uint64{1},
	}))
	verifier.On("VerifyConsenterSig", mock.Anything, mock.Anything).Return(bft.MarshalOrPanic(&protos.PreparesFrom{
		Ids: []uint64{1},
	}), nil)

	secondProposal := proposal
	secondProposal.Metadata = bft.MarshalOrPanic(&protos.ViewMetadata{
		DecisionsInView: 1,
		LatestSequence:  2,
		ViewId:          2,
	})

	assembler := &mocks.AssemblerMock{}
	assembler.On("AssembleProposal", mock.Anything, [][]byte{req}).Return(secondProposal).Once()

	comm := &mocks.CommMock{}
	commWG := sync.WaitGroup{}
	comm.On("SendConsensus", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		commWG.Done()
	})
	comm.On("Nodes").Return([]uint64{0, 1, 2, 3})

	commWithChan := &mocks.CommMock{}
	msgChan := make(chan *protos.Message)
	commWithChan.On("BroadcastConsensus", mock.Anything).Run(func(args mock.Arguments) {
		msgChan <- args.Get(0).(*protos.Message)
	})
	commWithChan.On("Nodes").Return([]uint64{0, 1, 2, 3})

	reqPool := &mocks.RequestPool{}
	reqPool.On("Close")
	leaderMon := &mocks.LeaderMonitor{}
	leaderMon.On("ChangeRole", bft.Follower, mock.Anything, mock.Anything)
	leaderMon.On("ChangeRole", bft.Leader, mock.Anything, mock.Anything)
	leaderMon.On("HeartbeatWasSent")
	leaderMon.On("Close")

	signer := &mocks.SignerMock{}
	signer.On("Sign", mock.Anything).Return(nil)

	testDir, err := os.MkdirTemp("", "controller-unittest")
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)
	wal, err := wal.Create(log, testDir, nil)
	assert.NoError(t, err)

	synchronizer := &mocks.SynchronizerMock{}
	synchronizerWG := sync.WaitGroup{}
	syncToView := uint64(2)
	synchronizer.On("Sync").Run(func(args mock.Arguments) {
		synchronizerWG.Done()
	}).Return(types.SyncResponse{Latest: types.Decision{
		Proposal: types.Proposal{
			Metadata: bft.MarshalOrPanic(&protos.ViewMetadata{
				LatestSequence: 1,
				ViewId:         syncToView,
			}),
			VerificationSequence: 1,
		},
		Signatures: []types.Signature{
			{ID: 1}, {ID: 2}, {ID: 3},
		},
	}, Reconfig: types.ReconfigSync{InReplicatedDecisions: false}})

	reqTimer := &mocks.RequestsTimer{}
	reqTimer.On("StopTimers")
	reqTimer.On("RestartTimers")
	controllerMock := &mocks.ViewController{}
	controllerMock.On("AbortView", mock.Anything)

	collector := bft.StateCollector{
		SelfID:         0,
		N:              4,
		Logger:         log,
		CollectTimeout: 100 * time.Millisecond,
	}
	collector.Start()

	vc := &bft.ViewChanger{
		SelfID:              2,
		N:                   4,
		NodesList:           []uint64{0, 1, 2, 3},
		Logger:              log,
		Comm:                commWithChan,
		RequestsTimer:       reqTimer,
		Ticker:              make(chan time.Time),
		Controller:          controllerMock,
		InMsqQSize:          100,
		ControllerStartedWG: sync.WaitGroup{},
		MetricsViewChange:   api.NewMetricsViewChange(&disabled.Provider{}),
	}

	vc.ControllerStartedWG.Add(1)

	controller := &bft.Controller{
		InFlight:      &bft.InFlightData{},
		Signer:        signer,
		WAL:           wal,
		ID:            2,
		N:             4,
		NodesList:     []uint64{0, 1, 2, 3},
		Logger:        log,
		Batcher:       batcher,
		Verifier:      verifier,
		Assembler:     assembler,
		Comm:          comm,
		RequestPool:   reqPool,
		LeaderMonitor: leaderMon,
		Synchronizer:  synchronizer,
		ViewChanger:   vc,
		Checkpoint:    &types.Checkpoint{},
		Collector:     &collector,
		StartedWG:     &vc.ControllerStartedWG,
	}
	controller.Deliver = &bft.MutuallyExclusiveDeliver{C: controller}
	configureProposerBuilder(controller)

	controller.Checkpoint.Set(proposal, []types.Signature{{ID: 1}, {ID: 2}, {ID: 3}})

	vc.Start(1)

	synchronizerWG.Add(1)
	commWG.Add(9)
	controller.Start(1, 0, 0, true)
	synchronizerWG.Wait()
	commWG.Wait()

	vc.StartViewChange(2, true)
	msg := <-msgChan
	assert.NotNil(t, msg.GetViewChange())
	assert.Equal(t, syncToView+1, msg.GetViewChange().NextView) // view number did change according to info

	batcher.AssertNumberOfCalls(t, "NextBatch", 1)
	assembler.AssertNumberOfCalls(t, "AssembleProposal", 1)
	comm.AssertNumberOfCalls(t, "SendConsensus", 9)
	leaderMon.AssertCalled(t, "HeartbeatWasSent")

	controller.Stop()
	vc.Stop()
	collector.Stop()
	wal.Close()
}

func TestRotateFromLeaderToFollower(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()

	testDir, err := os.MkdirTemp("", "controller-unittest")
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)
	wal, err := wal.Create(log, testDir, nil)
	assert.NoError(t, err)
	defer wal.Close()

	var restartTimersWG sync.WaitGroup
	restartTimersWG.Add(2)
	reqPool := &mocks.RequestPool{}
	reqPool.On("RestartTimers").Run(func(args mock.Arguments) {
		restartTimersWG.Done()
	})
	reqPool.On("Prune", mock.Anything)
	reqPool.On("Close")
	leaderMon := &mocks.LeaderMonitor{}
	leaderMon.On("ChangeRole", bft.Leader, mock.Anything, mock.Anything)
	leaderMonWG := sync.WaitGroup{}
	leaderMon.On("ChangeRole", bft.Follower, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		leaderMonWG.Done()
	})
	leaderMon.On("HeartbeatWasSent")
	leaderMon.On("InjectArtificialHeartbeat", uint64(3), mock.Anything)
	leaderMon.On("Close")
	req := []byte{1}
	batcher := &mocks.Batcher{}
	batcher.On("Close")
	batcher.On("Closed").Return(false)
	batcher.On("NextBatch").Return([][]byte{req}).Once()
	batcher.On("PopRemainder").Return([][]byte{})
	batcher.On("BatchRemainder", mock.Anything)
	verifier := &mocks.VerifierMock{}
	verifier.On("VerifySignature", mock.Anything).Return(nil)
	verifier.On("VerifyRequest", req).Return(types.RequestInfo{}, nil)
	verifier.On("VerificationSequence").Return(uint64(1))
	verifier.On("VerifyProposal", mock.Anything, mock.Anything).Return(nil, nil)
	verifier.On("VerifyConsenterSig", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	verifier.On("AuxiliaryData", mock.Anything).Return(bft.MarshalOrPanic(&protos.PreparesFrom{
		Ids: []uint64{1, 3},
	}))

	assembler := &mocks.AssemblerMock{}
	assembler.On("AssembleProposal", mock.Anything, [][]byte{req}).Return(proposal, [][]byte{}).Once()
	comm := &mocks.CommMock{}
	commWG := sync.WaitGroup{}
	comm.On("SendConsensus", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		commWG.Done()
	})
	signer := &mocks.SignerMock{}
	signer.On("Sign", mock.Anything).Return(nil)
	signer.On("SignProposal", mock.Anything, mock.Anything).Return(&types.Signature{
		ID:    2,
		Value: []byte{4},
	})
	app := &mocks.ApplicationMock{}
	appWG := sync.WaitGroup{}
	app.On("Deliver", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		appWG.Done()
	}).Return(types.Reconfig{InLatestDecision: false})

	startedWG := sync.WaitGroup{}
	startedWG.Add(1)

	controller := &bft.Controller{
		InFlight:           &bft.InFlightData{},
		RequestPool:        reqPool,
		LeaderMonitor:      leaderMon,
		WAL:                wal,
		ID:                 2, // the first leader
		N:                  4,
		NodesList:          []uint64{1, 2, 3, 4},
		Logger:             log,
		Batcher:            batcher,
		Verifier:           verifier,
		Assembler:          assembler,
		Comm:               comm,
		Signer:             signer,
		Application:        app,
		Checkpoint:         &types.Checkpoint{},
		ViewChanger:        &bft.ViewChanger{},
		StartedWG:          &startedWG,
		LeaderRotation:     true,
		DecisionsPerLeader: 1,
		MetricsView:        api.NewMetricsView(&disabled.Provider{}),
	}
	controller.Deliver = &bft.MutuallyExclusiveDeliver{C: controller}
	vs := configureProposerBuilder(controller)
	controller.ViewSequences = vs

	commWG.Add(6)
	controller.Start(1, 0, 0, false)
	commWG.Wait() // propose (pre-prepare + prepare)

	commWG.Add(3)
	controller.ProcessMessages(3, prepare)
	controller.ProcessMessages(4, prepare)
	commWG.Wait() // commit

	controller.ProcessMessages(1, commit1)
	appWG.Add(1)       // deliver
	leaderMonWG.Add(1) // change role
	controller.ProcessMessages(3, commit3)
	appWG.Wait()
	commWG.Wait()
	leaderMonWG.Wait()

	// leader rotation (now 3 is the leader)

	prePrepareNext := proto.Clone(prePrepare).(*protos.Message)
	prePrepareNextGet := prePrepareNext.GetPrePrepare()
	prePrepareNextGet.Seq = 1
	prePrepareNextGet.GetProposal().Metadata = bft.MarshalOrPanic(&protos.ViewMetadata{
		DecisionsInView: 1,
		LatestSequence:  1,
		ViewId:          1,
	})
	commWG.Add(3) // sending prepare
	controller.ProcessMessages(3, prePrepareNext)
	commWG.Wait()

	nextProp := types.Proposal{
		Header:               prePrepareNextGet.Proposal.Header,
		Payload:              prePrepareNextGet.Proposal.Payload,
		Metadata:             prePrepareNextGet.Proposal.Metadata,
		VerificationSequence: 1,
	}
	prepareNext := proto.Clone(prepare).(*protos.Message)
	prepareNextGet := prepareNext.GetPrepare()
	prepareNextGet.Seq = 1
	prepareNextGet.Digest = nextProp.Digest()
	commWG.Add(3) // sending commit
	controller.ProcessMessages(3, prepareNext)
	controller.ProcessMessages(4, prepareNext)
	commWG.Wait()

	commit1Next := proto.Clone(commit1).(*protos.Message)
	commit1NextGet := commit1Next.GetCommit()
	commit1NextGet.Seq = 1
	commit1NextGet.Digest = nextProp.Digest()

	commit3Next := proto.Clone(commit3).(*protos.Message)
	commit3NextGet := commit3Next.GetCommit()
	commit3NextGet.Seq = 1
	commit3NextGet.Digest = nextProp.Digest()

	appWG.Add(1)
	leaderMonWG.Add(1)
	controller.ProcessMessages(1, commit1Next)
	controller.ProcessMessages(3, commit3Next)
	leaderMonWG.Wait()
	restartTimersWG.Wait()
	appWG.Wait()
	app.AssertNumberOfCalls(t, "Deliver", 2)

	controller.Stop()
}

func TestRotateFromFollowerToLeader(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()

	testDir, err := os.MkdirTemp("", "controller-unittest")
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)
	wal, err := wal.Create(log, testDir, nil)
	assert.NoError(t, err)
	defer wal.Close()

	var restartTimersWG sync.WaitGroup
	restartTimersWG.Add(2)
	reqPool := &mocks.RequestPool{}
	reqPool.On("Prune", mock.Anything)
	reqPool.On("Close")
	reqPool.On("RestartTimers").Run(func(args mock.Arguments) {
		restartTimersWG.Done()
	})
	leaderMon := &mocks.LeaderMonitor{}
	leaderMonWG := sync.WaitGroup{}
	leaderMon.On("ChangeRole", bft.Leader, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		leaderMonWG.Done()
	})
	followerMonWG := sync.WaitGroup{}
	leaderMon.On("ChangeRole", bft.Follower, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		followerMonWG.Done()
	})
	leaderMon.On("HeartbeatWasSent")
	leaderMon.On("InjectArtificialHeartbeat", uint64(2), mock.Anything)
	leaderMon.On("Close")
	req := []byte{1}
	batcher := &mocks.Batcher{}
	batcher.On("Close")
	batcher.On("Reset")
	batcher.On("Closed").Return(false)
	batcher.On("NextBatch").Return([][]byte{req}).Once()
	batcher.On("PopRemainder").Return([][]byte{})
	batcher.On("BatchRemainder", mock.Anything)
	verifier := &mocks.VerifierMock{}
	verifier.On("VerifySignature", mock.Anything).Return(nil)
	verifier.On("VerifyRequest", req).Return(types.RequestInfo{}, nil)
	verifier.On("VerificationSequence").Return(uint64(1))
	verifier.On("VerifyProposal", mock.Anything, mock.Anything).Return(nil, nil)
	verifier.On("VerifyConsenterSig", mock.Anything, mock.Anything).Return(nil, nil)
	verifier.On("AuxiliaryData", mock.Anything).Return(bft.MarshalOrPanic(&protos.PreparesFrom{
		Ids: []uint64{1, 3},
	}))

	nextMD := bft.MarshalOrPanic(&protos.ViewMetadata{
		DecisionsInView: 1,
		LatestSequence:  1,
		ViewId:          1,
		PrevCommitSignatureDigest: bft.CommitSignaturesDigest([]*protos.Signature{
			commit1.GetCommit().Signature,
			commit2.GetCommit().Signature,
			commit3.GetCommit().Signature,
		}),
	})
	nextProp := types.Proposal{
		Header:               proposal.Header,
		Payload:              proposal.Payload,
		Metadata:             nextMD,
		VerificationSequence: 1,
	}

	assembler := &mocks.AssemblerMock{}
	assembler.On("AssembleProposal", mock.Anything, [][]byte{req}).Return(nextProp, [][]byte{}).Once()
	comm := &mocks.CommMock{}
	commWG := sync.WaitGroup{}
	comm.On("Nodes").Return([]uint64{1, 2, 3, 4})
	comm.On("SendConsensus", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		commWG.Done()
	})
	signer := &mocks.SignerMock{}
	signer.On("Sign", mock.Anything).Return(nil)
	signer.On("SignProposal", mock.Anything, mock.Anything).Return(&types.Signature{
		ID:    3,
		Value: []byte{4},
	})
	app := &mocks.ApplicationMock{}
	appWG := sync.WaitGroup{}
	app.On("Deliver", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		appWG.Done()
	}).Return(types.Reconfig{InLatestDecision: false})

	startedWG := sync.WaitGroup{}
	startedWG.Add(1)

	controller := &bft.Controller{
		InFlight:           &bft.InFlightData{},
		RequestPool:        reqPool,
		LeaderMonitor:      leaderMon,
		WAL:                wal,
		ID:                 3, // the second leader
		N:                  4,
		NodesList:          []uint64{1, 2, 3, 4},
		Logger:             log,
		Batcher:            batcher,
		Verifier:           verifier,
		Assembler:          assembler,
		Comm:               comm,
		Signer:             signer,
		Application:        app,
		Checkpoint:         &types.Checkpoint{},
		ViewChanger:        &bft.ViewChanger{},
		StartedWG:          &startedWG,
		LeaderRotation:     true,
		DecisionsPerLeader: 1,
		MetricsView:        api.NewMetricsView(&disabled.Provider{}),
	}
	controller.Deliver = &bft.MutuallyExclusiveDeliver{C: controller}
	vs := configureProposerBuilder(controller)
	controller.ViewSequences = vs

	followerMonWG.Add(1) // change role
	controller.Start(1, 0, 0, false)
	followerMonWG.Wait()

	commWG.Add(3)
	controller.ProcessMessages(2, prePrepare)
	commWG.Wait() // prepare

	commWG.Add(3)
	controller.ProcessMessages(1, prepare)
	controller.ProcessMessages(4, prepare)
	commWG.Wait() // commit

	controller.ProcessMessages(1, commit1)
	time.Sleep(time.Second)
	appWG.Add(1)       // deliver
	leaderMonWG.Add(1) // change role
	commWG.Add(6)      // propose + prepare
	controller.ProcessMessages(2, commit2)
	appWG.Wait()
	leaderMonWG.Wait()
	commWG.Wait()

	// leader rotation (now 3 is the leader)

	prepareNext := proto.Clone(prepare).(*protos.Message)
	prepareNextGet := prepareNext.GetPrepare()
	prepareNextGet.Seq = 1
	prepareNextGet.Digest = nextProp.Digest()
	commWG.Add(3) // sending commit
	controller.ProcessMessages(1, prepareNext)
	controller.ProcessMessages(4, prepareNext)
	commWG.Wait()

	commit1Next := proto.Clone(commit1).(*protos.Message)
	commit1NextGet := commit1Next.GetCommit()
	commit1NextGet.Seq = 1
	commit1NextGet.Digest = nextProp.Digest()

	commit2Next := proto.Clone(commit2).(*protos.Message)
	commit2NextGet := commit2Next.GetCommit()
	commit2NextGet.Seq = 1
	commit2NextGet.Digest = nextProp.Digest()

	appWG.Add(1)
	followerMonWG.Add(1)
	controller.ProcessMessages(1, commit1Next)
	controller.ProcessMessages(2, commit2Next)
	followerMonWG.Wait()
	restartTimersWG.Wait()
	appWG.Wait()
	app.AssertNumberOfCalls(t, "Deliver", 2)

	controller.Stop()
}

func TestDeliverTwiceOnceFromSyncAndOnceFromViewData(t *testing.T) {

	// In this scenario a decision is delivered twice.
	// Once by the synchronizer and a second time by the view data received during a view change.
	// In the beginning there is a view change timeout and there is a view still running concurrently.
	// Afterwards the view change is successful, but it does not include a decision made during the concurrent view.
	// Later there is a second view change timeout and the synchronizer is called and delivers the decision.
	// The checkpoint should be updated accordingly.
	// Then the node receives a view data message which include the decision.
	// If the checkpoint was updated correctly then this decision should not be delivered twice.

	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()

	testDir, err := os.MkdirTemp("", "controller-unittest")
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)
	wal, err := wal.Create(log, testDir, nil)
	assert.NoError(t, err)

	comm := &mocks.CommMock{}
	comm.On("BroadcastConsensus", mock.Anything)
	commSendWG := sync.WaitGroup{}
	comm.On("SendConsensus", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		commSendWG.Done()
	})

	reqTimer := &mocks.RequestsTimer{}
	reqTimer.On("StopTimers")
	reqTimer.On("RestartTimers")

	ticker := make(chan time.Time)

	signer := &mocks.SignerMock{}
	signer.On("Sign", mock.Anything).Return(nil)
	signer.On("SignProposal", mock.Anything, mock.Anything).Return(&types.Signature{
		ID:    4,
		Value: []byte{4},
	})

	batcher := &mocks.Batcher{}
	batcher.On("Close")

	state := &mocks.State{}
	state.On("Save", mock.Anything).Return(nil)

	verifier := &mocks.VerifierMock{}
	verifier.On("VerificationSequence").Return(uint64(1))
	verifierSigWG := sync.WaitGroup{}
	verifier.On("VerifySignature", mock.Anything).Run(func(args mock.Arguments) {
		verifierSigWG.Done()
	}).Return(nil)
	verifier.On("VerifyConsenterSig", mock.Anything, mock.Anything).Return(bft.MarshalOrPanic(&protos.PreparesFrom{
		Ids: []uint64{1},
	}), nil)
	verifier.On("VerifyProposal", mock.Anything, mock.Anything).Return(nil, nil)

	assembler := &mocks.AssemblerMock{}

	reqPool := &mocks.RequestPool{}
	reqPool.On("Close")

	leaderMon := &mocks.LeaderMonitor{}
	leaderMonWG := sync.WaitGroup{}
	leaderMon.On("ChangeRole", bft.Follower, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		leaderMonWG.Done()
	})
	leaderMon.On("InjectArtificialHeartbeat", uint64(1), mock.Anything)
	leaderMon.On("Close")

	collector := bft.StateCollector{
		SelfID:         4,
		N:              7,
		Logger:         log,
		CollectTimeout: 100 * time.Millisecond,
	}
	collector.Start()

	// The synchronizer returns on the first call a proposal with sequence 0 and on the second call a proposal with sequence 1.
	// In both times the synchronizer is called after a view change timeout.
	synchronizer := &mocks.SynchronizerMock{}
	synchronizerWG := sync.WaitGroup{}
	synchronizer.On("Sync").Run(func(args mock.Arguments) {
		synchronizerWG.Done()
	}).Return(types.SyncResponse{Latest: types.Decision{
		Proposal: types.Proposal{
			Metadata: bft.MarshalOrPanic(&protos.ViewMetadata{
				LatestSequence: 0,
				ViewId:         0,
			}),
			VerificationSequence: 1,
		},
		Signatures: []types.Signature{
			{ID: 1}, {ID: 2}, {ID: 3}, {ID: 4}, {ID: 5},
		},
	}, Reconfig: types.ReconfigSync{InReplicatedDecisions: false}}).Once()
	synchronizer.On("Sync").Run(func(args mock.Arguments) {
		synchronizerWG.Done()
	}).Return(types.SyncResponse{Latest: types.Decision{
		Proposal: types.Proposal{
			Metadata: bft.MarshalOrPanic(&protos.ViewMetadata{
				LatestSequence: 1,
				ViewId:         0,
			}),
			VerificationSequence: 1,
		},
		Signatures: []types.Signature{
			{ID: 1}, {ID: 2}, {ID: 3}, {ID: 4}, {ID: 5},
		},
	}, Reconfig: types.ReconfigSync{InReplicatedDecisions: false}}).Once()

	// If deliver is called with sequence 1 then the test fails.
	// This is after the synchronizer returns with sequence 1.
	app := &mocks.ApplicationMock{}
	app.On("Deliver", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		proposal := args.Get(0).(types.Proposal)
		md := &protos.ViewMetadata{}
		if err := proto.Unmarshal(proposal.Metadata, md); err != nil {
			panic("Error unmarshaling metadata")
		}
		if md.LatestSequence == 1 {
			panic("Delivering sequence 1 twice")
		}
	}).Return(types.Reconfig{InLatestDecision: false})

	vc := &bft.ViewChanger{
		SelfID:            4,
		N:                 7,
		NodesList:         []uint64{1, 2, 3, 4, 5, 6, 7},
		Logger:            log,
		Comm:              comm,
		Application:       app,
		Verifier:          verifier,
		Signer:            signer,
		RequestsTimer:     reqTimer,
		State:             state,
		InFlight:          &bft.InFlightData{},
		Ticker:            ticker,
		InMsqQSize:        100,
		MetricsViewChange: api.NewMetricsViewChange(&disabled.Provider{}),
		ViewChangeTimeout: 10 * time.Second,
		ResendTimeout:     20 * time.Second,
	}

	vc.ControllerStartedWG.Add(1)

	controller := &bft.Controller{
		InFlight:      &bft.InFlightData{},
		Signer:        signer,
		State:         state,
		WAL:           wal,
		ID:            4,
		N:             7,
		NodesList:     []uint64{1, 2, 3, 4, 5, 6, 7},
		Logger:        log,
		Batcher:       batcher,
		Verifier:      verifier,
		Assembler:     assembler,
		Comm:          comm,
		RequestPool:   reqPool,
		LeaderMonitor: leaderMon,
		Synchronizer:  synchronizer,
		ViewChanger:   vc,
		Checkpoint:    &types.Checkpoint{},
		Collector:     &collector,
		StartedWG:     &vc.ControllerStartedWG,
	}
	configureProposerBuilder(controller)

	controller.Checkpoint.Set(proposal, []types.Signature{{ID: 1}, {ID: 2}, {ID: 3}, {ID: 4}, {ID: 5}})

	vc.Controller = controller
	vc.Synchronizer = controller
	vc.Checkpoint = controller.Checkpoint
	vc.Start(0)

	vs := configureProposerBuilder(controller)
	controller.ViewSequences = vs

	leaderMonWG.Add(1)
	controller.Start(0, 1, 0, false)
	leaderMonWG.Wait()

	// Starting view change

	startTime := time.Now()
	vc.StartViewChange(1, true)

	vc.HandleMessage(1, viewChangeMsg)
	vc.HandleMessage(2, viewChangeMsg)
	vc.HandleMessage(3, viewChangeMsg)
	commSendWG.Add(1) // sending view data msg and joining the view change
	vc.HandleMessage(5, viewChangeMsg)
	commSendWG.Wait()

	// View change timeout, synchronizer is called

	synchronizerWG.Add(1)
	commSendWG.Add(6) // fetch and send state after sync
	leaderMonWG.Add(1)
	ticker <- startTime.Add(5 * time.Second)
	ticker <- startTime.Add(10 * time.Second)
	ticker <- startTime.Add(9 * time.Second)
	ticker <- startTime.Add(12 * time.Second)
	ticker <- startTime.Add(15 * time.Second) // view change timeout
	synchronizerWG.Wait()
	commSendWG.Wait()
	synchronizer.AssertNumberOfCalls(t, "Sync", 1)
	leaderMonWG.Wait()

	// Concurrent view is running and a decision is delivered (by the other nodes)

	commSendWG.Add(6) // send prepares
	controller.ProcessMessages(1, prePrepareSeq1View0)
	commSendWG.Wait()

	commSendWG.Add(6) // send commits
	controller.ProcessMessages(1, prepareSeq1View0)
	controller.ProcessMessages(2, prepareSeq1View0)
	controller.ProcessMessages(3, prepareSeq1View0)
	controller.ProcessMessages(7, prepareSeq1View0)
	commSendWG.Wait()

	// A view change occurs (the new view message does not include the decision made by the concurrent view)

	verifierSigWG.Add(5)
	leaderMonWG.Add(1)
	vc.HandleMessage(2, createNewViewMsgForDeliverTwiceTest(1))
	verifierSigWG.Wait() // got the new view msg
	leaderMonWG.Wait()   // changed view and created the view

	// Starting another view change

	startTime = time.Now()
	vc.StartViewChange(2, true)

	viewChangeMsg2 := proto.Clone(viewChangeMsg).(*protos.Message)
	viewChangeMsg2.GetViewChange().NextView = 2
	vc.HandleMessage(1, viewChangeMsg2)
	vc.HandleMessage(2, viewChangeMsg2)
	vc.HandleMessage(3, viewChangeMsg2)
	commSendWG.Add(1) // sending view data msg and joining the view change
	vc.HandleMessage(5, viewChangeMsg2)
	commSendWG.Wait()

	// Again there is a view change timeout
	// The synchronizer returns the decision

	synchronizerWG.Add(1)
	commSendWG.Add(6) // fetch and send state after sync
	leaderMonWG.Add(1)
	ticker <- startTime.Add(5 * time.Second)
	ticker <- startTime.Add(10 * time.Second)
	ticker <- startTime.Add(9 * time.Second)
	ticker <- startTime.Add(12 * time.Second)
	ticker <- startTime.Add(15 * time.Second)
	ticker <- startTime.Add(20 * time.Second)
	ticker <- startTime.Add(25 * time.Second) // view change timeout (with backoff)
	synchronizerWG.Wait()
	commSendWG.Wait()
	leaderMonWG.Wait() // change view after sync returns
	synchronizer.AssertNumberOfCalls(t, "Sync", 2)

	// The view change continues

	viewChangeMsg3 := proto.Clone(viewChangeMsg).(*protos.Message)
	viewChangeMsg3.GetViewChange().NextView = 3
	vc.HandleMessage(1, viewChangeMsg3)
	vc.HandleMessage(2, viewChangeMsg3)
	vc.HandleMessage(3, viewChangeMsg3)
	vc.HandleMessage(5, viewChangeMsg3)

	// The node receives a view data message which includes the decision

	verifierSigWG.Add(1)
	vc.HandleMessage(1, viewDataMsgSeq1View3)
	verifierSigWG.Wait()

	controller.Stop()
	vc.Stop()
	collector.Stop()
	wal.Close()
}

var (
	prePrepareSeq1View0 = &protos.Message{
		Content: &protos.Message_PrePrepare{
			PrePrepare: &protos.PrePrepare{
				View: 0,
				Seq:  1,
				Proposal: &protos.Proposal{
					Header:  []byte{0},
					Payload: []byte{1},
					Metadata: bft.MarshalOrPanic(&protos.ViewMetadata{
						LatestSequence: 1,
						ViewId:         0,
					}),
					VerificationSequence: 1,
				},
			},
		},
	}
	proposalSeq1View0 = &types.Proposal{
		Header:  []byte{0},
		Payload: []byte{1},
		Metadata: bft.MarshalOrPanic(&protos.ViewMetadata{
			LatestSequence: 1,
			ViewId:         0,
		}),
		VerificationSequence: 1,
	}
	prepareSeq1View0 = &protos.Message{
		Content: &protos.Message_Prepare{
			Prepare: &protos.Prepare{
				View:   0,
				Seq:    1,
				Digest: proposalSeq1View0.Digest(),
			},
		},
	}
	viewDataSeq1View3    = createViewDataForDeliverTwiceTest(3, 1)
	viewDataMsgSeq1View3 = &protos.Message{
		Content: &protos.Message_ViewData{
			ViewData: &protos.SignedViewData{
				RawViewData: bft.MarshalOrPanic(viewDataSeq1View3),
				Signer:      1,
				Signature:   nil,
			},
		},
	}
)

func createViewDataForDeliverTwiceTest(nextView, lastDecisionSequence uint64) *protos.ViewData {
	lastDecision := types.Proposal{
		Metadata: bft.MarshalOrPanic(&protos.ViewMetadata{
			LatestSequence: lastDecisionSequence,
			ViewId:         0,
		}),
		VerificationSequence: 1,
	}
	lastDecisionSignaturesProtos := []*protos.Signature{{Signer: 1, Value: []byte{4}, Msg: []byte{5}}, {Signer: 2, Value: []byte{4}, Msg: []byte{5}}, {Signer: 3, Value: []byte{4}, Msg: []byte{5}}, {Signer: 4, Value: []byte{4}, Msg: []byte{5}}, {Signer: 5, Value: []byte{4}, Msg: []byte{5}}}
	return &protos.ViewData{
		NextView: nextView,
		LastDecision: &protos.Proposal{
			Header:               lastDecision.Header,
			Payload:              lastDecision.Payload,
			Metadata:             lastDecision.Metadata,
			VerificationSequence: uint64(lastDecision.VerificationSequence),
		},
		LastDecisionSignatures: lastDecisionSignaturesProtos,
	}
}

func createNewViewMsgForDeliverTwiceTest(nextView uint64) *protos.Message {
	q := 5
	vd := createViewDataForDeliverTwiceTest(nextView, 0)
	vdBytes := bft.MarshalOrPanic(vd)

	signed := make([]*protos.SignedViewData, 0)
	for len(signed) < q { // quorum
		msg := &protos.Message{
			Content: &protos.Message_ViewData{
				ViewData: &protos.SignedViewData{
					RawViewData: vdBytes,
					Signer:      uint64(len(signed) + 1),
					Signature:   nil,
				},
			},
		}
		signed = append(signed, msg.GetViewData())
	}
	return &protos.Message{
		Content: &protos.Message_NewView{
			NewView: &protos.NewView{
				SignedViewData: signed,
			},
		},
	}
}

// stoppedController returns a started-then-stopped controller, so that nothing
// drains the channels its Proposer-facing methods write to.
func stoppedController(t *testing.T) *bft.Controller {
	t.Helper()

	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()

	app := &mocks.ApplicationMock{}
	app.On("Deliver", mock.Anything, mock.Anything).Return(types.Reconfig{})
	batcher := &mocks.Batcher{}
	batcher.On("Close")
	pool := &mocks.RequestPool{}
	pool.On("Close")
	leaderMon := &mocks.LeaderMonitor{}
	leaderMon.On("ChangeRole", mock.Anything, mock.Anything, mock.Anything)
	leaderMon.On("Close")
	comm := &mocks.CommMock{}
	comm.On("SendConsensus", mock.Anything, mock.Anything)
	verifier := &mocks.VerifierMock{}
	verifier.On("VerificationSequence").Return(uint64(1))

	startedWG := sync.WaitGroup{}
	startedWG.Add(1)

	controller := &bft.Controller{
		Checkpoint:    &types.Checkpoint{},
		Batcher:       batcher,
		RequestPool:   pool,
		LeaderMonitor: leaderMon,
		ID:            1,
		N:             4,
		NodesList:     []uint64{1, 2, 3, 4},
		Logger:        log,
		Application:   app,
		Comm:          comm,
		Verifier:      verifier,
		StartedWG:     &startedWG,
	}
	controller.Deliver = app

	configureProposerBuilder(controller)

	controller.Start(1, 0, 0, false)
	controller.Stop()

	return controller
}

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

func TestAbortViewAfterControllerStopped(t *testing.T) {
	controller := stoppedController(t)

	// abortViewChan holds one, so only the second send exposes the missing guard.
	assert.True(t, returnsWithin(10*time.Second, func() {
		controller.AbortView(1)
		controller.AbortView(1)
	}), "AbortView did not return after the controller was stopped")
}

func TestViewChangedAfterControllerStopped(t *testing.T) {
	controller := stoppedController(t)

	assert.True(t, returnsWithin(10*time.Second, func() {
		controller.ViewChanged(2, 1)
		controller.ViewChanged(3, 2)
	}), "ViewChanged did not return after the controller was stopped")
}

func TestAbortViewWhileADecisionIsPending(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()

	batcher := &mocks.Batcher{}
	batcher.On("Close")
	batcher.On("Closed").Return(false)
	batcher.On("Reset")
	pool := &mocks.RequestPool{}
	pool.On("Close")
	pool.On("RemoveRequest", mock.Anything).Return(nil)
	leaderMon := &mocks.LeaderMonitor{}
	leaderMon.On("ChangeRole", mock.Anything, mock.Anything, mock.Anything)
	leaderMon.On("Close")
	comm := &mocks.CommMock{}
	comm.On("SendConsensus", mock.Anything, mock.Anything)
	verifier := &mocks.VerifierMock{}
	verifier.On("VerificationSequence").Return(uint64(1))
	app := &mocks.ApplicationMock{}

	startedWG := sync.WaitGroup{}
	startedWG.Add(1)

	controller := &bft.Controller{
		Checkpoint:    &types.Checkpoint{},
		Batcher:       batcher,
		RequestPool:   pool,
		LeaderMonitor: leaderMon,
		ID:            1,
		N:             4,
		NodesList:     []uint64{1, 2, 3, 4},
		Logger:        log,
		Application:   app,
		Comm:          comm,
		Verifier:      verifier,
		StartedWG:     &startedWG,
	}
	controller.Deliver = app

	views := make(chan bft.Proposer, 4)
	pb := &mocks.ProposerBuilder{}
	vs := &atomic.Value{}
	pb.On("NewProposer", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(func(a uint64, b uint64, c uint64, d uint64, e int) bft.Proposer {
			view := createView(controller, a, b, c, d, e, vs)
			views <- view
			return view
		}, bft.Phase(bft.COMMITTED))
	controller.ProposerBuilder = pb

	decideReturned := make(chan struct{})
	deliverCalled := make(chan struct{})
	var deliverOnce sync.Once
	app.On("Deliver", mock.Anything, mock.Anything).Run(func(mock.Arguments) {
		deliverOnce.Do(func() {
			// We are on the controller's run goroutine, between taking the decision off
			// decisionChan and answering it: abort the view that produced it and let the
			// decider walk away, so nothing is left to receive the answer.
			view := <-views
			view.Abort()
			<-decideReturned
			close(deliverCalled)
		})
	}).Return(types.Reconfig{})

	controller.Start(1, 0, 0, false)
	defer controller.Stop()

	go func() {
		controller.Decide(types.Proposal{}, nil, nil)
		close(decideReturned)
	}()

	select {
	case <-deliverCalled:
	case <-time.After(10 * time.Second):
		t.Fatal("the decision was never delivered")
	}

	assert.True(t, returnsWithin(10*time.Second, func() {
		controller.AbortView(1)
		controller.AbortView(1)
	}), "AbortView did not return; the run loop is still stuck answering an abandoned decision")
}

type startedView struct {
	view      uint64
	decisions uint64
}

// nextStartedView returns the next view the controller started, or fails the test.
func nextStartedView(t *testing.T, started <-chan startedView) startedView {
	t.Helper()
	select {
	case sv := <-started:
		return sv
	case <-time.After(10 * time.Second):
		t.Fatal("the controller did not start another view")
		return startedView{}
	}
}

func TestDecisionOfAnAbortedViewDoesNotCountInItsReplacement(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()

	batcher := &mocks.Batcher{}
	batcher.On("Close")
	batcher.On("Closed").Return(false)
	batcher.On("Reset")
	pool := &mocks.RequestPool{}
	pool.On("Close")
	pool.On("RemoveRequest", mock.Anything).Return(nil)
	pool.On("RestartTimers")
	leaderMon := &mocks.LeaderMonitor{}
	leaderMon.On("ChangeRole", mock.Anything, mock.Anything, mock.Anything)
	leaderMon.On("Close")
	comm := &mocks.CommMock{}
	comm.On("SendConsensus", mock.Anything, mock.Anything)
	verifier := &mocks.VerifierMock{}
	verifier.On("VerificationSequence").Return(uint64(1))
	app := &mocks.ApplicationMock{}

	startedWG := sync.WaitGroup{}
	startedWG.Add(1)

	controller := &bft.Controller{
		Checkpoint:         &types.Checkpoint{},
		Batcher:            batcher,
		RequestPool:        pool,
		LeaderMonitor:      leaderMon,
		ID:                 1,
		N:                  4,
		NodesList:          []uint64{1, 2, 3, 4},
		LeaderRotation:     true,
		DecisionsPerLeader: 1,
		Logger:             log,
		Application:        app,
		Comm:               comm,
		Verifier:           verifier,
		StartedWG:          &startedWG,
	}
	controller.Deliver = app

	started := make(chan startedView, 8)
	holdView2 := make(chan struct{})
	var holdOnce sync.Once
	pb := &mocks.ProposerBuilder{}
	vs := &atomic.Value{}
	pb.On("NewProposer", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(func(leader uint64, seq uint64, viewNum uint64, decisions uint64, quorum int) bft.Proposer {
			started <- startedView{view: viewNum, decisions: decisions}
			if viewNum == 2 {
				// Park the run loop here: view 1 is aborted but not yet replaced, which is
				// where Decide has to pick up the abort channel for the interleaving below.
				holdOnce.Do(func() { <-holdView2 })
			}
			return createView(controller, leader, seq, viewNum, decisions, quorum, vs)
		}, bft.Phase(bft.COMMITTED))
	controller.ProposerBuilder = pb

	deliverCalled := make(chan struct{})
	app.On("Deliver", mock.Anything, mock.Anything).Run(func(mock.Arguments) {
		close(deliverCalled)
	}).Return(types.Reconfig{})

	controller.Start(1, 0, 0, false)
	defer controller.Stop()

	assert.Equal(t, startedView{view: 1, decisions: 0}, nextStartedView(t, started))

	controller.ViewChanged(2, 1)
	assert.Equal(t, startedView{view: 2, decisions: 0}, nextStartedView(t, started))

	// View 1 decides while its own abort channel is already closed: the decision lands in
	// the buffer and the deciding goroutine walks away without waiting for an answer.
	decided := make(chan struct{})
	go func() {
		controller.Decide(types.Proposal{
			Metadata: bft.MarshalOrPanic(&protos.ViewMetadata{ViewId: 1, LatestSequence: 1}),
		}, nil, nil)
		close(decided)
	}()

	select {
	case <-decided:
	case <-time.After(10 * time.Second):
		t.Fatal("Decide did not return although the view it decided in was aborted")
	}

	close(holdView2) // let view 2 start; the run loop then picks up the buffered decision

	select {
	case <-deliverCalled:
	case <-time.After(10 * time.Second):
		t.Fatal("the decision was never delivered")
	}

	// The run loop is sequential, so anything decide does with the stale decision happens
	// before this view change: the next view started must be view 3 with a fresh counter.
	controller.ViewChanged(3, 5)
	assert.Equal(t, startedView{view: 3, decisions: 0}, nextStartedView(t, started),
		"decide applied the aborted view's decision count to the view that replaced it")
}
