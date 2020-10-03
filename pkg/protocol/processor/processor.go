package processor

import (
	"errors"
	"time"

	"github.com/iotaledger/hive.go/events"
	"github.com/iotaledger/hive.go/objectstorage"
	"github.com/iotaledger/hive.go/workerpool"

	iotago "github.com/iotaledger/iota.go"

	"github.com/gohornet/hornet/pkg/metrics"
	"github.com/gohornet/hornet/pkg/model/hornet"
	"github.com/gohornet/hornet/pkg/model/tangle"
	"github.com/gohornet/hornet/pkg/peering"
	"github.com/gohornet/hornet/pkg/peering/peer"
	"github.com/gohornet/hornet/pkg/profile"
	"github.com/gohornet/hornet/pkg/protocol/bqueue"
	"github.com/gohornet/hornet/pkg/protocol/message"
	"github.com/gohornet/hornet/pkg/protocol/rqueue"
	"github.com/gohornet/hornet/pkg/protocol/sting"
)

const (
	WorkerQueueSize = 50000
)

var (
	workerCount         = 64
	ErrInvalidTimestamp = errors.New("invalid timestamp")
)

// New creates a new processor which parses messages.
func New(requestQueue rqueue.Queue, peerManager *peering.Manager, opts *Options) *Processor {
	proc := &Processor{
		pm:           peerManager,
		requestQueue: requestQueue,
		Events: Events{
			MessageProcessed: events.NewEvent(MessageProcessedCaller),
			BroadcastMessage: events.NewEvent(BroadcastCaller),
		},
		opts: *opts,
	}
	wuCacheOpts := opts.WorkUnitCacheOpts
	proc.workUnits = objectstorage.New(
		nil,
		workUnitFactory,
		objectstorage.CacheTime(time.Duration(wuCacheOpts.CacheTimeMs)),
		objectstorage.PersistenceEnabled(false),
		objectstorage.KeysOnly(true),
		objectstorage.StoreOnCreation(false),
		objectstorage.LeakDetectionEnabled(wuCacheOpts.LeakDetectionOptions.Enabled,
			objectstorage.LeakDetectionOptions{
				MaxConsumersPerObject: wuCacheOpts.LeakDetectionOptions.MaxConsumersPerObject,
				MaxConsumerHoldTime:   time.Duration(wuCacheOpts.LeakDetectionOptions.MaxConsumerHoldTimeSec) * time.Second,
			}),
	)

	proc.wp = workerpool.New(func(task workerpool.Task) {
		p := task.Param(0).(*peer.Peer)
		data := task.Param(2).([]byte)

		switch task.Param(1).(message.Type) {
		case sting.MessageTypeMessage:
			proc.processMessage(p, data)
		case sting.MessageTypeMessageRequest:
			proc.processMessageRequest(p, data)
		case sting.MessageTypeMilestoneRequest:
			proc.processMilestoneRequest(p, data)
		}

		task.Return(nil)
	}, workerpool.WorkerCount(workerCount), workerpool.QueueSize(WorkerQueueSize))

	return proc
}

func MessageProcessedCaller(handler interface{}, params ...interface{}) {
	handler.(func(msg *tangle.Message, request *rqueue.Request, p *peer.Peer))(params[0].(*tangle.Message), params[1].(*rqueue.Request), params[2].(*peer.Peer))
}

func BroadcastCaller(handler interface{}, params ...interface{}) {
	handler.(func(b *bqueue.Broadcast))(params[0].(*bqueue.Broadcast))
}

// Events are the events fired by the Processor.
type Events struct {
	// Fired when a message was fully processed.
	MessageProcessed *events.Event
	// Fired when a message is meant to be broadcasted.
	BroadcastMessage *events.Event
}

// Processor processes submitted messages in parallel and fires appropriate completion events.
type Processor struct {
	Events       Events
	pm           *peering.Manager
	wp           *workerpool.WorkerPool
	requestQueue rqueue.Queue
	workUnits    *objectstorage.ObjectStorage
	opts         Options
}

// The Options for the Processor.
type Options struct {
	ValidMWM          uint64
	WorkUnitCacheOpts profile.CacheOpts
}

// Run runs the processor and blocks until the shutdown signal is triggered.
func (proc *Processor) Run(shutdownSignal <-chan struct{}) {
	proc.wp.Start()
	<-shutdownSignal
	proc.wp.StopAndWait()
}

// Process submits the given message to the processor for processing.
func (proc *Processor) Process(p *peer.Peer, msgType message.Type, data []byte) {
	proc.wp.Submit(p, msgType, data)
}

// SerializeAndEmit serializes the given message and emits MessageProcessed and BroadcastMessage events.
func (proc *Processor) SerializeAndEmit(msg *tangle.Message, deSeriMode iotago.DeSerializationMode) error {

	msgData, err := msg.GetMessage().Serialize(deSeriMode)
	if err != nil {
		return err
	}

	proc.Events.MessageProcessed.Trigger(msg, (*rqueue.Request)(nil), (*peer.Peer)(nil))
	proc.Events.BroadcastMessage.Trigger(&bqueue.Broadcast{MsgData: msgData})

	return nil
}

// WorkUnitSize returns the size of WorkUnits currently cached.
func (proc *Processor) WorkUnitsSize() int {
	return proc.workUnits.GetSize()
}

// gets a CachedWorkUnit or creates a new one if it not existent.
func (proc *Processor) workUnitFor(receivedTxBytes []byte) *CachedWorkUnit {
	return &CachedWorkUnit{
		proc.workUnits.ComputeIfAbsent(receivedTxBytes, func(key []byte) objectstorage.StorableObject { // cachedWorkUnit +1
			return newWorkUnit(receivedTxBytes)
		}),
	}
}

// processes the given milestone request by parsing it and then replying to the peer with it.
func (proc *Processor) processMilestoneRequest(p *peer.Peer, data []byte) {
	msIndex, err := sting.ExtractRequestedMilestoneIndex(data)
	if err != nil {
		metrics.SharedServerMetrics.InvalidRequests.Inc()

		// drop the connection to the peer
		proc.pm.Remove(p.ID)
		return
	}

	// peers can request the latest milestone we know
	if msIndex == sting.LatestMilestoneRequestIndex {
		msIndex = tangle.GetLatestMilestoneIndex()
	}

	cachedMessage := tangle.GetMilestoneCachedMessageOrNil(msIndex) // message +1
	if cachedMessage == nil {
		// can't reply if we don't have the wanted milestone
		return
	}
	defer cachedMessage.Release(true) // message -1

	cachedRequestedData, err := cachedMessage.GetMessage().GetMessage().Serialize(iotago.DeSeriModeNoValidation)
	if err != nil {
		// can't reply if serialization fails
		return
	}

	msg, err := sting.NewMessageMsg(cachedRequestedData)
	if err != nil {
		// can't reply if serialization fails
		return
	}

	p.EnqueueForSending(msg)
}

// processes the given message request by parsing it and then replying to the peer with it.
func (proc *Processor) processMessageRequest(p *peer.Peer, data []byte) {
	if len(data) != 32 {
		return
	}

	cachedMessage := tangle.GetCachedMessageOrNil(hornet.MessageIDFromBytes(data)) // message +1
	if cachedMessage == nil {
		// can't reply if we don't have the requested message
		return
	}
	defer cachedMessage.Release(true) // message -1

	cachedRequestedData, err := cachedMessage.GetMessage().GetMessage().Serialize(iotago.DeSeriModeNoValidation)
	if err != nil {
		// can't reply if serialization fails
		return
	}

	msg, err := sting.NewMessageMsg(cachedRequestedData)
	if err != nil {
		// can't reply if serialization fails
		return
	}

	p.EnqueueForSending(msg)
}

// gets or creates a new WorkUnit for the given message and then processes the WorkUnit.
func (proc *Processor) processMessage(p *peer.Peer, data []byte) {
	cachedWorkUnit := proc.workUnitFor(data) // workUnit +1
	defer cachedWorkUnit.Release()           // workUnit -1
	workUnit := cachedWorkUnit.WorkUnit()
	workUnit.addReceivedFrom(p, nil)
	proc.processWorkUnit(workUnit, p)
}

// tries to process the WorkUnit by first checking in what state it is.
// if the WorkUnit is invalid (because the underlying message is invalid), the given peer is punished.
// if the WorkUnit is already completed, and the message was requested, this function emits a MessageProcessed event.
// it is safe to call this function for the same WorkUnit multiple times.
func (proc *Processor) processWorkUnit(wu *WorkUnit, p *peer.Peer) {
	wu.processingLock.Lock()

	switch {
	case wu.Is(Hashing):
		wu.processingLock.Unlock()
		return
	case wu.Is(Invalid):
		wu.processingLock.Unlock()

		metrics.SharedServerMetrics.InvalidMessages.Inc()

		// drop the connection to the peer
		proc.pm.Remove(p.ID)

		return
	case wu.Is(Hashed):
		wu.processingLock.Unlock()

		// emit an event to say that a message was fully processed
		if request := proc.requestQueue.Received(wu.msg.GetMessageID()); request != nil {
			proc.Events.MessageProcessed.Trigger(wu.msg, request, p)
			return
		}

		if tangle.ContainsMessage(wu.msg.GetMessageID()) {
			metrics.SharedServerMetrics.KnownMessages.Inc()
			p.Metrics.KnownMessages.Inc()
			return
		}

		return
	}

	wu.UpdateState(Hashing)
	wu.processingLock.Unlock()

	// build Hornet representation of the message
	msg, err := tangle.MessageFromBytes(wu.receivedMsgBytes, iotago.DeSeriModePerformValidation)
	if err != nil {
		wu.UpdateState(Invalid)
		wu.punish()
		return
	}

	// mark the message as received
	request := proc.requestQueue.Received(msg.GetMessageID())

	/*
		// ToDo:
		// validate minimum weight magnitude requirement
		if request == nil && !message.HasValidNonce(msg, proc.opts.ValidMWM) {
			wu.UpdateState(Invalid)
			wu.punish()
			return
		}
	*/

	wu.dataLock.Lock()
	wu.msg = msg
	wu.dataLock.Unlock()

	wu.UpdateState(Hashed)

	// check the existence of the message before broadcasting it
	containsTx := tangle.ContainsMessage(msg.GetMessageID())

	proc.Events.MessageProcessed.Trigger(msg, request, p)

	// increase the known message count for all other peers
	wu.increaseKnownTxCount(p)

	// ToDo: broadcast on solidification
	// broadcast the message if it wasn't requested and not known yet
	if request == nil && !containsTx {
		proc.Events.BroadcastMessage.Trigger(wu.broadcast())
	}
}
