// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"math/rand"
	"sort"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	// "github.com/pingcap-incubator/tinykv/scheduler/pkg/slice"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters（参数） to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	// 选举间隔时长，需要超过心跳时长
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	// Leader 发送心跳维持地位
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
// Leader 维持 Followers entries 的进度
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval（心跳间隔）, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// 距离上次心跳的 tick 次数
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	randomElectionTimeout int
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	r := &Raft{
		id:               c.ID,
		Prs:              make(map[uint64]*Progress),
		votes:            make(map[uint64]bool),
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		RaftLog:          newLog(c.Storage),
	}
	hardState, confState, _ := r.RaftLog.storage.InitialState()
	if c.peers == nil {
		c.peers = confState.Nodes
	}
	lastIndex := r.RaftLog.LastIndex()
	for _, peer := range c.peers {
		if peer == r.id {
			r.Prs[peer] = &Progress{
				Next:  lastIndex + 1,
				Match: lastIndex,
			}
		} else {
			r.Prs[peer] = &Progress{
				Next: lastIndex + 1,
			}
		}
	}
	r.becomeFollower(0, None)
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.Term, r.Vote, r.RaftLog.committed = hardState.GetTerm(), hardState.GetVote(), hardState.GetCommit()
	if c.Applied > 0 {
		r.RaftLog.applied = c.Applied
	}
	return r
}

func (r *Raft) sendAppendResponse(to uint64, reject bool, term, index uint64) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Reject:  reject,
		LogTerm: term,
		Index:   index,
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) sendHeartbeatResponse(to uint64, reject bool) {
	msg := pb.Message {
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From: r.id,
		To: to,
		Term: r.Term,
		Reject: reject,
	}
	r.msgs = append(r.msgs, msg)
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	prevIndex := r.Prs[to].Next - 1
	prevLogTerm, err := r.RaftLog.Term(prevIndex)
	if err != nil {
		if err == ErrCompacted {
			// 该索引位置的日志已经被压缩
			return false
		}
		panic(err)
	}
	var entries []*pb.Entry
	n := len(r.RaftLog.entries)
	for i := r.RaftLog.toSliceIndex(prevIndex + 1); i < n; i++ {
		entries = append(entries, &r.RaftLog.entries[i])
	}

	m := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: prevLogTerm,
		Commit:  r.RaftLog.committed,
		Index:   prevIndex,
		Entries: entries,
	}
	r.msgs = append(r.msgs, m)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	m := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
	}
	r.msgs = append(r.msgs, m)
}

func (r *Raft) sendRequestVote(to, index, term uint64) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: term,
		Index:   index,
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) sendRequestVoteResponse(to uint64, reject bool) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From: r.id,
		To: to,
		Term: r.Term,
		Reject: reject,
	}
	r.msgs = append(r.msgs, msg)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower, StateCandidate:
		r.electionElapsed++
		if r.electionElapsed >= r.randomElectionTimeout {
			r.electionElapsed = 0
			_ = r.Step(pb.Message{
				MsgType: pb.MessageType_MsgHup,
				From:    r.id,
				Term:    r.Term,
			})
		}
	case StateLeader:
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			_ = r.Step(pb.Message{
				MsgType: pb.MessageType_MsgBeat,
				To:      r.id,
				Term:    r.Term,
			})

		}
	}
}

func (r *Raft) resetTime() {
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.votes = make(map[uint64]bool, 0)
	r.Lead = None
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.resetTime()
	r.State = StateFollower
	r.Lead = lead
	r.Term = term
	r.Vote = None

}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.resetTime()
	r.State = StateCandidate
	r.Term++
	r.Lead = None
	r.Vote = r.id
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
	if len(r.Prs) <= 1 {
		r.becomeLeader()
	}
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.Lead = r.id
	lastIndex := r.RaftLog.LastIndex()
	r.heartbeatElapsed = 0
	for peer := range r.Prs {
		if peer == r.id {
			r.Prs[peer].Next = lastIndex + 2
			r.Prs[peer].Match = lastIndex + 1
		} else {
			r.Prs[peer].Next = lastIndex + 1
		}
	}
	r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{
		Term:  r.Term,
		Index: r.RaftLog.LastIndex() + 1,
	})
	r.bcastAppend()
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.Prs[r.id].Match
	}
}

// Step the entrance（入口） of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
	}

	switch r.State {
	case StateFollower:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.doElection()
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		}
	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.doElection()
		case pb.MessageType_MsgAppend:
			r.becomeFollower(r.Term, m.From)
		case pb.MessageType_MsgRequestVoteResponse:
			r.votes[m.From] = !m.Reject
			agree, reject := 0, 0
			for _, v := range r.votes {
				if v {
					agree++
				} else {
					reject++
				}
			}
			if len(r.Prs) == 1 || agree > len(r.Prs)/2 {
				r.becomeLeader()
				for p := range r.Prs {
					if p != r.id {
						r.sendHeartbeat(p)
					}
				}
			}
			if reject > len(r.Prs)/2 {
				r.becomeFollower(r.Term, None)
			}
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgHeartbeat:
			if m.Term == r.Term {
				r.becomeFollower(m.Term, m.From)
			}
			r.handleHeartbeat(m)
		}
		
	case StateLeader:
		switch m.MsgType {
		case pb.MessageType_MsgBeat:
			for p := range r.Prs {
				if p != r.id {
					r.sendHeartbeat(p)
				}
			}
		case pb.MessageType_MsgPropose:
			r.appendEntries(m.Entries)
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgAppendResponse:
			r.handleAppendEntriesResponse(m)
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgHeartbeatResponse:
			r.sendAppend(m.From)
		}
	}

	// if m.MsgType == pb.MessageType_MsgRequestVote {
	// 	reject := true
	// 	if ((r.Vote == None && r.Lead == m.From) || r.Vote == m.From) &&
	// 		(m.LogTerm > r.RaftLog.LastTerm() || (m.LogTerm == r.RaftLog.LastTerm() && m.Index >= r.RaftLog.LastIndex())) {
	// 		reject = false
	// 		r.Vote = m.From
	// 	}
	// 	msg := pb.Message{
	// 		MsgType: pb.MessageType_MsgRequestVoteResponse,
	// 		To:      m.From,
	// 		From:    r.id,
	// 		Term:    r.Term,
	// 		Reject:  reject,
	// 	}
	// 	r.msgs = append(r.msgs, msg)
	// }
	return nil
}

func (r *Raft) bcastAppend() {
	for peer := range r.Prs {
		if peer == r.id {
			continue
		}
		r.sendAppend(peer)
	}
}

func (r *Raft) doElection() {
	r.becomeCandidate()
	r.heartbeatElapsed = 0
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	// if len(r.Prs) == 1 {
	// 	r.becomeLeader()
	//	return
	// }
	lastIndex := r.RaftLog.LastIndex()
	lastLogTerm, _ := r.RaftLog.Term(lastIndex)
	for peer := range r.Prs {
		if peer == r.id {
			continue
		}
		r.sendRequestVote(peer, lastIndex, lastLogTerm)
	}
}

func (r *Raft) appendEntries(entries []*pb.Entry) {
	lastIndex := r.RaftLog.LastIndex()
	for i, entry := range entries {
		entry.Term = r.Term
		entry.Index = lastIndex + uint64(i) + 1
		if entry.EntryType == pb.EntryType_EntryConfChange {
			// 配置文件更改

		}
		r.RaftLog.entries = append(r.RaftLog.entries, *entry)
	}
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.Prs[r.id].Match + 1
	r.bcastAppend()
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.Prs[r.id].Match
	}
}

func (r *Raft) handleRequestVote(m pb.Message) {
	if m.Term != None && m.Term < r.Term {
		r.sendRequestVoteResponse(m.From, true)
		return
	}
	if r.Vote != None && r.Vote != m.From {
		r.sendRequestVoteResponse(m.From, true)
		return
	}
	lastIndex := r.RaftLog.LastIndex()
	lastLogTerm,_ := r.RaftLog.Term(lastIndex)
	if lastLogTerm > m.LogTerm ||
	 	lastLogTerm == m.LogTerm && lastIndex > m.Index{
		r.sendRequestVoteResponse(m.From, true)
		return
	}
	r.Vote = m.From
	r.electionElapsed = 0
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.sendRequestVoteResponse(m.From, false)
}

func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	if m.Term != None && m.Term < r.Term {
		return
	}
	if m.Reject {
		index := m.Index
		if index == None {
			return
		}
		if m.LogTerm != None {
			logTerm := m.LogTerm
			l := r.RaftLog
			sliceIndex := sort.Search(len(l.entries), 
				func(i int) bool {return l.entries[i].Term > logTerm})
			if sliceIndex > 0 && l.entries[sliceIndex-1].Term == logTerm {
				index = l.toEntryIndex(sliceIndex)
			}
		}
		r.Prs[m.From].Next = index
		r.sendAppend(m.From)
		return 
	}
	if m.Index > r.Prs[m.From].Match {
		r.Prs[m.From].Match = m.Index
		r.Prs[m.From].Next = m.Index + 1
		r.leaderCommit()
		if m.From == r.leadTransferee && m.Index == r.RaftLog.LastIndex() {
			
		}
		
	}
}

func (r *Raft) leaderCommit() {
	match := make(uint64Slice, len(r.Prs))
	i := 0
	for _, prs := range r.Prs {
		match[i] = prs.Match
		i++
	}
	sort.Sort(match)
	n := match[(len(r.Prs)-1)/2]

	if n > r.RaftLog.committed {
		logTerm, err := r.RaftLog.Term(n)
		if err != nil {
			panic(err)
		}
		if logTerm == r.Term {
			r.RaftLog.committed = n
			r.bcastAppend()
		}
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	if m.Term != None && m.Term < r.Term {
		r.sendAppendResponse(m.From, true, None, None)
		return
	}
	r.electionElapsed = 0
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.Lead = m.From
	l := r.RaftLog
	lastIndex := l.LastIndex()
	if m.Index > lastIndex {
		r.sendAppendResponse(m.From, true, None, lastIndex+1)
		return
	}
	if m.Index >= l.FirstIndex {
		logTerm, err := l.Term(m.Index)
		if err != nil {
			panic(err)
		}
		if logTerm != m.LogTerm {
			index := l.toEntryIndex(sort.Search(l.toSliceIndex(m.Index+1),
				func(i int) bool { return l.entries[i].Term == logTerm }  )  )
			
			r.sendAppendResponse(m.From, true, logTerm, index)
			return
		}
	}

	for i, entry := range m.Entries {
		if entry.Index < l.FirstIndex {
			continue
		}
		if entry.Index <= l.LastIndex() {
			logTerm, err := l.Term(entry.Index)
			if err != nil {
				panic(err)
			}
			if logTerm != entry.Term {
				idx := l.toSliceIndex(entry.Index)
				l.entries[idx] = *entry
				l.entries = l.entries[:idx+1]
				l.stabled = min(l.stabled, entry.Index-1)
			}
		} else {
			n := len(m.Entries)
			for j := i; j < n; j++ {
				l.entries = append(l.entries, *m.Entries[j])
			}
			break
		}
	}
	if m.Commit > l.committed {
		l.committed = min(m.Commit, m.Index+uint64(len(m.Entries)))
	}
	r.sendAppendResponse(m.From, false, None, l.LastIndex())
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	if m.Term != None && m.Term < r.Term {
		r.sendHeartbeatResponse(m.From, true)
		return
	}
	r.Lead = m.From
	r.electionElapsed = 0
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.sendHeartbeatResponse(m.From, false)
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
