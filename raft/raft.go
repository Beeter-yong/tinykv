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

	// "github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
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

// Config contains the parameters to start a raft.
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
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
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

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
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
	PendingConfIndex      uint64
	randomElectionTimeout int
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	raft := &Raft{
		id:      c.ID,
		RaftLog: newLog(c.Storage),
		Prs:     make(map[uint64]*Progress),
		State:   StateFollower,
		votes:   make(map[uint64]bool),
		// msgs:                  make([]pb.Message, 0),
		Lead:                  None,
		heartbeatTimeout:      c.HeartbeatTick,
		electionTimeout:       c.ElectionTick,
		randomElectionTimeout: c.ElectionTick + rand.Intn(c.ElectionTick),
	}

	hardState, confState, _ := c.Storage.InitialState()
	raft.Term = hardState.Term
	raft.Vote = hardState.Vote
	raft.RaftLog.committed = hardState.Commit

	if c.peers == nil {
		c.peers = confState.Nodes
	}
	if c.Applied > 0 {
		raft.RaftLog.applied = c.Applied
	}
	for _, p := range c.peers {
		// 如果是重启需要考虑初始化
		if p == raft.id {
			raft.Prs[p] = &Progress{
				Next:  raft.RaftLog.LastIndex() + 1,
				Match: raft.RaftLog.LastIndex(),
			}
		} else {
			raft.Prs[p] = &Progress{
				Next: raft.RaftLog.LastIndex() + 1,
			}
		}
	}

	return raft
}

func (r *Raft) getHardState() pb.HardState {
	hardState := pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
	return hardState
}

func (r *Raft) getSoftState() *SoftState {
	softState := &SoftState{
		Lead: r.id,
	}
	return softState
}

func (r *Raft) sendTimeoutNow(to uint64) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgTimeoutNow,
		From:    r.id,
		To:      to,
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) sendSnapshot(to uint64) {
	snapshot, err := r.RaftLog.storage.Snapshot()
	if err != nil {
		return
	}
	msg := pb.Message{
		MsgType:  pb.MessageType_MsgSnapshot,
		From:     r.id,
		To:       to,
		Term:     r.Term,
		Snapshot: &snapshot,
	}
	r.msgs = append(r.msgs, msg)
	r.Prs[to].Next = snapshot.Metadata.Index + 1
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	if r.Prs[to].Next == 0 {
		r.Prs[to].Next = 1
	}
	preIndex := r.Prs[to].Next - 1
	logTerm, err := r.RaftLog.Term(preIndex)

	if err != nil {
		if err == ErrCompacted {
			r.sendSnapshot(to)
			return false
		}
		panic(err)
	}

	var entries []*pb.Entry
	for i := int(preIndex + 1 - r.RaftLog.FirstIndex); i < len(r.RaftLog.entries); i++ {
		entries = append(entries, &r.RaftLog.entries[i])
	}
	mes := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		LogTerm: logTerm,
		Index:   preIndex,
		Commit:  r.RaftLog.committed,
		Entries: entries,
	}
	r.msgs = append(r.msgs, mes)
	return true
}

func (r *Raft) sendAppendResponse(to uint64, reject bool) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Reject:  reject,
		Index:   r.RaftLog.LastIndex(),
	}
	r.msgs = append(r.msgs, msg)
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	mes := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
	}
	r.msgs = append(r.msgs, mes)
}

func (r *Raft) sendHeartbeatResponse(to uint64, reject bool) {
	mes := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Reject:  reject,
	}
	r.msgs = append(r.msgs, mes)
}

func (r *Raft) sendRequestVote(to uint64) {
	mes := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: r.RaftLog.LastTerm(),
		Index:   r.RaftLog.LastIndex(),
	}
	r.msgs = append(r.msgs, mes)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	r.electionElapsed++
	r.heartbeatElapsed++
	switch r.State {
	case StateFollower:
		if r.electionElapsed >= r.randomElectionTimeout {
			r.electionElapsed = 0
			msg := pb.Message{
				MsgType: pb.MessageType_MsgHup,
			}
			r.Step(msg)
		}
	case StateCandidate:
		if r.electionElapsed >= r.randomElectionTimeout {
			r.electionElapsed = 0
			msg := pb.Message{
				MsgType: pb.MessageType_MsgHup,
			}
			r.Step(msg)
		}
	case StateLeader:
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			msg := pb.Message{
				MsgType: pb.MessageType_MsgBeat,
				// To: r.id,
				// From: r.id,
			}
			r.Step(msg)
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Term = term
	r.Vote = None
	r.Lead = lead
	r.votes = make(map[uint64]bool)
	r.electionElapsed = 0
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	// 当选举时间超时就会使得 Follower 变为 Candidate,并投自己一票
	r.State = StateCandidate
	r.Term++
	r.Vote = r.id
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
	r.electionElapsed = 0
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.Lead = r.id
	r.heartbeatElapsed = 0

	lastIndex := r.RaftLog.LastIndex()
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
		Index: lastIndex + 1,
	})
	r.bcastAppend()
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.Prs[r.id].Match
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	// TODO: 这是什么作用
	if _, ok := r.Prs[r.id]; !ok && m.MsgType == pb.MessageType_MsgTimeoutNow {
		return nil
	}

	if m.Term > r.Term {
		r.leadTransferee = None
		r.becomeFollower(m.Term, None)
	}
	switch r.State {
	case StateFollower:
		switch m.MsgType {
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgHup:
			// 当选举间隔超时，则晋升 Candidate 开始一次选举
			r.doElection()
		case pb.MessageType_MsgRequestVote:
			// 处理投票
			r.handleVote(m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgSnapshot:
			r.handleSnapshot(m)
		case pb.MessageType_MsgTransferLeader:
			if r.Lead != None {
				m.To = r.Lead
				r.msgs = append(r.msgs, m)
			}
		case pb.MessageType_MsgTimeoutNow:
			r.doElection()
		}
	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			// 当选举间隔超时，则晋升 Candidate 开始一次选举
			r.doElection()
		case pb.MessageType_MsgRequestVoteResponse:
			// 其他节点响应自己的投票
			r.handleVoteResponse(m)
		case pb.MessageType_MsgRequestVote:
			// 处理投票
			r.handleVote(m)
		case pb.MessageType_MsgAppend:
			// 处理日志
			r.becomeFollower(m.Term, m.From)
			r.handleAppendEntries(m)
		case pb.MessageType_MsgHeartbeat:
			r.becomeFollower(m.Term, m.From)
			r.handleHeartbeat(m)
		case pb.MessageType_MsgSnapshot:
			r.handleSnapshot(m)
		case pb.MessageType_MsgTransferLeader:
			if r.Lead != None {
				m.To = r.Lead
				r.msgs = append(r.msgs, m)
			}
		}

	case StateLeader:
		switch m.MsgType {
		case pb.MessageType_MsgPropose:
			// 客户端发给 Leader 日志条目
			r.appendEntry(m)
		case pb.MessageType_MsgAppendResponse:
			r.handleAppendEntriesResponse(m)
		case pb.MessageType_MsgBeat:
			// 当心跳时间超时后需要广播心跳
			r.bcastHeart()
		case pb.MessageType_MsgRequestVote:
			// 处理投票
			r.handleVote(m)
		case pb.MessageType_MsgHeartbeatResponse:
			r.handleHeartbeatResponse(m)
		case pb.MessageType_MsgSnapshot:
			r.handleSnapshot(m)
		case pb.MessageType_MsgTransferLeader:
			r.handleTransferLeader(m)
		}
	}
	return nil
}

// 开始一次选举
func (r *Raft) doElection() {
	r.becomeCandidate()
	for peer := range r.Prs {
		if peer != r.id {
			r.sendRequestVote(peer)
		}
	}
	if len(r.Prs) == 1 {
		r.becomeLeader()
		return
	}

}

func (r *Raft) bcastHeart() {
	for peer := range r.Prs {
		if peer != r.id {
			r.sendHeartbeat(peer)
		}
	}
}

func (r *Raft) bcastAppend() {
	for peer := range r.Prs {
		if peer != r.id {
			r.sendAppend(peer)
		}
	}
}

func (r *Raft) appendEntry(m pb.Message) {
	entries := m.Entries
	for i, entrie := range entries {
		// 将日志依次加在 RaftLog 中
		entrie.Term = r.Term
		entrie.Index = r.RaftLog.LastIndex() + uint64(i) + 1
		r.RaftLog.entries = append(r.RaftLog.entries, *entrie)
	}
	// 添加日志后更改 Prs 中的记录进度
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.bcastAppend()
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.Prs[r.id].Match
	}
}

func (r *Raft) handleVote(m pb.Message) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Reject:  true,
	}
	if (r.Vote == None || r.Vote == m.From) &&
		(m.LogTerm > r.RaftLog.LastTerm() ||
			(m.LogTerm == r.RaftLog.LastTerm() && m.Index >= r.RaftLog.LastIndex())) {
		msg.Reject = false
		r.Vote = m.From
	}
	r.msgs = append(r.msgs, msg)
}

// Candidate 统计票数
func (r *Raft) handleVoteResponse(m pb.Message) {
	if m.Term != None && m.Term < r.Term {
		return
	}
	r.votes[m.From] = !m.Reject
	agree, reject := 0, 0
	for _, vote := range r.votes {
		if vote == true {
			agree++
		} else {
			reject++
		}
	}
	if len(r.Prs) == 1 || agree > len(r.Prs)/2 {
		r.becomeLeader()
		r.bcastHeart() // 立即开始心跳
	} else if reject > len(r.Prs)/2 {
		r.becomeFollower(m.Term, None)
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	r.electionElapsed = 0
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.Lead = m.From

	lastLogIndex := r.RaftLog.LastIndex()

	if m.Index > lastLogIndex {
		// Follower 缺更多的日志待补全
		r.sendAppendResponse(m.From, true)
		return
	}

	logTerm, _ := r.RaftLog.Term(m.Index)
	if logTerm != m.LogTerm {
		r.sendAppendResponse(m.From, true)
		return
	}
	for _, entry := range m.Entries {
		if entry.Index < r.RaftLog.FirstIndex {
			continue
		}
		if entry.Index <= r.RaftLog.LastIndex() {
			idx := entry.Index - r.RaftLog.FirstIndex
			if int(idx) >= len(r.RaftLog.entries) {
				r.RaftLog.entries = append(r.RaftLog.entries, *entry)
			}
			if r.RaftLog.entries[idx].Term == entry.Term {
				continue
			}
			r.RaftLog.entries[idx] = *entry
			r.RaftLog.entries = r.RaftLog.entries[:idx+1]
			r.RaftLog.stabled = min(r.RaftLog.stabled, entry.Index-1)
		} else {
			r.RaftLog.entries = append(r.RaftLog.entries, *entry)
		}

	}
	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(m.Commit, m.Index+uint64(len(m.Entries)))
	}
	r.sendAppendResponse(m.From, false)

}

func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	if m.Term != None && m.Term < r.Term {
		return
	}
	if m.Reject {
		// Follower 拒绝请求
		r.Prs[m.From].Next = r.Prs[m.From].Next - uint64(1)
		r.sendAppend(m.From)
		return
	}
	// 如果 Forrowler 接收了请求，需要更新该节点的日志记录进度
	r.Prs[m.From].Match = m.Index
	r.Prs[m.From].Next = m.Index + 1
	r.leaderCommit()
	if m.From == r.leadTransferee && m.Index == r.RaftLog.LastIndex() {
		r.sendTimeoutNow(m.From)
		r.leadTransferee = None
	}

}

func (r *Raft) leaderCommit() {
	match := make(uint64Slice, len(r.Prs))
	i := 0
	for peer := range r.Prs {
		match[i] = r.Prs[peer].Match
		i++
	}
	sort.Sort(match)
	n := match[(len(r.Prs)-1)/2] // 取所有节点中一半以上已经 commited 的位置
	if n > r.RaftLog.committed {
		// 更改 Leader 提交的位置
		logTerm, _ := r.RaftLog.Term(n)
		if logTerm == r.Term {
			r.RaftLog.committed = n
			r.bcastAppend()
		}

	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	if m.Term < r.Term {
		r.sendHeartbeatResponse(m.From, true)
		return
	}
	r.Lead = m.From
	r.electionElapsed = 0
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.sendHeartbeatResponse(m.From, false)
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	if m.Term > r.Term {
		return
	}
	r.sendAppend(m.From)
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	// 1. 如果快照的 index 小于自己的 committed，则不能自己不能生成快照，因为提交后的数据才能不再改变
	meta := m.Snapshot.Metadata
	if meta.Index <= r.RaftLog.committed {
		msg := pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			From:    r.id,
			To:      m.From,
			Term:    r.Term,
			Reject:  false,
			LogTerm: None,
			Index:   r.RaftLog.committed,
		}
		r.msgs = append(r.msgs, msg)
		return
	}
	r.becomeFollower(max(r.Term, m.Term), m.From)
	first := meta.Index + 1
	if len(r.RaftLog.entries) > 0 {
		r.RaftLog.entries = nil
	}
	r.RaftLog.FirstIndex = first
	r.RaftLog.applied = meta.Index
	r.RaftLog.committed = meta.Index
	r.RaftLog.stabled = meta.Index
	r.Prs = make(map[uint64]*Progress)
	for _, peer := range meta.ConfState.Nodes {
		r.Prs[peer] = &Progress{}
	}
	r.RaftLog.pendingSnapshot = m.Snapshot
	r.sendAppendResponse(m.From, false)
}

func (r *Raft) handleTransferLeader(m pb.Message) {
	if m.From == r.id {
		return
	}
	if r.leadTransferee != None && r.leadTransferee == m.From {
		return
	}
	if _, ok := r.Prs[m.From]; !ok {
		return
	}
	r.leadTransferee = m.From
	if r.Prs[m.From].Match == r.RaftLog.LastIndex() {
		r.sendTimeoutNow(m.From)
	} else {
		r.sendAppend(m.From)
	}

}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
	if _, ok := r.Prs[id]; !ok {
		r.Prs[id] = &Progress{Next: 1}
	}
	r.PendingConfIndex = None
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
	_, ok := r.Prs[id]
	if ok {
		delete(r.Prs, id)
		if r.State == StateLeader {
			r.leaderCommit()
		}
	}
	r.PendingConfIndex = None
}
