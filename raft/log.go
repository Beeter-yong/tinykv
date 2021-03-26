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

import pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// f
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	// 已经存储到稳定 storage 中日志的最大位置
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	// 已经在状态机中成功执行的日志最大位置
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// 用于记录还没有持久化存储的日志位置
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	// 存储还没有被压缩的日志
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	FirstIndex uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	lo, _ := storage.FirstIndex()
	hi, _ := storage.LastIndex()
	entries, err := storage.Entries(lo, hi+1)
	if err != nil {
		panic(err)
	}
	return &RaftLog{
		storage: storage,
		entries: entries,
		applied: lo - 1,

		stabled: hi,
		FirstIndex: lo,
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
// 返回还没有提交的日志
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		return l.entries[l.stabled - l.FirstIndex+1 :]
	}
	return nil
}

// nextEnts returns all the committed but not applied entries
// 返回所有提交但还没有应用到状态机中的日志
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		return l.entries[l.applied-l.FirstIndex+1 : l.committed-l.FirstIndex+1]
	}
	return nil
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	var index uint64
	if !IsEmptySnap(l.pendingSnapshot) {
		index = l.pendingSnapshot.Metadata.Index
	}
	if len(l.entries) > 0 {
		return max(l.entries[len(l.entries)-1].Index, index)
	}
	i, _ := l.storage.LastIndex()
	return max(i, index)
}

func (l *RaftLog) LastTerm() uint64 {
	lt, _ := l.Term(l.LastIndex())
	return lt
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if len(l.entries) > 0 && i >= l.FirstIndex {
		return l.entries[i - l.FirstIndex].Term, nil
	}

	term, err := l.storage.Term(i)
	if err == ErrUnavailable && !IsEmptySnap(l.pendingSnapshot) {
		if i == l.pendingSnapshot.Metadata.Index {
			// 刚好查看的是快照最后一个日志的 Term
			term = l.pendingSnapshot.Metadata.Term
			err = nil
		} else if i < l.pendingSnapshot.Metadata.Index {
			// 请求查看的索引已经压缩、
			err = ErrCompacted
		}
	}
	return term, err
}

func (l *RaftLog) toSliceIndex(i uint64) int {
	idx := int(i - l.FirstIndex)
	if idx < 0 {
		panic("toSliceIndex: index<0")
	}
	return idx
}

func (l *RaftLog) toEntryIndex(i int) uint64 {
	return uint64(i) + l.FirstIndex
}
