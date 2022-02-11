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
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"math/rand"
	"sort"

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
	votes      map[uint64]bool
	heartbeats map[uint64]bool
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
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	hardState, _, _ := c.Storage.InitialState()
	var raft = &Raft{

		id:               c.ID,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		Term:             hardState.Term,
		Vote:             hardState.Vote,
		RaftLog:          newLog(c.Storage),
		Prs:              make(map[uint64]*Progress),
		votes:            make(map[uint64]bool),
		msgs:             nil,
		//Exist irrationality here
		//Msgs should be initialized with make([]pb.Messages,0)
		//But can not pass Project2ac
		heartbeats: make(map[uint64]bool),
		Lead:       None,
		State:      StateFollower,
	}

	lastIndex := raft.RaftLog.LastIndex()
	if c.Applied > 0 {
		raft.RaftLog.applied = c.Applied
	}
	for _, fuckCCF := range c.peers {
		raft.Prs[fuckCCF] = &Progress{
			Next:  lastIndex + 1,
			Match: 0,
		}
	}
	return raft
}

func (r *Raft) sendSnapshot(to uint64) {
	snapshot, _error := r.RaftLog.storage.Snapshot()
	if _error != nil {
		return
	}
	message := pb.Message{
		MsgType:  pb.MessageType_MsgSnapshot,
		From:     r.id,
		To:       to,
		Term:     r.Term,
		Snapshot: &snapshot,
	}
	r.msgs = append(r.msgs, message)
	r.Prs[to].Next = snapshot.Metadata.Index + 1
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).

	//preLogIndex	紧邻新日志条目之前的那个日志条目的索引
	//preLogTerm	紧邻新日志条目之前的那个日志条目的任期
	//entries[]	需要被保存的日志条目（被当做心跳使用时，则日志条目内容为空；为了提高效率可能一次性发送多个）
	//Commit	领导者的已知已提交的最高的日志条目的索引

	if _, ok := r.Prs[to]; !ok {
		return false
	}
	preLogIndex := r.Prs[to].Next - 1
	firstIndex := r.RaftLog.FirstIndex()
	preLogTerm, _ := r.RaftLog.Term(preLogIndex)

	var _entries []*pb.Entry
	n := r.RaftLog.LastIndex() + 1

	for i := preLogIndex + 1; i < n; i++ {
		_entries = append(_entries, &r.RaftLog.entries[i-firstIndex])
	}

	_Append := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Entries: _entries,
		Commit:  r.RaftLog.committed,
		LogTerm: preLogTerm,
		Index:   preLogIndex,
	}
	r.msgs = append(r.msgs, _Append)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	if _, ok := r.Prs[to]; !ok {
		return
	}
	if r.leadTransferee != None {
		r.Step(pb.Message{MsgType: pb.MessageType_MsgTransferLeader, From: r.leadTransferee})
	}
	_Heartbeat := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
	}
	r.msgs = append(r.msgs, _Heartbeat)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		r.electionElapsed++
		if r.electionElapsed >= r.electionTimeout {
			r.electionElapsed = 0 - rand.Intn(r.electionTimeout)

			_error := r.Step(pb.Message{
				MsgType: pb.MessageType_MsgHup,
			})
			if _error != nil {
				println(_error)
				return
			}
		}
	case StateCandidate:
		r.electionElapsed++
		if r.electionElapsed >= r.electionTimeout {
			r.electionElapsed = 0 - rand.Intn(r.electionTimeout)

			_error := r.Step(pb.Message{
				MsgType: pb.MessageType_MsgHup,
			})
			if _error != nil {
				println(_error)
				return
			}
		}
	case StateLeader:
		r.electionElapsed++
		number := len(r.heartbeats)
		length := len(r.Prs) / 2
		if r.electionElapsed >= r.electionTimeout {
			r.electionElapsed = 0 - rand.Intn(r.electionTimeout)
			r.heartbeats = make(map[uint64]bool)
			r.heartbeats[r.id] = true
			if number <= length {
				if _, ok := r.Prs[r.id]; !ok {
					return
				}
				r.becomeCandidate()
				if len(r.Prs) == 1 {
					r.becomeLeader()
				}
				for peer := range r.Prs {
					if peer != r.id {
						r.sendRequestVote(peer)
					}
				}
			}
		}
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			_error := r.Step(pb.Message{
				MsgType: pb.MessageType_MsgBeat,
			})
			if _error != nil {
				println(_error)

				return
			}
		}
	}
}

func (r *Raft) sendRequestVote(To uint64) bool {
	if _, ok := r.Prs[To]; !ok {
		return false
	}
	_Request := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		To:      To,
		From:    r.id,
		Term:    r.Term,
		LogTerm: r.RaftLog.LastTerm(),
		Index:   r.RaftLog.LastIndex(),
		Commit:  r.RaftLog.committed,
		Entries: nil,
	}
	r.msgs = append(r.msgs, _Request)

	return true
}

//按照论文所述，主要注意lastIndex，lastLogTerm的获得，这里需要先修改raftLog。然后对每个peer（不包含自己）发送sendRequestVote。

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.Term = term
	r.Vote = None
	r.Lead = lead
	r.State = StateFollower
	r.heartbeatElapsed = 0
	r.electionElapsed = 0 - rand.Intn(r.electionTimeout)

}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	if _, ok := r.Prs[r.id]; !ok {
		return
	}
	r.Lead = None
	r.Term++
	r.Vote = r.id
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
	r.State = StateCandidate
	r.heartbeatElapsed = 0
	r.electionElapsed = 0 - rand.Intn(r.electionTimeout)

}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	if _, ok := r.Prs[r.id]; !ok {
		return
	}
	r.State = StateLeader
	r.Lead = r.id
	r.leadTransferee = None
	r.heartbeatElapsed = 0
	r.electionElapsed = 0 - rand.Intn(r.electionTimeout)

	r.heartbeats = make(map[uint64]bool)
	r.heartbeats[r.id] = true

	lastLogIndex := r.RaftLog.LastIndex()
	for to := range r.Prs {
		r.Prs[to].Next = lastLogIndex + 1
		r.Prs[to].Match = 0
	}

	r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{Term: r.Term, Index: r.RaftLog.LastIndex() + 1})
	r.Prs[r.id].Match = r.Prs[r.id].Next
	r.Prs[r.id].Next += 1

	for to, _ := range r.Prs {
		if to != r.id {
			r.sendAppend(to)
		}
	}
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.Prs[r.id].Match
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	//

	switch r.State {
	case StateFollower:

		switch m.MsgType {
		case pb.MessageType_MsgHup:
			if _, ok := r.Prs[r.id]; !ok {
				return nil
			}
			r.becomeCandidate()
			if len(r.Prs) == 1 {
				r.becomeLeader()
			}
			for peer := range r.Prs {
				if peer != r.id {
					r.sendRequestVote(peer)
				}
			}
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgTimeoutNow:
			if _, ok := r.Prs[r.id]; !ok {
				return nil
			}
			r.becomeCandidate()
			if len(r.Prs) == 1 {
				r.becomeLeader()
			}
			for peer := range r.Prs {
				if peer != r.id {
					r.sendRequestVote(peer)
				}
			}
		}
	case StateCandidate:
		if _, ok := r.Prs[r.id]; !ok {
			return nil
		}
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			if _, ok := r.Prs[r.id]; !ok {
				return nil
			}
			r.becomeCandidate()
			if len(r.Prs) == 1 {
				r.becomeLeader()
			}
			for peer := range r.Prs {
				if peer != r.id {
					r.sendRequestVote(peer)
				}
			}
		case pb.MessageType_MsgAppend:
			if m.Term >= r.Term {
				r.becomeFollower(m.Term, m.From)
			}
			r.handleAppendEntries(m)
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgRequestVoteResponse:
			r.votes[m.From] = !m.Reject
			length := len(r.Prs) / 2
			yes := 0
			no := 0
			for _, ok := range r.votes {
				if ok {
					yes++
				} else {
					no++
				}
			}
			if yes > length {
				r.becomeLeader()
			} else if no > length {
				r.becomeFollower(r.Term, m.From)
			}
		case pb.MessageType_MsgHeartbeat:
			if r.Term < m.Term {
				r.becomeFollower(m.Term, m.From)
			}
			r.handleHeartbeat(m)
		case pb.MessageType_MsgTransferLeader:
			if r.Lead != None {
				m.To = r.Lead
				r.msgs = append(r.msgs, m)
			}
		case pb.MessageType_MsgTimeoutNow:
			if _, ok := r.Prs[r.id]; !ok {
				return nil
			}
			r.becomeCandidate()
			if len(r.Prs) == 1 {
				r.becomeLeader()
			}
			for peer := range r.Prs {
				if peer != r.id {
					r.sendRequestVote(peer)
				}
			}
		}
	case StateLeader:
		if _, ok := r.Prs[r.id]; !ok {
			return nil
		}
		switch m.MsgType {
		case pb.MessageType_MsgBeat:
			for to := range r.Prs {
				if to != r.id {
					r.sendHeartbeat(to)
				}
			}
		case pb.MessageType_MsgPropose:
			if r.leadTransferee == None {
				lastLogindex := r.RaftLog.LastIndex()
				for i, entry := range m.Entries {
					entry.Term = r.Term
					entry.Index = lastLogindex + uint64((i)) + 1
					r.RaftLog.entries = append(r.RaftLog.entries, *entry)
				}

				r.Prs[r.id].Match = r.RaftLog.LastIndex()
				r.Prs[r.id].Next = r.Prs[r.id].Match + 1

				for to := range r.Prs {
					if to != r.id {
						r.sendAppend(to)
					}
				}

				if len(r.Prs) == 1 {
					r.RaftLog.committed = r.Prs[r.id].Match
				}
			}
		case pb.MessageType_MsgAppend:
			if m.Term >= r.Term {
				r.becomeFollower(m.Term, m.From)
			}
			r.handleAppendEntries(m)
		case pb.MessageType_MsgAppendResponse:

			if m.Reject && m.Term <= r.Term {
				next := r.Prs[m.From].Next - 1
				r.Prs[m.From].Next = min(m.Index, next)
				r.sendAppend(m.From)
				return nil
			}
			r.Prs[m.From].Match = m.Index
			r.Prs[m.From].Next = m.Index + 1

			if m.From == r.leadTransferee {
				r.Step(pb.Message{
					MsgType: pb.MessageType_MsgTransferLeader,
					From:    m.From,
				})
			}
			i := 0
			match := make(uint64Slice, len(r.Prs))
			for _, prs := range r.Prs {
				match[i] = prs.Match
				i++
			}
			sort.Sort(match)

			ma := match[(len(r.Prs)-1)/2]
			mat, _ := r.RaftLog.Term(ma)

			if ma > r.RaftLog.committed && mat == r.Term {
				r.RaftLog.committed = ma
				for to := range r.Prs {
					if to != r.id {
						r.sendAppend(to)
					}
				}
			}
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgHeartbeat:
			if m.Term > r.Term {
				r.becomeFollower(m.Term, m.From)
			}
			r.handleHeartbeat(m)
		case pb.MessageType_MsgHeartbeatResponse:
			r.heartbeats[m.From] = true
			if m.Reject || m.Commit < r.RaftLog.committed {
				r.sendAppend(m.From)
			}
		case pb.MessageType_MsgTransferLeader:
			if _, ok := r.Prs[m.From]; !ok {
				return nil
			}
			r.leadTransferee = m.From
			if r.Prs[m.From].Match == r.RaftLog.LastIndex() {
				message := pb.Message{
					MsgType: pb.MessageType_MsgTimeoutNow,
					From:    r.id,
					To:      m.From,
				}
				r.msgs = append(r.msgs, message)
			} else {
				r.sendAppend(m.From)
			}
		case pb.MessageType_MsgTimeoutNow:
			if _, ok := r.Prs[r.id]; !ok {
				return nil
			}
			r.becomeCandidate()
			if len(r.Prs) == 1 {
				r.becomeLeader()
			}
			for peer := range r.Prs {
				if peer != r.id {
					r.sendRequestVote(peer)
				}
			}
		}

	}
	//if r.Term < m.Term {
	//	r.becomeFollower(m.Term, m.From)
	//}
	////当其他领导人或候选人接收到当前信息后，如果任期小于等于本身任期则直接拒绝；大于当前任期，则直接变回追随者。
	//
	//if m.MsgType == pb.MessageType_MsgRequestVote {
	//	if m.Term < r.Term {
	//		_response := pb.Message{
	//			MsgType: pb.MessageType_MsgRequestVoteResponse,
	//			Reject:  true,
	//			From:    r.id,
	//			To:      m.From,
	//			Term:    r.Term,
	//		}
	//		r.msgs = append(r.msgs, _response)
	//		return nil
	//	}
	//	if r.State != StateFollower && m.Term > r.Term {
	//		r.becomeFollower(m.Term, None)
	//	}
	//	if m.Term > r.Term {
	//		r.Vote = None
	//		r.Term = m.Term
	//	}
	//
	//	if r.Vote == None || r.Vote == m.From {
	//		lastIndex := r.RaftLog.LastIndex()
	//		lastTerm, _ := r.RaftLog.Term(lastIndex)
	//		if m.LogTerm > lastTerm ||
	//			(m.LogTerm == lastTerm && m.Index >= lastIndex) {
	//			r.Vote = m.From
	//			if r.Term < m.Term {
	//				r.Term = m.Term
	//			}
	//			_response := pb.Message{
	//				MsgType: pb.MessageType_MsgRequestVoteResponse,
	//				From:    r.id,
	//				To:      m.From,
	//				Term:    r.Term,
	//				Reject:  false,
	//			}
	//			r.msgs = append(r.msgs, _response)
	//			return nil
	//		}
	//	}
	//
	//	_response := pb.Message{
	//		MsgType: pb.MessageType_MsgRequestVoteResponse,
	//		From:    r.id,
	//		To:      m.From,
	//		Term:    r.Term,
	//		Reject:  true,
	//	}
	//	r.msgs = append(r.msgs, _response)
	//	return nil
	//
	//}
	//
	//switch r.State {
	//case StateFollower:
	//	switch m.MsgType {
	//	case pb.MessageType_MsgAppend:
	//		r.handleAppendEntries(m)
	//	case pb.MessageType_MsgHeartbeat:
	//		r.handleHeartbeat(m)
	//	case pb.MessageType_MsgHup:
	//		r.becomeCandidate()
	//		for to, _ := range r.Prs {
	//			if to != r.id {
	//				r.sendRequestVote(to)
	//			}
	//		}
	//
	//	}
	//case StateCandidate:
	//	switch m.MsgType {
	//	case pb.MessageType_MsgAppend:
	//		r.becomeFollower(m.Term, m.From)
	//	case pb.MessageType_MsgHeartbeat:
	//		r.becomeFollower(m.Term, m.From)
	//	case pb.MessageType_MsgHup:
	//		r.becomeCandidate()
	//		for to, _ := range r.Prs {
	//			if to != r.id {
	//				r.sendRequestVote(to)
	//			}
	//		}
	//	case pb.MessageType_MsgRequestVoteResponse:
	//		if m.Reject == true {
	//			r.votes[m.From] = false
	//		} else {
	//			r.votes[m.From] = true
	//		}
	//
	//		length := len(r.Prs)
	//
	//		if length == 1 {
	//			r.becomeLeader()
	//		}
	//
	//		//候选人就收到大部分选票则直接成为领导人，如果收到大多数的拒绝则变回追随者。
	//		yes, no := 0, 0
	//		for _, _vote := range r.votes {
	//			if _vote == true {
	//				yes++
	//			} else {
	//				no++
	//			}
	//			if yes > length/2 {
	//				r.becomeLeader()
	//			}
	//			if no > length/2 {
	//				r.becomeFollower(r.Term, None)
	//			}
	//		}
	//	}
	//case StateLeader:
	//	switch m.MsgType {
	//	case pb.MessageType_MsgBeat:
	//		for to, _ := range r.Prs {
	//			if to != r.id {
	//				r.sendHeartbeat(to)
	//			}
	//		}
	//	case pb.MessageType_MsgHeartbeatResponse:
	//		if m.Reject || m.Commit < r.RaftLog.committed {
	//			r.sendAppend(m.From)
	//		}
	//	case pb.MessageType_MsgPropose:
	//		//MessageType_MsgPropose是追随者发送给领导者的信息，当客户端将请求发送到追随者后，
	//		//追随者将请求使用信息MessageType_MsgPropose重定向到领导者的结点。
	//		lastLogIndex := r.RaftLog.LastIndex()
	//
	//		//handle entries
	//		for i, _entry := range m.Entries {
	//			_entry.Term = r.Term
	//			_entry.Index = lastLogIndex + 1 + uint64(i)
	//			//Index下标
	//			r.RaftLog.entries = append(r.RaftLog.entries, *_entry)
	//		}
	//
	//		r.Prs[r.id].Match = r.RaftLog.LastIndex()
	//		r.Prs[r.id].Next = r.Prs[r.id].Match + 1
	//
	//		if len(r.Prs) == 1 {
	//			r.RaftLog.committed = r.Prs[r.id].Match
	//		}
	//
	//		for to, _ := range r.Prs {
	//			if to != r.id {
	//				r.sendAppend(to)
	//			}
	//		}
	//	}
	//}
	return nil
}

func (r *Raft) AppendResponse(to uint64, reject bool, index uint64) {
	_response := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      to,
		Reject:  reject,
		Index:   index,
		Term:    r.Term,
	}
	r.msgs = append(r.msgs, _response)
	return
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	//如果任期更大，就reject

	// When we create a region peer, we should initialize its log term/index > 0,
	// so that we can force the follower peer to sync the snapshot first.
	if m.Term < r.Term {
		r.AppendResponse(m.From, true, 0)
		return
	}

	if len(r.Prs) == 0 && r.RaftLog.LastIndex() < meta.RaftInitLogIndex {
		r.AppendResponse(m.From, true, r.RaftLog.LastIndex())
		return
	}

	if m.Term > r.Term {
		r.Term = m.Term
	}
	if m.From != r.Lead {
		r.Lead = m.From
	}

	lastLogIndex := r.RaftLog.LastIndex()
	if m.Index <= lastLogIndex {

		LogTerm, _error := r.RaftLog.Term(m.Index)

		if _error != nil && _error == ErrCompacted {
			r.Vote = None
			r.AppendResponse(m.From, false, r.RaftLog.LastIndex())
			return
		}

		//term不一样但是index一样的日志，就必须把旧的删掉
		if m.LogTerm == LogTerm {
			for i, entry := range m.Entries {
				var Term uint64
				entry.Index = m.Index + uint64(i) + 1
				if entry.Index <= lastLogIndex {
					Term, _ = r.RaftLog.Term(entry.Index)
					if Term != entry.Term {
						firstIndex := r.RaftLog.FirstIndex()
						if len(r.RaftLog.entries) != 0 && int64(m.Index-firstIndex+1) >= 0 {
							r.RaftLog.entries = r.RaftLog.entries[0 : m.Index-firstIndex+1]
						}
						lastLogIndex = r.RaftLog.LastIndex()
						r.RaftLog.entries = append(r.RaftLog.entries, *entry)
						r.RaftLog.stabled = m.Index
					}
				} else {
					//直接合并
					r.RaftLog.entries = append(r.RaftLog.entries, *entry)
				}
			}
			r.Vote = None
			r.AppendResponse(m.From, false, r.RaftLog.LastIndex())
			// set committed
			if m.Commit > r.RaftLog.committed {
				committed := min(m.Commit, m.Index+uint64(len(m.Entries)))
				r.RaftLog.committed = min(committed, r.RaftLog.LastIndex())
			}
			return
		}

	}
	index := max(lastLogIndex+1, meta.RaftInitLogIndex+1)
	r.AppendResponse(m.From, true, index)
	return

}

func (r *Raft) handleRequestVote(m pb.Message) {
	if m.Term < r.Term {
		message := pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			From:    r.id,
			To:      m.From,
			Term:    r.Term,
			Reject:  true,
		}
		r.msgs = append(r.msgs, message)
		return
	}
	if r.State != StateFollower && m.Term > r.Term {
		r.becomeFollower(m.Term, None)
	}
	if r.Term < m.Term {
		r.Vote = None
		r.Term = m.Term
	}

	if r.Vote == None || r.Vote == m.From {
		lastLogIndex := r.RaftLog.LastIndex()
		lastLogTerm, _ := r.RaftLog.Term(lastLogIndex)
		if m.LogTerm > lastLogTerm || (m.LogTerm == lastLogTerm && m.Index >= lastLogIndex) {
			r.Vote = m.From
			if r.Term < m.Term {
				r.Term = m.Term
			}
			message := pb.Message{
				MsgType: pb.MessageType_MsgRequestVoteResponse,
				From:    r.id,
				To:      m.From,
				Term:    r.Term,
				Reject:  false,
			}
			r.msgs = append(r.msgs, message)
			return
		}
	}
	message := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Reject:  true,
	}
	r.msgs = append(r.msgs, message)
	return
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	if m.Term != None && m.Term < r.Term {
		message := pb.Message{
			MsgType: pb.MessageType_MsgHeartbeatResponse,
			From:    r.id,
			To:      m.From,
			Term:    r.Term,
			Reject:  true,
			Commit:  r.RaftLog.committed,
			Index:   r.RaftLog.stabled,
		}
		r.msgs = append(r.msgs, message)
		return
	}
	if m.Term > r.Term {
		r.Term = m.Term
	}
	if m.From != r.Lead {
		r.Lead = m.From
	}
	r.electionElapsed -= r.heartbeatTimeout
	message := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Reject:  false,
		Commit:  r.RaftLog.committed,
		Index:   r.RaftLog.stabled,
	}
	r.msgs = append(r.msgs, message)
	return
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
