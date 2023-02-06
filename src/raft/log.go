package raft

type Log struct {
	Entries   []LogEntry
	NextIndex int
}

// 日志条目结构体
type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

func (l *Log) append(newlog ...LogEntry) {
	l.NextIndex += len(newlog)
	l.Entries = append(l.Entries, newlog...)
}

func (l *Log) delendslice(count int) {
	l.NextIndex -= len(l.Entries[count:])
	l.Entries = l.Entries[:count]
}

func (l *Log) delpreslice(count int) {
	var enties []LogEntry
	for i := count; i < len(l.Entries); i++ {
		enties = append(enties, l.Entries[i])
	}

	l.Entries = enties
}

func (l *Log) endslice(count int) []LogEntry {
	elogs := l.Entries[count:]
	return elogs
}

func (l *Log) atIndex(count int) LogEntry {
	alog := l.Entries[count]
	return alog
}

func (rf *Raft) dellogs(flag int, LogIndex int) {
	// flag 为1从头删除(删除index之前的, 包含index)
	if flag == 1 {
		count := LogIndex - rf.LastIncludedIndex
		rf.Logs.delpreslice(count)
	} else {
		// flag 为0从尾巴删除(删除index之后的，包含index)
		count := LogIndex - rf.LastIncludedIndex - 1
		rf.Logs.delendslice(count)
	}
}

func (rf *Raft) endlogs(nextindex int) []LogEntry {
	count := nextindex - rf.LastIncludedIndex - 1
	return rf.Logs.endslice(count)
}

func (rf *Raft) logat(n int) LogEntry {
	if n == rf.LastIncludedIndex {
		return LogEntry{Term: rf.LastIncludedTerm, Index: rf.LastIncludedIndex}
	}
	count := n - rf.LastIncludedIndex - 1
	return rf.Logs.atIndex(count)
}

func emptyLog() Log {
	elog := Log{
		Entries:   make([]LogEntry, 0),
		NextIndex: 1,
	}
	return elog
}
