package server

import (
	"fmt"
	"strings"
)

type taskInfo struct {
	platform  string
	org       string
	repo      string
	branch    string
	branchSHA string
	files     []string
	exit      bool
}

func (t taskInfo) toString() string {
	return fmt.Sprintf(
		"org:%s, repo:%s, branch:%s, branch name:%s, branch sha:%s, files:%s",
		t.org, t.repo, t.branch, t.branchSHA, strings.Join(t.files, ", "),
	)
}

type taskQueue struct {
	queue chan taskInfo
}

func (q *taskQueue) push(t *taskInfo) {
	q.queue <- *t
}

func (q *taskQueue) pop(t *taskInfo) {
	*t = <-q.queue
}

func newTaskQueue(capacity int) *taskQueue {
	return &taskQueue{
		queue: make(chan taskInfo, capacity),
	}
}
