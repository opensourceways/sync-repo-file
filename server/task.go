package server

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/util/sets"
)

type taskInfo struct {
	cfg       *OrgRepos
	ctx       context.Context
	cli       SyncFileClient
	platform  string
	org       string
	repo      string
	branch    string
	branchSHA string
	files     []string
	retryNum  int
}

func (t taskInfo) toString() string {
	org := t.org
	if t.cfg != nil {
		org = t.cfg.Org
	}

	return fmt.Sprintf(
		"org:%s, repo:%s, branch:%s, branch sha:%s, files:%s, retry number:%d",
		org, t.repo, t.branch, t.branchSHA,
		strings.Join(t.files, ", "),
		t.retryNum,
	)
}

type taskQueue struct {
	queue chan taskInfo
}

func (q *taskQueue) push(ctx context.Context, t *taskInfo) bool {
	select {
	case <-ctx.Done():
		return false

	case q.queue <- *t:
		return true
	}
}

func (q *taskQueue) pushWithTimeOut(ctx context.Context, t *taskInfo, timer *time.Timer) bool {
	select {
	case q.queue <- *t:
		if !timer.Stop() {
			<-timer.C
		}
		return true

	case <-timer.C:
		return false

	case <-ctx.Done():
		return false
	}
}

func (q *taskQueue) popWithTimeOut(ctx context.Context, t *taskInfo, timer *time.Timer) bool {
	select {
	case *t = <-q.queue:
		if !timer.Stop() {
			<-timer.C
		}
		return true

	case <-timer.C:
		return false

	case <-ctx.Done():
		return false
	}
}

func newTaskQueue(capacity int) *taskQueue {
	return &taskQueue{
		queue: make(chan taskInfo, capacity),
	}
}

type taskExecutor struct {
	timer       *time.Timer
	queue       *taskQueue
	maxRetry    int
	waitOnQueue time.Duration
	idleTimeOut time.Duration
}

func (w *taskExecutor) run(ctx context.Context) {
	t := new(taskInfo)
	w.timer = time.NewTimer(w.idleTimeOut)
	defer w.timer.Stop()

	for {
		if !w.queue.popWithTimeOut(ctx, t, w.timer) {
			logrus.Info("executor exits")
			break
		}

		w.execTask(t)

		w.timer.Reset(w.idleTimeOut)
	}
}

func (w *taskExecutor) execTask(t *taskInfo) {
	if t.cfg != nil {
		w.listRepos(t)
		return
	}

	if t.branch == "" {
		w.listBranch(t)
	} else {
		w.syncFile(t)
	}
}

func (w *taskExecutor) listRepos(t *taskInfo) {
	var repos []string
	var err error
	f := func(t *taskInfo) error {
		repos, err = w.listReposOfOrg(t.platform, t.cfg, t.cli)
		if err != nil {
			logrus.WithError(err).Errorf(
				"list repos of org:%s/%s", t.platform, t.cfg.Org,
			)
		}
		return err
	}

	if w.try(t, f) != nil {
		return
	}

	nt := &taskInfo{
		ctx:      t.ctx,
		cli:      t.cli,
		files:    t.files,
		platform: t.platform,
		org:      t.cfg.Org,
	}

	for _, repo := range repos {
		nt.repo = repo

		if !w.pushTask(nt) {
			if isCancelled(nt.ctx) {
				break
			}

			w.listBranch(nt)
		}
	}
}

func (w *taskExecutor) listBranch(t *taskInfo) {
	var branches []BranchInfo
	var err error
	f := func(t *taskInfo) error {
		org, repo := t.org, t.repo

		branches, err = t.cli.ListBranchOfRepo(org, repo)
		if err != nil {
			logrus.WithError(err).Errorf(
				"list branch of repo:%s/%s/%s", t.platform, org, repo,
			)
		}
		return err
	}

	if w.try(t, f) != nil {
		return
	}

	nt := &taskInfo{
		ctx:      t.ctx,
		cli:      t.cli,
		files:    t.files,
		platform: t.platform,
		org:      t.org,
		repo:     t.repo,
	}

	for _, b := range branches {
		nt.branch = b.Name
		nt.branchSHA = b.SHA

		if !w.pushTask(nt) {
			if isCancelled(nt.ctx) {
				break
			}

			w.syncFile(nt)
		}
	}
}

func (w *taskExecutor) syncFile(t *taskInfo) {
	f := func(t *taskInfo) error {
		err := t.cli.SyncFileOfBranch(t.org, t.repo, t.branch, t.branchSHA, t.files)
		if err != nil {
			logrus.WithError(err).Errorf("sync file of repo:%s", t.toString())
		}
		return err
	}

	w.try(t, f)
}

func (w *taskExecutor) try(t *taskInfo, tryOnce func(t *taskInfo) error) error {
	err := tryOnce(t)
	if err == nil {
		return nil
	}

	if t.retryNum++; t.retryNum >= w.maxRetry {
		return fmt.Errorf("excced max retry")
	}

	if w.pushTask(t) {
		return err
	}

	if isCancelled(t.ctx) {
		return fmt.Errorf("recieving signal to stop executing task")
	}

	return w.try(t, tryOnce)
}

func (w *taskExecutor) pushTask(t *taskInfo) bool {
	logrus.Infof("push task:%s", t.toString())

	w.timer.Reset(w.waitOnQueue)
	return w.queue.pushWithTimeOut(t.ctx, t, w.timer)
}

func (w *taskExecutor) listReposOfOrg(platform string, org *OrgRepos, cli SyncFileClient) ([]string, error) {
	repos, err := cli.ListRepos(org.Org)
	if err != nil {
		return nil, err
	}

	if len(org.Repos) > 0 {
		return sets.NewString(repos...).Intersection(
			sets.NewString(org.Repos...),
		).UnsortedList(), nil
	}

	if len(org.ExcludedRepos) > 0 {
		return sets.NewString(repos...).Difference(
			sets.NewString(org.ExcludedRepos...),
		).UnsortedList(), nil
	}
	return repos, nil
}
