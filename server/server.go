package server

import (
	"context"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

type BranchInfo struct {
	Name string
	SHA  string
}

type SyncFileClient interface {
	ListRepos(org string) ([]string, error)
	ListBranchOfRepo(org, repo string) ([]BranchInfo, error)
	SyncFileOfBranch(org, repo, branch, branchSHA string, files []string) error
}

func DoOnce(clients map[string]SyncFileClient, cfg []SyncFileConfig, concurrentSize int) (func(), func()) {
	w := worker{
		clients: clients,
		queue:   newTaskQueue(concurrentSize),
	}

	logrus.Info("start doing once")

	ctx, cancel := context.WithCancel(context.Background())

	w.produce(ctx, cfg)
	// there are 3 kinds of task.
	for i := 0; i < 3; i++ {
		w.consume(ctx)
	}

	return w.wait, cancel
}

type worker struct {
	clients map[string]SyncFileClient

	wg    sync.WaitGroup
	queue *taskQueue
}

func (w *worker) wait() {
	w.wg.Wait()
}

func (w *worker) produce(ctx context.Context, cfg []SyncFileConfig) {
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()

		for i := range cfg {
			if isCancelled(ctx) {
				break
			}

			item := &cfg[i]

			if cli, ok := w.clients[item.Platform]; !ok {
				logrus.Warningf("generate tasks, no client for platform:%s", item.Platform)
			} else {
				w.kickoff(ctx, item, cli)
			}
		}

		logrus.Info("generate tasks done")
	}()
}

func (w *worker) kickoff(ctx context.Context, cfg *SyncFileConfig, cli SyncFileClient) {
	task := &taskInfo{
		ctx:      ctx,
		cli:      cli,
		files:    cfg.FileNames,
		platform: cfg.Platform,
	}

	for i := range cfg.OrgRepos {
		if isCancelled(ctx) {
			break
		}

		task.cfg = &cfg.OrgRepos[i]
		w.queue.push(ctx, task)
		logrus.Infof("generate list repo task for org:%s", task.cfg.Org)
	}
}

func (w *worker) consume(ctx context.Context) {
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()

		e := &taskExecutor{
			queue:       w.queue,
			maxRetry:    3,
			waitOnQueue: 10 * time.Millisecond,
			idleTimeOut: 3 * time.Second,
		}
		e.run(ctx)
	}()
}

func isCancelled(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}
