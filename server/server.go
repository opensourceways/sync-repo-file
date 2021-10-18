package server

import (
	"sync"

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

func DoOnce(clients map[string]SyncFileClient, cfg []SyncFileConfig) {
	w := worker{
		clients: clients,
		queue:   newTaskQueue(1000),
	}

	w.produce(cfg, 3)
	w.consume()
	w.wait()
}

type worker struct {
	clients map[string]SyncFileClient

	wg    sync.WaitGroup
	queue *taskQueue
}

func (w *worker) wait() {
	w.wg.Wait()
}

func (w *worker) produce(cfg []SyncFileConfig, maxRetry int) {
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()

		w.sendTask(cfg, 0, maxRetry)
		w.pushTask(taskInfo{exit: true})
	}()
}

func (w *worker) sendTask(cfg []SyncFileConfig, num, maxRetry int) {
	if num == maxRetry {
		return
	}

	failed := []SyncFileConfig{}

	task := taskInfo{}
	for i := range cfg {
		item := &cfg[i]

		cli := w.getClient(item.Platform)
		if cli == nil {
			continue
		}

		task.platform = item.Platform
		task.files = item.FileNames

		failedRepo := []string{}
		gtf := func() {
			if err := w.genTask(task, cli); err != nil {
				failedRepo = append(failedRepo, genRepoName(task.org, task.repo))
			}
		}

		for _, v := range item.Repos {
			if org, repo := parseRepo(v); repo != "" {
				task.org = org
				task.repo = repo
				gtf()
			} else {
				repos, err := cli.ListRepos(org)
				if err != nil {
					logrus.WithError(err).Errorf("list repos:%s/%s", task.platform, task.org)
					failedRepo = append(failedRepo, org)
					continue
				}

				task.org = org

				for _, repo := range repos {
					if !item.isExcluded(org, repo) {
						task.repo = repo
						gtf()
					}
				}
			}
		}

		if len(failedRepo) > 0 {
			v := *item
			v.Repos = failedRepo
			failed = append(failed, v)
		}
	}

	if len(failed) > 0 {
		w.sendTask(failed, num+1, maxRetry)
	}
}

func (w *worker) genTask(task taskInfo, cli SyncFileClient) error {
	branches, err := cli.ListBranchOfRepo(task.org, task.repo)
	if err != nil {
		logrus.WithError(err).Errorf("list branch of repo:%s/%s/%s", task.platform, task.org, task.repo)
		return err
	}

	for _, item := range branches {
		task.branch = item.Name
		task.branchSHA = item.SHA
		w.pushTask(task)
	}
	return nil
}

func (w *worker) getClient(platform string) SyncFileClient {
	if c, b := w.clients[platform]; b {
		return c
	}
	return nil
}

func (w *worker) pushTask(t taskInfo) {
	w.queue.push(&t)
}

func (w *worker) consume() {
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()

		t := new(taskInfo)
		for {
			w.popTask(t)
			if t.exit {
				break
			}

			w.runTask(t)
		}
	}()
}

func (w *worker) runTask(t *taskInfo) {
	cli := w.getClient(t.platform)
	if cli == nil {
		return
	}

	var err error
	for i := 0; i < 3; i++ {
		err = cli.SyncFileOfBranch(
			t.org, t.repo, t.branch, t.branchSHA, t.files,
		)
		if err == nil {
			return
		}
	}
	logrus.WithField("task", t.toString()).WithError(err).Error("sync file of branch")
}

func (w *worker) popTask(t *taskInfo) {
	w.queue.pop(t)
}
