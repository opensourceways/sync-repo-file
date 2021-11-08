package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/opensourceways/community-robot-lib/logrusutil"
	"github.com/sirupsen/logrus"
	"sigs.k8s.io/yaml"

	"github.com/opensourceways/sync-repo-file/client"
	"github.com/opensourceways/sync-repo-file/server"
)

type options struct {
	configFile     string
	concurrentSize int
}

func (o options) validate() error {
	if o.configFile == "" {
		return fmt.Errorf("config-file must be set")
	}

	if o.concurrentSize <= 0 {
		return fmt.Errorf("concurrent size must be bigger than 0")
	}
	return nil
}

func gatherOptions(fs *flag.FlagSet, args ...string) options {
	var o options

	fs.StringVar(&o.configFile, "config-file", "", "Path to the config file.")
	fs.IntVar(&o.concurrentSize, "concurrent-size", 500, "The concurrent size for synchronizing files of repo branch.")

	fs.Parse(args)
	return o
}

func main() {
	logrusutil.ComponentInit("sync-repo-file-once")

	o := gatherOptions(flag.NewFlagSet(os.Args[0], flag.ExitOnError), os.Args[1:]...)
	if err := o.validate(); err != nil {
		logrus.WithError(err).Error("parse option")
		return
	}

	cfg, err := loadConfig(o.configFile)
	if err != nil {
		logrus.WithError(err).Errorf("load config:%s", o.configFile)
		return
	}

	clients := initClients(cfg.Clients)
	if len(clients) == 0 {
		return
	}
	defer func() {
		for _, cli := range clients {
			cli.Stop()
		}
	}()

	clis := map[string]server.SyncFileClient{}
	for k, v := range clients {
		clis[k] = v
	}
	wait, cancel := server.DoOnce(clis, cfg.SyncFiles, o.concurrentSize)

	run(wait, cancel)
}

func run(wait, cancel func()) {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)

	var wg sync.WaitGroup
	defer wg.Wait()

	ctx, done := context.WithCancel(context.Background())
	defer done()

	wg.Add(1)
	go func(ctx context.Context) {
		defer wg.Done()

		select {
		case <-ctx.Done():
			logrus.Info("receive done. exit normally")
			return
		case <-sig:
			logrus.Info("receive exit signal")
			cancel()
			return
		}
	}(ctx)

	wait()
}

func initClients(cfg []clientConfig) map[string]*client.SyncFileClient {
	clients := map[string]*client.SyncFileClient{}

	for _, item := range cfg {
		if _, ok := clients[item.Platform]; ok {
			continue
		}

		if cli, err := client.NewSyncFileClient(item.Endpoint); err == nil {
			clients[item.Platform] = cli
		} else {
			logrus.WithField("enpoint", item.Endpoint).WithError(err).Infof(
				"init sync file client for platform:%s", item.Platform,
			)
		}
	}
	return clients
}

func loadConfig(path string) (*configuration, error) {
	b, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	v := new(configuration)
	if err := yaml.Unmarshal(b, v); err != nil {
		return nil, err
	}

	v.SetDefault()

	return v, v.Validate()
}
