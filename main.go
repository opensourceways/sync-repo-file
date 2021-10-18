package main

import (
	"flag"
	"os"
	"time"

	"github.com/opensourceways/robot-gitee-plugin-lib/config"
	"github.com/opensourceways/robot-gitee-plugin-lib/interrupts"
	"github.com/opensourceways/robot-gitee-plugin-lib/logrusutil"
	"github.com/opensourceways/robot-gitee-plugin-lib/utils"
	"github.com/sirupsen/logrus"

	"github.com/opensourceways/sync-repo-file/client"
	"github.com/opensourceways/sync-repo-file/server"
)

type options struct {
	configFile string
}

func gatherOptions(fs *flag.FlagSet, args ...string) options {
	var o options

	fs.StringVar(&o.configFile, "config-file", "", "Path to the config file.")

	fs.Parse(args)
	return o
}

func main() {
	logrusutil.ComponentInit("sync-repo-file-trigger")

	o := gatherOptions(flag.NewFlagSet(os.Args[0], flag.ExitOnError), os.Args[1:]...)

	agent := config.NewConfigAgent(func() config.PluginConfig {
		return new(configuration)
	})
	if err := agent.Start(o.configFile); err != nil {
		logrus.WithError(err).Error("start config file agent")
		return
	}

	clients := map[string]*client.SyncFileClient{}

	task := func() {
		_, v := agent.GetConfig()
		cfg := v.(*configuration)

		for _, item := range cfg.Clients {
			if _, ok := clients[item.Platform]; !ok {
				cli, err := client.NewSyncFileClient(item.Endpoint)
				if err != nil {
					logrus.WithField("enpoint", item.Endpoint).WithError(err).Infof("init sync file client")
				} else {
					clients[item.Platform] = cli
				}
			}
		}

		clis := map[string]server.SyncFileClient{}
		for k, v := range clients {
			clis[k] = v
		}
		server.DoOnce(clis, cfg.SyncFiles)
	}

	t := utils.NewTimer()

	t.Start(func() { go task() }, 24*time.Hour)

	defer interrupts.WaitForGracefulShutdown()

	ch := make(chan struct{})

	interrupts.OnInterrupt(func() {
		t.Stop()
		for _, item := range clients {
			item.Stop()
		}
		close(ch)
	})

	<-ch
}

func newConfig() config.PluginConfig {
	return new(configuration)
}
