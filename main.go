package main

import (
	"flag"
	"fmt"
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
	startTime  string
	interval   int
}

func (o options) validate() error {
	if _, _, err := o.parseStartTime(); err != nil {
		return err
	}
	return nil
}

func (o options) parseStartTime() (int, int, error) {
	format := "2006-01-02T"
	t, err := time.Parse(format+"15:04", format+o.startTime)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid start_time: %s", err.Error())
	}

	return t.Hour(), t.Minute(), nil
}

func (o options) getStartTime() time.Duration {
	seconds := func(h, m int) int {
		return h*3600 + m*60
	}

	now := time.Now()
	t0 := seconds(now.Hour(), now.Minute())

	h, m, _ := o.parseStartTime()
	t1 := seconds(h, m)

	diff := t1 - t0
	if diff >= 0 {
		return time.Duration(diff) * time.Second
	}
	return time.Duration(seconds(24, 0)+diff) * time.Second
}

func gatherOptions(fs *flag.FlagSet, args ...string) options {
	var o options

	fs.StringVar(&o.configFile, "config-file", "", "Path to the config file.")
	fs.StringVar(&o.startTime, "start-time", "01:00", "Time to synchronize repo file for the first time. The format is Hour:Minute")
	fs.IntVar(&o.interval, "interval", 24, "Interval between two synchronizations. The unit is hour")

	fs.Parse(args)
	return o
}

func main() {
	logrusutil.ComponentInit("sync-repo-file-trigger")

	o := gatherOptions(flag.NewFlagSet(os.Args[0], flag.ExitOnError), os.Args[1:]...)
	if err := o.validate(); err != nil {
		logrus.WithError(err).Error("parse option")
		return
	}

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

	time.Sleep(o.getStartTime())
	go task()

	t := utils.NewTimer()

	t.Start(func() { go task() }, time.Duration(o.interval)*time.Hour)

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
