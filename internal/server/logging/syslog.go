package logging

import (
	"fmt"
	"log/syslog"
	"strings"

	"github.com/lxc/incus/v6/shared/api"
)

var facilityMap = map[string]syslog.Priority{
	"kern":     syslog.LOG_KERN,
	"user":     syslog.LOG_USER,
	"mail":     syslog.LOG_MAIL,
	"daemon":   syslog.LOG_DAEMON,
	"auth":     syslog.LOG_AUTH,
	"syslog":   syslog.LOG_SYSLOG,
	"lpr":      syslog.LOG_LPR,
	"news":     syslog.LOG_NEWS,
	"uucp":     syslog.LOG_UUCP,
	"cron":     syslog.LOG_CRON,
	"authpriv": syslog.LOG_AUTHPRIV,
	"ftp":      syslog.LOG_FTP,
}

type SyslogClient struct {
	common
	writer *syslog.Writer
}

func NewSyslogClient(config map[string]string) (*SyslogClient, error) {
	network, address := parseAddress(config["target.address"])

	writer, err := syslog.Dial(network, address, parseFacility(config["target.facility"]), "incus")
	if err != nil {
		return nil, err
	}

	return &SyslogClient{
		common: common{
			config:            config,
			types:             sliceFromString(config["types"]),
			lifecycleProjects: sliceFromString(config["lifecycle.projects"]),
		},
		writer: writer,
	}, nil
}

// HandleEvent handles the event received from the internal event listener.
func (c *SyslogClient) HandleEvent(event api.Event) {
	if !c.processEvent(event) {
		return
	}

	c.writer.Info(fmt.Sprintf("type: %s log: %s", event.Type, string(event.Metadata)))
}

// Close cleanups everything.
func (c *SyslogClient) Shutdown() {
	if c.writer != nil {
		c.writer.Close()
	}
}

func parseAddress(address string) (string, string) {
	if strings.Contains(address, "://") {
		parts := strings.SplitN(address, "://", 2)
		return parts[0], parts[1]
	} else {
		// Default protocol
		return "udp", address
	}
}

func parseFacility(facility string) syslog.Priority {
	facility = strings.ToLower(strings.TrimSpace(facility))

	if val, ok := facilityMap[facility]; ok {
		return val
	}
	return syslog.LOG_DAEMON
}
