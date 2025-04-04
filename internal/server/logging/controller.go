package logging

import (
	"fmt"

	"github.com/lxc/incus/v6/internal/server/events"
	"github.com/lxc/incus/v6/shared/api"
)

type Config map[string]map[string]string

type Logger interface {
	HandleEvent(event api.Event)
	Shutdown()
}

type LoggingController struct {
	config   Config
	listener *events.InternalListener
	loggers  map[string]Logger
}

func LoggerFromType(loggerName string, config map[string]string) (Logger, error) {
	key, ok := config["target.type"]
	if !ok {
		return nil, fmt.Errorf("No type definition for logger %s", loggerName)
	}

	switch key {
	case "syslog":
		client, err := NewSyslogClient(config)
		if err != nil {
			return nil, err
		}

		return client, nil
	default:
		return nil, fmt.Errorf("%s is not supported logger type", key)
	}
}

func NewLoggingController(config Config, listener *events.InternalListener) *LoggingController {
	return &LoggingController{
		config:   config,
		listener: listener,
		loggers:  map[string]Logger{},
	}
}

func (l *LoggingController) Setup() error {
	for conf := range l.config {
		logger, err := LoggerFromType(conf, l.config[conf])
		if err != nil {
			return err
		}

		l.listener.AddHandler(conf, logger.HandleEvent)
	}

	return nil
}

func (l *LoggingController) Shutdown() {
	for _, logger := range l.loggers {
		logger.Shutdown()
	}
}
