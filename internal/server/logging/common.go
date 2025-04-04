package logging

import (
	"encoding/json"
	"strings"

	"github.com/sirupsen/logrus"

	"github.com/lxc/incus/v6/shared/api"
)

type common struct {
	config            map[string]string
	lifecycleProjects []string
	types             []string
}

func (c *common) processEvent(event api.Event) bool {
	if event.Type == api.EventTypeLifecycle {
		if !contains(c.types, "lifecycle") {
			return false
		}

		lifecycleEvent := api.EventLifecycle{}

		err := json.Unmarshal(event.Metadata, &lifecycleEvent)
		if err != nil {
			return false
		}

		if lifecycleEvent.Project != "" && len(c.lifecycleProjects) > 0 {
			if !contains(c.lifecycleProjects, lifecycleEvent.Project) {
				return false
			}
		}

		return true
	} else if event.Type == api.EventTypeLogging || event.Type == api.EventTypeNetworkACL {
		if !contains(c.types, "logging") && event.Type == api.EventTypeLogging {
			return false
		}

		if !contains(c.types, "network-acl") && event.Type == api.EventTypeNetworkACL {
			return false
		}

		logEvent := api.EventLogging{}

		err := json.Unmarshal(event.Metadata, &logEvent)
		if err != nil {
			return false
		}

		// The errors can be ignored as the values are validated elsewhere.
		l1, _ := logrus.ParseLevel(logEvent.Level)
		l2, _ := logrus.ParseLevel(c.config["logging.level"])

		// Only consider log messages with a certain log level.
		if l2 < l1 {
			return false
		}

		return true
	}

	return false
}

func sliceFromString(input string) []string {
	parts := strings.Split(input, ",")
	result := []string{}
	for _, v := range parts {
		if part := strings.TrimSpace(v); part != "" {
			result = append(result, part)
		}
	}
	return result
}

func contains(slice []string, target string) bool {
	for _, v := range slice {
		if v == target {
			return true
		}
	}
	return false
}
