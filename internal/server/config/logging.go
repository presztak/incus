package config

import (
	"strings"

	"github.com/sirupsen/logrus"

	"github.com/lxc/incus/v6/shared/validate"
)

// IsLoggingConfig returns true if the config key is a logging configuration.
func IsLoggingConfig(key string) bool {
	return strings.HasPrefix(key, "logging.")
}

func GetLoggingRuleForKey(key string) Key {
	fields := strings.Split(key, ".")
	loggingKey := strings.Join(fields[2:], ".")

	var rule Key
	switch loggingKey {
		case "target.address":
			rule = Key{}
		case "target.facility":
			rule = Key{Default: "daemon"}
		case "target.type":
			rule = Key{Validator: validate.Optional(validate.IsListOf(validate.IsOneOf("syslog")))}
		case "types":
			rule = Key{Validator: validate.Optional(validate.IsListOf(validate.IsOneOf("lifecycle", "logging", "network-acl"))), Default: "lifecycle,logging"}
		case "logging.level":
			rule = Key{Validator: LogLevelValidator, Default: logrus.InfoLevel.String()}
		case "lifecycle.types":
			rule = Key{Validator: validate.Optional(validate.IsAny)}
		case "lifecycle.projects":
			rule = Key{Validator: validate.Optional(validate.IsAny)}
		}

	return rule
}
