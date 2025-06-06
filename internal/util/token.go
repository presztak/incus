package util

import (
	"encoding/base64"
	"encoding/json"
	"errors"

	"github.com/lxc/incus/v6/shared/api"
)

// JoinTokenDecode decodes a base64 and JSON encoded join token.
func JoinTokenDecode(input string) (*api.ClusterMemberJoinToken, error) {
	joinTokenJSON, err := base64.StdEncoding.DecodeString(input)
	if err != nil {
		return nil, err
	}

	var j api.ClusterMemberJoinToken
	err = json.Unmarshal(joinTokenJSON, &j)
	if err != nil {
		return nil, err
	}

	if j.ServerName == "" {
		return nil, errors.New("No server name in join token")
	}

	if len(j.Addresses) < 1 {
		return nil, errors.New("No cluster member addresses in join token")
	}

	if j.Secret == "" {
		return nil, errors.New("No secret in join token")
	}

	if j.Fingerprint == "" {
		return nil, errors.New("No certificate fingerprint in join token")
	}

	return &j, nil
}
