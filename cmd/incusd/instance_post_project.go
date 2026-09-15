package main

import (
	"fmt"

	"github.com/lxc/incus/v7/internal/server/db"
	"github.com/lxc/incus/v7/internal/server/instance"
	"github.com/lxc/incus/v7/internal/server/project"
	"github.com/lxc/incus/v7/internal/server/state"
)

// checkProjectMove validates that an instance can be moved to the given project.
func checkProjectMove(s *state.State, inst instance.Instance, targetProject string) error {
	srcVolProject, err := project.StorageVolumeProject(s.DB.Cluster, inst.Project().Name, db.StoragePoolVolumeTypeCustom)
	if err != nil {
		return err
	}

	dstVolProject, err := project.StorageVolumeProject(s.DB.Cluster, targetProject, db.StoragePoolVolumeTypeCustom)
	if err != nil {
		return err
	}

	if srcVolProject == dstVolProject {
		return nil
	}

	// Attached custom volumes are left behind when the volume project changes.
	for _, dev := range inst.ExpandedDevices().Sorted() {
		if dev.Config["type"] != "disk" || dev.Config["path"] == "/" || dev.Config["pool"] == "" || dev.Config["source"] == "" {
			continue
		}

		return fmt.Errorf("Device %q uses a custom volume which isn't available in project %q", dev.Name, targetProject)
	}

	return nil
}
