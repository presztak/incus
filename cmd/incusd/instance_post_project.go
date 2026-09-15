package main

import (
	"context"
	"fmt"
	"maps"

	"github.com/lxc/incus/v7/internal/server/db"
	dbCluster "github.com/lxc/incus/v7/internal/server/db/cluster"
	deviceConfig "github.com/lxc/incus/v7/internal/server/device/config"
	"github.com/lxc/incus/v7/internal/server/instance"
	"github.com/lxc/incus/v7/internal/server/project"
	"github.com/lxc/incus/v7/internal/server/state"
	"github.com/lxc/incus/v7/shared/api"
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

// targetProjectProfiles returns the named profiles as resolved in the target project.
func targetProjectProfiles(ctx context.Context, s *state.State, targetProject string, profileNames []string) ([]api.Profile, error) {
	profiles := make([]api.Profile, 0, len(profileNames))

	err := s.DB.Cluster.Transaction(ctx, func(ctx context.Context, tx *db.ClusterTx) error {
		dbProfiles, err := dbCluster.GetProfilesIfEnabled(ctx, tx.Tx(), targetProject, profileNames)
		if err != nil {
			return err
		}

		profileConfigs, err := dbCluster.GetReferencedProfileConfigs(ctx, tx.Tx(), dbProfiles)
		if err != nil {
			return err
		}

		profileDevices, err := dbCluster.GetReferencedProfileDevices(ctx, tx.Tx(), dbProfiles)
		if err != nil {
			return err
		}

		profilesByName := make(map[string]dbCluster.Profile, len(dbProfiles))
		for _, dbProfile := range dbProfiles {
			profilesByName[dbProfile.Name] = dbProfile
		}

		// Expansion is order sensitive, so follow the instance's own profile order.
		for _, profileName := range profileNames {
			dbProfile, found := profilesByName[profileName]
			if !found {
				return fmt.Errorf("Requested profile %q doesn't exist in project %q", profileName, targetProject)
			}

			apiProfile, err := dbProfile.ToAPI(ctx, tx.Tx(), profileConfigs, profileDevices)
			if err != nil {
				return err
			}

			profiles = append(profiles, *apiProfile)
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return profiles, nil
}

// checkProjectMoveLive validates that a running instance can be handed over to the target project unchanged.
func checkProjectMoveLive(ctx context.Context, s *state.State, inst instance.Instance, targetProject string, req api.InstancePost) error {
	// Dependent volumes can't follow an instance across projects yet.
	err := inst.ForEachDependentDiskType(func(dev deviceConfig.DeviceNamed) error {
		return fmt.Errorf("Dependent disk %q can't be moved across projects", dev.Name)
	})
	if err != nil {
		return err
	}

	profileNames := req.Profiles
	if profileNames == nil {
		profileNames = make([]string, 0, len(inst.Profiles()))
		for _, profile := range inst.Profiles() {
			profileNames = append(profileNames, profile.Name)
		}
	}

	profiles, err := targetProjectProfiles(ctx, s, targetProject, profileNames)
	if err != nil {
		return err
	}

	localDevices := inst.LocalDevices().CloneNative()
	maps.Copy(localDevices, req.Devices)

	// A live hand-over can't apply a different device set to the running instance.
	targetDevices := db.ExpandInstanceDevices(deviceConfig.NewDevices(localDevices), profiles)
	currentDevices := inst.ExpandedDevices()

	for name, dev := range targetDevices {
		current, ok := currentDevices[name]
		if !ok {
			return fmt.Errorf("Device %q is only present in project %q, which a live migration can't apply", name, targetProject)
		}

		if !maps.Equal(current, dev) {
			return fmt.Errorf("Device %q differs in project %q, which a live migration can't apply", name, targetProject)
		}
	}

	for name := range currentDevices {
		_, ok := targetDevices[name]
		if !ok {
			return fmt.Errorf("Device %q is missing from project %q, which a live migration can't apply", name, targetProject)
		}
	}

	// The target member skips this check, as it receives the instance as a cluster notification.
	localConfig := maps.Clone(inst.LocalConfig())
	maps.Copy(localConfig, req.Config)

	return s.DB.Cluster.Transaction(ctx, func(ctx context.Context, tx *db.ClusterTx) error {
		return project.AllowInstanceCreation(tx, targetProject, api.InstancesPost{
			Name: inst.Name(),
			Type: inst.Type().ToAPI(),
			InstancePut: api.InstancePut{
				Config:   localConfig,
				Devices:  localDevices,
				Profiles: profileNames,
			},
		})
	})
}
