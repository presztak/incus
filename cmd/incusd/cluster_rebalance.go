package main

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/lxc/incus/v6/internal/server/cluster"
	"github.com/lxc/incus/v6/internal/server/db"
	internalInstance "github.com/lxc/incus/v6/internal/instance"
	dbCluster "github.com/lxc/incus/v6/internal/server/db/cluster"
	"github.com/lxc/incus/v6/internal/server/instance"
	"github.com/lxc/incus/v6/internal/server/state"
	"github.com/lxc/incus/v6/internal/server/task"
	"github.com/lxc/incus/v6/shared/api"
	"github.com/lxc/incus/v6/shared/logger"
)

type Server struct {
	NodeInfo db.NodeInfo
	Resources *api.Resources
	Score uint8
}

// sortAndGroupByArch sorts servers by its score and groups them by cpu architecture.
func sortAndGroupByArch(servers []*Server) map[string][]*Server {
	sort.Slice(servers, func(i, j int) bool {
		return servers[i].Score > servers[j].Score
	})

	result := make(map[string][]*Server)
	for _, s := range servers {
		arch := s.Resources.CPU.Architecture
		_, ok := result[arch]
		if !ok {
			result[arch] = []*Server{}
		}

		result[arch] = append(result[arch], s)
	}

	return result
}

// calculateServersScore calculates score based on memory and CPU usage for servers in cluster.
func calculateServersScore(s *state.State, members []db.NodeInfo) (map[string][]*Server, error) {
	scores := []*Server{}
	for _, member := range members {
		clusterMember, err := cluster.Connect(member.Address, s.Endpoints.NetworkCert(), s.ServerCert(), nil, true)
		if err != nil {
			return nil, fmt.Errorf("Failed to connect to cluster member: %w", err)
		}

		resources, err := clusterMember.GetServerResources()
		if err != nil {
			return nil, fmt.Errorf("Failed to get resources for cluster member: %w", err)
		}

		memoryScore := uint8(float64(resources.Memory.Used) * 100 / float64(resources.Memory.Total))
		cpuScore := uint8((resources.Load.Average1Min * 100) / float64(resources.CPU.Total))
		
		serverScore := (memoryScore + cpuScore) / 2

		scores = append(scores, &Server{NodeInfo: member, Resources: resources, Score: serverScore})
	}

	return sortAndGroupByArch(scores), nil
}

// findInstancesToMigrate finds instances on most busy server in cluster to migrate.
func findInstancesToMigrate(ctx context.Context, s *state.State, server *Server) ([]instance.Instance, error) {
	// Get all instances from our maxscore server
	var dbInstances []dbCluster.Instance
	err := s.DB.Cluster.Transaction(ctx, func(ctx context.Context, tx *db.ClusterTx) error {
		var err error
		dbInstances, err = dbCluster.GetInstances(ctx, tx.Tx(), dbCluster.InstanceFilter{Node: &server.NodeInfo.Name})
		if err != nil {
			return fmt.Errorf("Failed to get instances: %w", err)
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("Failed to get instances: %w", err)
		
	}

	// Filter for instances that can be live migrated
	var instances []instance.Instance
	for _, dbInst := range dbInstances {
		inst, err := instance.LoadByProjectAndName(s, dbInst.Project, dbInst.Name)
		if err != nil {
			return nil, fmt.Errorf("Failed to load instance: %w", err)
		}

		// Do not allow to migrate which doesn't support live migration
		if inst.CanMigrate() != "live-migrate" {
			continue
		}

		// Check if instance is ready for next migration
		last_move := inst.LocalConfig()["volatile.rebalance.last_move"]
		cooldown := s.GlobalConfig.ClusterRebalanceCooldown()
		if last_move != "" {
			v, err := strconv.ParseInt(last_move, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("Failed to parse last_move value: %w", err)
			}

			expiry, err := internalInstance.GetExpiry(time.Unix(v, 0), cooldown)
			if err != nil {
				return nil, fmt.Errorf("Failed to calculate expiration for cooldown time: %w", err)
			}

			if time.Now().Before(expiry) {
				continue
			}
		}
		
		instances = append(instances, inst)
	}

	return instances, nil
}

// migrateInstances is responsible for instances migration from most to less busy server.
func migrateInstances(ctx context.Context, s *state.State, srcServer *Server, dstServer *Server, maxToMigration int64) (int64, error) {
	numOfMigrated := int64(0)
	instances, err := findInstancesToMigrate(ctx, s, srcServer)
	if err != nil {
		return numOfMigrated, fmt.Errorf("Failed to find instances for migration: %w", err)
	}
	logger.Error("instances", logger.Ctx{"instances": instances})

	for _, inst := range instances {

		if numOfMigrated >= maxToMigration {
			return numOfMigrated, nil
		}

		req := api.InstancePost{
			Migration: true,
			Live:      true,
		}

		srcNode, err := cluster.Connect(srcServer.NodeInfo.Address, s.Endpoints.NetworkCert(), s.ServerCert(), nil, true)
		if err != nil {
			return numOfMigrated, fmt.Errorf("Failed to connect to cluster member: %w", err)
		}
		srcNode = srcNode.UseTarget(dstServer.NodeInfo.Name)

		migrationOp, err := srcNode.MigrateInstance(inst.Name(), req)
		if err != nil {
			return numOfMigrated, fmt.Errorf("Migration API failure: %w", err)
		}

		err = migrationOp.Wait()
		if err != nil {
			return numOfMigrated, fmt.Errorf("Failed to wait for migration to finish: %w", err)
		}

		inst.VolatileSet(map[string]string{"volatile.rebalance.last_move": strconv.FormatInt(time.Now().Unix(), 10)})
		numOfMigrated += 1
	}

	return numOfMigrated, nil
}

// rebalance performs cluster re-balancing.
func rebalance(ctx context.Context, s *state.State, servers map[string][]*Server) error {
	rebalanceThreshold := s.GlobalConfig.ClusterRebalanceThreshold()
	rebalanceBatch := s.GlobalConfig.ClusterRebalanceBatch()
	numOfMigrated := int64(0)

	for _, v := range servers {
		logger.Error("Min Server", logger.Ctx{"ID": v[len(v) - 1].NodeInfo.ID, "Name": v[len(v) - 1].NodeInfo.Name, "Score": v[len(v) - 1].Score})
		logger.Error("Max Server", logger.Ctx{"ID": v[0].NodeInfo.ID, "Name": v[0].NodeInfo.Name, "Score": v[0].Score})

		if numOfMigrated >= rebalanceBatch {
			break // Maximum number of instances already migrated in this run.
		}

		if len(v) < 2 {
			continue // Skip if there isn't at least 2 servers with specific arch.
		}

		if v[0].Score - v[len(v) - 1].Score < uint8(rebalanceThreshold) {
			continue // Skip as threshold condition is not met.
		}

		n, err := migrateInstances(ctx, s, v[0], v[len(v) - 1], rebalanceBatch - numOfMigrated)
		if err != nil {
			return fmt.Errorf("Failed to rebalance cluster: %w", err)
		}

		numOfMigrated += n
	}

	return nil
}

func autoRebalanceLoad(ctx context.Context, d *Daemon) error {
	s := d.State()
	rebalanceThreshold := s.GlobalConfig.ClusterRebalanceThreshold()
	if rebalanceThreshold == 0 {
		logger.Error("autoRebalance: No threshold")
		return nil // Skip rebalancing if it's disabled
	}

	leader, err := s.Cluster.LeaderAddress()
	if err != nil {
		if errors.Is(err, cluster.ErrNodeIsNotClustered) {
			logger.Error("autoRebalance: Not clustered")
			return nil // Skip rebalance if not clustered.
		}

		return fmt.Errorf("Failed to get leader cluster member address: %w", err)
	}

	if s.LocalConfig.ClusterAddress() != leader {
		logger.Error("autoRebalance: Not a leader")
		return nil// Skip rebalance if not cluster leader.
	}

	// Get all online members
	var onlineMembers []db.NodeInfo
	err = s.DB.Cluster.Transaction(ctx, func(ctx context.Context, tx *db.ClusterTx) error {
		members, err := tx.GetNodes(ctx)
		if err != nil {
			return fmt.Errorf("Failed getting cluster members: %w", err)
		}

		onlineMembers, err = tx.GetCandidateMembers(ctx, members, nil, "", nil, s.GlobalConfig.OfflineThreshold())
		if err != nil {
			return fmt.Errorf("Failed getting online cluster members: %w", err)
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("Failed getting cluster members: %w", err)
	}

	servers, err := calculateServersScore(s, onlineMembers)
	if err != nil {
		return fmt.Errorf("Failed calculating servers score: %w", err)
	}

	err = rebalance(ctx, s, servers)
	if err != nil {
		return fmt.Errorf("Failed rebalancing cluster: %w", err)
	}

	return nil
}

func autoRebalanceLoadTask(d *Daemon) (task.Func, task.Schedule) {

	f := func(ctx context.Context) {
		err := autoRebalanceLoad(ctx, d)
		if err != nil {
			logger.Error("Failed during cluster auto rebalancing", logger.Ctx{"err": err})
		}
	}

	s := d.State()
	rebalanceFrequency := s.GlobalConfig.ClusterRebalanceFrequency()
	return f, task.Every(time.Duration(rebalanceFrequency) * time.Minute)
}
