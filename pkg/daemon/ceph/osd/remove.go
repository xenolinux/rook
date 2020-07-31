/*
Copyright 2020 The Rook Authors. All rights reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
	http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package osd

import (
	"fmt"
	"strconv"

	"github.com/pkg/errors"
	"github.com/rook/rook/pkg/clusterd"
	"github.com/rook/rook/pkg/daemon/ceph/client"
	"github.com/rook/rook/pkg/operator/k8sutil"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
)

// RemoveOSDs purges a list of OSDs from the cluster
func RemoveOSDs(context *clusterd.Context, clusterInfo *client.ClusterInfo, osdsToRemove []string) error {

	// Generate the ceph config for running ceph commands similar to the operator
	if err := writeCephConfig(context, clusterInfo); err != nil {
		return errors.Wrap(err, "failed to write the ceph config")
	}

	for _, osdIDStr := range osdsToRemove {
		logger.Infof("removing OSD %q", osdIDStr)
		osdID, err := strconv.Atoi(osdIDStr)
		if err != nil {
			logger.Errorf("invalid OSD ID: %s. %v", osdIDStr, err)
			continue
		}

		if err := removeOSD(context, clusterInfo, osdID); err != nil {
			logger.Errorf("failed to remove OSD %d. %v", osdID, err)
			continue
		}
	}

	return nil
}

func removeOSD(context *clusterd.Context, clusterInfo *client.ClusterInfo, osdID int) error {

	// TODO: Get the CRUSH map to determine
	// 1. The host where the OSD is found
	// 2. If the OSD is down
	hostName := "minikube"
	osdDown := true

	if !osdDown {
		return fmt.Errorf("OSD %d cannot be removed unless it is 'down'", osdID)
	}

	// Mark the OSD as out. If it is not down, it is expected this command will fail, in which case
	// we expect to abort purging the OSD
	output, err := context.Executor.ExecuteCommandWithOutput("ceph", "osd", "out", fmt.Sprintf("osd.%d", osdID))
	if err != nil {
		return errors.Wrapf(err, fmt.Sprintf("cannot remove the osd. %s", output))
	}

	// Remove the OSD deployment
	deploymentName := fmt.Sprintf("rook-ceph-osd-%d", osdID)
	logger.Infof("removing the OSD deployment %q", deploymentName)
	if err := k8sutil.DeleteDeployment(context.Clientset, clusterInfo.Namespace, deploymentName); err != nil {
		if !kerrors.IsNotFound(err) {
			// Continue purging the OSD even if the deployment fails to be deleted
			logger.Errorf("failed to delete deployment for OSD %d. %v", osdID, err)
		}
	}

	// TODO: If running on a PVC, delete the OSD prepare job

	// TODO: If running on a PVC, delete the OSD PVC

	// purge the osd
	output, err = context.Executor.ExecuteCommandWithOutput("ceph", "osd", "purge", fmt.Sprintf("osd.%d", osdID), "--force", "--yes-i-really-mean-it")
	if err != nil {
		return errors.Wrapf(err, fmt.Sprintf("failed ceph status. %s", output))
	}

	// Attempting to remove the parent host. Errors can be ignored if there are other OSDs on the same host
	output, err = context.Executor.ExecuteCommandWithOutput("ceph", "osd", "crush", "rm", hostName)
	if err != nil {
		logger.Infof("unable to remove CRUSH host %q. %v", hostName, err)
	}

	logger.Infof("completed removal of OSD %d", osdID)
	return nil
}

