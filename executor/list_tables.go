/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package executor

import (
	"context"
	"time"

	"github.com/XiaoMi/pegasus-go-client/idl/admin"
	"github.com/pegasus-kv/admin-cli/executor/util"
	"github.com/pegasus-kv/admin-cli/tabular"
)

// ListTables command.
func ListTables(client *Client, showDropped bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	var status admin.AppStatus
	if showDropped {
		status = admin.AppStatus_AS_DROPPED
	} else {
		status = admin.AppStatus_AS_AVAILABLE
	}

	resp, err := client.Meta.ListApps(ctx, &admin.ListAppsRequest{
		Status: status,
	})
	if err != nil {
		return err
	}

	type dropTableStruct struct {
		AppID          int32  `json:"id"`
		Name           string `json:"name"`
		Status         string `json:"status"`
		PartitionCount int32  `json:"partitionCount"`
		DropTime       string `json:"dropTime"`
		ExpireTime     string `json:"expireTime"`
	}

	type availableTableStruct struct {
		AppID           int32  `json:"id"`
		Name            string `json:"name"`
		Status          string `json:"status"`
		PartitionCount  int32  `json:"partitionCount"`
		FullHealthy     int32  `json:"fullHealthy"`
		UnHealthy       int32  `json:"unhealthy"`
		WriteUnHealthy  int32  `json:"writeUnhealthy"`
		ReadUnHealthy   int32  `json:"readUnhealthy"`
		CreateTime      string `json:"createTime"`
		WReqRateLimit   string `json:"writeReqLimit"`
		WBytesRateLimit string `json:"writeBytesLimit"`
	}

	var tbList []interface{}
	for _, tb := range resp.Infos {
		if status == admin.AppStatus_AS_AVAILABLE {
			fullHealthy, unHealthy, writeUnHealthy, readUnHealthy, err := getPartitionHealthyCount(client, tb)
			if err != nil {
				return err
			}
			tbList = append(tbList, availableTableStruct{
				AppID:           tb.AppID,
				Name:            tb.AppName,
				Status:          tb.Status.String(),
				FullHealthy:     fullHealthy,
				UnHealthy:       unHealthy,
				WriteUnHealthy:  writeUnHealthy,
				ReadUnHealthy:   readUnHealthy,
				PartitionCount:  tb.PartitionCount,
				CreateTime:      util.FormatDate(tb.CreateSecond),
				WReqRateLimit:   tb.Envs["replica.write_throttling"],
				WBytesRateLimit: tb.Envs["replica.write_throttling_by_size"],
			})
		} else if status == admin.AppStatus_AS_DROPPED {
			tbList = append(tbList, dropTableStruct{
				AppID:          tb.AppID,
				Name:           tb.AppName,
				Status:         tb.Status.String(),
				DropTime:       util.FormatDate(tb.DropSecond),
				ExpireTime:     util.FormatDate(tb.ExpireSecond),
				PartitionCount: tb.PartitionCount,
			})
		}

	}

	// formats into tabular
	tabular.Print(client, tbList)
	return nil
}

// return (FullHealthy, UnHealthy, WriteUnHealthy, ReadUnHealthy, Err)
func getPartitionHealthyCount(client *Client, table *admin.AppInfo) (int32, int32, int32, int32, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	resp, err := client.Meta.QueryConfig(ctx, table.AppName)
	if err != nil {
		return 0, 0, 0, 0, err
	}

	var fullHealthy, unHealthy, writeUnHealthy, readUnHealthy int32
	for _, partition := range resp.Partitions {
		var replicaCnt int32
		if partition.Primary == nil {
			writeUnHealthy++
			readUnHealthy++
		} else {
			replicaCnt = int32(len(partition.Secondaries) + 1)
			if replicaCnt >= partition.MaxReplicaCount {
				fullHealthy++
			} else if replicaCnt < 2 {
				writeUnHealthy++
			}
		}
	}
	unHealthy = table.PartitionCount - fullHealthy
	return fullHealthy, unHealthy, writeUnHealthy, readUnHealthy, nil
}
