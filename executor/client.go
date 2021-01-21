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
	"fmt"
	"io"
	"math/rand"
	"os"
	"time"

	"github.com/XiaoMi/pegasus-go-client/idl/admin"
	"github.com/XiaoMi/pegasus-go-client/session"
	falcon "github.com/niean/goperfcounter"
	"github.com/pegasus-kv/admin-cli/executor/util"
	"github.com/pegasus-kv/collector/aggregate"
)

// Client represents as a manager of various SDKs that
// can access both Pegasus ReplicaServer and MetaServer.
type Client struct {
	// Every command should use Client as the fmt.Fprint's writer.
	io.Writer

	// to access administration APIs
	Meta *session.MetaManager

	// to obtain perf-counters of ReplicaServers
	Perf *aggregate.PerfClient

	Nodes *util.PegasusNodeManager
}

// NewClient creates a client for accessing Pegasus cluster for use of admin-cli.
func NewClient(writer io.Writer, metaAddrs []string, testAddrs []string, table string, interval int) *Client {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*1000000)
	defer cancel()

	meta := session.NewMetaManager(metaAddrs, session.NewNodeSession)

	tstMeta := session.NewMetaManager(testAddrs, session.NewNodeSession)
	respt, _ := tstMeta.ListApps(ctx, &admin.ListAppsRequest{
		Status: admin.AppStatus_AS_DROPPED,
	})

	var tbList = []string{table}
	for _, tb := range respt.Infos {
		tbList = append(tbList, tb.AppName)
	}
	thread := (interval + 10) / 10

	for thread > 0 {
		go func() {
			fmt.Printf("submit task=%d \n", thread)
			for {
				start := time.Now().Nanosecond()
				resp, err := meta.QueryConfig(ctx, tbList[rand.Intn(len(tbList))])
				falcon.SetGaugeValue("query_meta_proxy_latency", float64(time.Now().Nanosecond()-start))
				if err != nil {
					fmt.Println(err)
					time.Sleep(time.Duration(interval * 1000 * 1000))
					continue
				}
				fmt.Printf("table=%s, id=%v \n", table, resp)
				time.Sleep(time.Duration(interval * 1000 * 1000))
			}
		}()
		thread--
		time.Sleep(time.Second * 10)
	}

	// TODO(wutao): initialize replica-nodes lazily
	resp, err := meta.ListNodes(ctx, &admin.ListNodesRequest{
		Status: admin.NodeStatus_NS_INVALID,
	})
	if err != nil {
		fmt.Fprintf(writer, "fatal: failed to list nodes [%s]\n", err)
		os.Exit(1)
	}

	var replicaAddrs []string
	for _, node := range resp.Infos {
		replicaAddrs = append(replicaAddrs, node.Address.GetAddress())
	}

	return &Client{
		Writer: writer,
		Meta:   meta,
		Nodes:  util.NewPegasusNodeManager(metaAddrs, replicaAddrs),
		Perf:   aggregate.NewPerfClient(metaAddrs),
	}
}
