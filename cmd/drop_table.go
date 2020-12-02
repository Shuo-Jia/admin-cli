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

package cmd

import (
	"fmt"

	"github.com/desertbit/grumble"
	"github.com/pegasus-kv/admin-cli/executor"
	"github.com/pegasus-kv/admin-cli/shell"
)

func init() {
	shell.AddCommand(&grumble.Command{
		Name: "drop",
		Help: "drop a table",
		Run: func(c *grumble.Context) error {
			if len(c.Args) != 1 {
				return fmt.Errorf("must specify a table name")
			}
			return executor.DropTable(pegasusClient, c.Args[0], c.Flags.Duration("reserved"))
		},
		Flags: func(f *grumble.Flags) {
			f.Duration("r", "reserved", 4, "the soft-deletion period, which is the time before table actually deleted")
		},
		AllowArgs: true,
	})
}
