// Licensed to Elasticsearch B.V. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Elasticsearch B.V. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package clickhouse

import (
	"fmt"
	"time"

	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/outputs"
	"github.com/elastic/beats/v7/libbeat/outputs/codec"
	cjson "github.com/elastic/beats/v7/libbeat/outputs/codec/json"
	"github.com/elastic/elastic-agent-libs/config"
)

type consoleEvent struct {
	Timestamp time.Time `json:"@timestamp" struct:"@timestamp"`

	// Note: stdlib json doesn't support inlining :( -> use `codec: 2`, to generate proper event
	Fields interface{} `struct:",inline"`
}

type ftField struct {
	Table string `json:"table"`
}

type tableConf map[string][]string

type batchRows map[string]tableData

type tableData struct {
	Table   string
	Columns []string
	Rows    [][]interface{}
}

func init() {
	outputs.RegisterType("clickhouse", makeClickHouse)
}

func makeClickHouse(_ outputs.IndexManager, beat beat.Info, observer outputs.Observer, cfg *config.C) (outputs.Group, error) {
	config := defaultConfig
	err := cfg.Unpack(&config)
	if err != nil {
		return outputs.Fail(err)
	}

	fmt.Printf("======> %+v \n", config)

	var enc codec.Codec
	if config.Codec.Namespace.IsSet() {
		enc, err = codec.CreateEncoder(beat, config.Codec)
		if err != nil {
			return outputs.Fail(err)
		}
	} else {
		enc = cjson.New(beat.Version, cjson.Config{
			Pretty:     config.Pretty,
			EscapeHTML: false,
		})
	}

	index := beat.Beat
	// fmt.Printf("======> %+v", index)

	c, err := newClient(config, observer, enc, index)
	if err != nil {
		return outputs.Fail(fmt.Errorf("clickhouse output initialization failed with: %v", err))
	}

	return outputs.Success(config.BatchSize, config.MaxRetries, c)
}
