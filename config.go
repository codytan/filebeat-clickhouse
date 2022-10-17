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

import "github.com/elastic/beats/v7/libbeat/outputs/codec"

type Config struct {
	Codec      codec.Config `config:"codec"`
	Host       []string     `config:"host"`
	Db         string       `config:"db"`
	UserName   string       `config:"user_name"`
	PassWord   string       `config:"pass_word"`
	BatchSize  int          `config:"batch_size"`
	Pretty     bool
	MaxRetries int `config:"max_retries"`
	Tables     []struct {
		Table   string   `config:"table"`
		Columns []string `config:"columns"`
	} `config:"tables"`
}

var defaultConfig = Config{}
