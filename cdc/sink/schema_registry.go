// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package sink

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"regexp"
	"strings"

	"github.com/linkedin/goavro/v2"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"go.uber.org/zap"
)

type AvroSchemaManager struct {
	registryUrl string
	cache       map[string]*schemaCacheEntry
}

type schemaCacheEntry struct {
	tiSchemaId int64
	registryId int64
	codec      *goavro.Codec
}

type registerRequest struct {
	Schema     string `json:"schema"`
	SchemaType string `json:"schemaType"`
}

type registerResponse struct {
	Id int `json:"id"`
}

type lookupResponse struct {
	Name       string `json:"name"`
	RegistryId int64  `json:"id"`
	Schema     string `json:"schema"`
}

func NewAvroSchemaManager(registryUrl string) (*AvroSchemaManager, error) {
	registryUrl = strings.TrimRight(registryUrl, "/")
	// Test connectivity to the Schema Registry
	resp, err := http.Get(registryUrl)
	if err != nil {
		return nil, errors.Annotate(err, "Test connection to Schema Registry failed")
	}
	defer resp.Body.Close()

	text, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Annotate(err, "Reading response from Schema Registry failed")
	}

	if string(text[:]) != "{}" {
		return nil, errors.New("Unexpected response from Schema Registry")
	}

	log.Info("Successfully tested connectivity to Schema Registry", zap.String("registryUrl", registryUrl))

	return &AvroSchemaManager{
		registryUrl: registryUrl,
		cache:       make(map[string]*schemaCacheEntry, 1),
	}, nil
}

var regexRemoveSpaces = regexp.MustCompile("\\s")

func (m *AvroSchemaManager) Register(tableName model.TableName, codec *goavro.Codec) error {
	// The Schema Registry expect the JSON to be without newline characters
	reqBody := registerRequest{
		Schema:     regexRemoveSpaces.ReplaceAllString(codec.Schema(), ""),
		SchemaType: "AVRO",
	}
	payload, err := json.Marshal(&reqBody)

	uri := m.registryUrl + "/subjects/" + url.QueryEscape(tableNameToSchemaSubject(tableName)) + "/versions"
	log.Debug("Registering schema", zap.String("uri", uri), zap.ByteString("payload", payload))

	resp, err := http.Post(uri, "application/vnd.schemaregistry.v1+json", bytes.NewReader(payload))
	if err != nil {
		log.Warn("Failed to register schema to the Registry",
			zap.String("uri", uri),
			zap.ByteString("payload", payload))
		return errors.Annotate(err, "Failed to register schema to the Registry")
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return errors.Annotate(err, "Failed to read response from Registry")
	}

	if resp.StatusCode != 200 {
		log.Warn("Failed to register schema to the Registry, HTTP error",
			zap.Int("status", resp.StatusCode),
			zap.String("uri", uri),
			zap.ByteString("requestBody", payload),
			zap.ByteString("responseBody", body))
		return errors.New("Failed to register schema to the Registry, HTTP error")
	}

	var jsonResp registerResponse
	err = json.Unmarshal(body, &jsonResp)

	if err != nil {
		return errors.Annotate(err, "Failed to parse result from Registry")
	}

	if jsonResp.Id == 0 {
		return errors.New(fmt.Sprintf("Illegal schema ID returned from Registry %d", jsonResp.Id))
	}

	log.Info("Registered schema successfully",
		zap.Int("id", jsonResp.Id),
		zap.String("uri", uri),
		zap.ByteString("body", body))

	return nil
}

// TiSchemaId is only used to trigger fetching from the Registry server.
// Calling this method with a tiSchemaId other than that used last time will invariably trigger a RESTful request to the Registry.
// Returns (codec, registry schema ID, error)
func (m *AvroSchemaManager) Lookup(tableName model.TableName, tiSchemaId int64) (*goavro.Codec, int64, error) {
	key := tableNameToSchemaSubject(tableName)
	if entry, exists := m.cache[key]; exists && entry.tiSchemaId == tiSchemaId {
		log.Info("Avro schema lookup cache hit",
			zap.String("key", key),
			zap.Int64("tiSchemaId", tiSchemaId),
			zap.Int64("registryId", entry.registryId))
		return entry.codec, entry.registryId, nil
	}

	log.Info("Avro schema lookup cache miss",
		zap.String("key", key),
		zap.Int64("tiSchemaId", tiSchemaId))

	uri := m.registryUrl + "/subjects/" + url.QueryEscape(tableNameToSchemaSubject(tableName)) + "/versions/latest"
	log.Debug("Querying for latest schema", zap.String("uri", uri))

	req, err := http.NewRequest("GET", uri, nil)
	if err != nil {
		return nil, 0, errors.Annotate(err, "Error constructing request for Registry lookup")
	}
	req.Header.Add("Accept", "application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Warn("Failed to query the registry",
			zap.String("uri", uri))
		return nil, 0, errors.Annotate(err, "Failed to query the registry")
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, errors.Annotate(err, "Failed to read response from Registry")
	}

	if resp.StatusCode != 200 && resp.StatusCode != 404 {
		log.Warn("Failed to query schema from the Registry, HTTP error",
			zap.Int("status", resp.StatusCode),
			zap.String("uri", uri),

			zap.ByteString("responseBody", body))
		return nil, 0, errors.New("Failed to query schema from the Registry, HTTP error")
	}

	if resp.StatusCode == 404 {
		log.Warn("Specified schema not found in Registry",
			zap.String("key", key),
			zap.Int64("tiSchemaId", tiSchemaId))

		return nil, 0, errors.New("Schema not found in Registry")
	}

	var jsonResp lookupResponse
	err = json.Unmarshal(body, &jsonResp)
	if err != nil {
		return nil, 0, errors.Annotate(err, "Failed to parse result from Registry")
	}

	cacheEntry := new(schemaCacheEntry)
	cacheEntry.codec, err = goavro.NewCodec(jsonResp.Schema)
	if err != nil {
		return nil, 0, errors.Annotate(err, "Creating Avro codec failed")
	}
	cacheEntry.registryId = jsonResp.RegistryId
	cacheEntry.tiSchemaId = tiSchemaId
	m.cache[tableNameToSchemaSubject(tableName)] = cacheEntry

	log.Info("Avro schema lookup successful with cache miss",
		zap.Int64("tiSchemaId", cacheEntry.tiSchemaId),
		zap.Int64("registryId", cacheEntry.registryId),
		zap.String("schema", cacheEntry.codec.Schema()))

	return cacheEntry.codec, cacheEntry.registryId, nil
}

// For testing only. Should be idempotent
func (m *AvroSchemaManager) clearRegistry(tableName model.TableName) error {
	uri := m.registryUrl + "/subjects/" + url.QueryEscape(tableNameToSchemaSubject(tableName))
	req, err := http.NewRequest("DELETE", uri, nil)
	if err != nil {
		log.Error("Could not construct request for clearRegistry", zap.String("uri", uri))
		return err
	}
	req.Header.Add("Accept", "application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Error("Could not send delete request to clear Registry")
		return err
	}

	if resp.StatusCode == 200 {
		log.Info("Clearing Registry successful")
		return nil
	}
	if resp.StatusCode == 404 {
		log.Info("Clearing Registry: topic does not exists, no-op", zap.String("uri", uri))
		return nil
	}

	log.Error("Other error when clearing Registry")
	return err
}

func tableNameToSchemaSubject(tableName model.TableName) string {
	// We should guarantee unique names for subjects
	return url.QueryEscape(tableName.Schema) + "%" + url.QueryEscape(tableName.Table)
}
