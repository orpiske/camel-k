/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package resume

import (
	v1 "github.com/apache/camel-k/pkg/apis/camel/v1"
	"github.com/apache/camel-k/pkg/metadata"
	"github.com/apache/camel-k/pkg/trait"
	"github.com/apache/camel-k/pkg/util"
	"github.com/apache/camel-k/pkg/util/kubernetes"
	"github.com/apache/camel-k/pkg/util/log"
)

// The Resume trait can be used to manage and configure resume strategies
//
// The Resume trait is disabled by default.
//
// +camel-k:trait=resume.
type resumeTrait struct {
	trait.BaseTrait `property:",squash"`
	// Enables automatic configuration of the trait.
	Auto *bool `property:"auto" json:"auto,omitempty"`
	// The type of the resume strategy to use
	ResumeStrategy string `property:"resume-strategy,omitempty"`
	// The path used by the resume strategy (this is specific to the resume strategy type)
	ResumePath string `property:"resume-path,omitempty"`
	// The address of the resume server to use (protocol / implementation specific)
	ResumeServer string `property:"resume-server,omitempty"`
	// The adapter-specific policy to use when filling the cache (use: minimizing / maximizing). Check
	// the component documentation if unsure
	CacheFillPolicy string `property:"cache-fill-policy,omitempty"`
}

const (
	KafkaSingle   = "org.apache.camel.processor.resume.kafka.SingleNodeKafkaResumeStrategy"
	KafkaMulti    = "org.apache.camel.processor.resume.kafka.MultiNodeKafkaResumeStrategy"
	StrategyPath  = "camel-k-offsets"
	fileComponent = "file"
)

func NewResumeTrait() trait.Trait {
	return &resumeTrait{
		BaseTrait: trait.NewBaseTrait("resume", trait.TraitOrderBeforeControllerCreation),
	}
}

func (r *resumeTrait) Configure(environment *trait.Environment) (bool, error) {
	if trait.IsNilOrFalse(r.Enabled) {
		return false, nil
	}

	if !environment.IntegrationInPhase(v1.IntegrationPhaseInitialization) && !environment.IntegrationInRunningPhases() {
		return false, nil
	}

	if trait.IsNilOrTrue(r.Auto) {
		// Check which components have been used
		sources, err := kubernetes.ResolveIntegrationSources(environment.Ctx, r.Client, environment.Integration, environment.Resources)
		if err != nil {
			return false, err
		}

		meta := metadata.ExtractAll(environment.CamelCatalog, sources)

		for _, endpoint := range meta.FromURIs {
			log.Infof("Processing component %s", endpoint)
		}

		if r.ResumeStrategy == "" {
			r.ResumeStrategy = KafkaSingle
		}

		if r.ResumePath == "" {
			r.ResumePath = StrategyPath
		}
	}

	return r.Enabled != nil && *r.Enabled, nil
}

func (r *resumeTrait) Apply(environment *trait.Environment) error {
	if environment.IntegrationInPhase(v1.IntegrationPhaseInitialization) {
		util.StringSliceUniqueAdd(&environment.Integration.Status.Capabilities, v1.CapabilityResume)
	}

	if environment.IntegrationInRunningPhases() {
		environment.ApplicationProperties["customizer.resume.enabled"] = "true"
		environment.ApplicationProperties["customizer.resume.resumeStrategy"] = r.ResumeStrategy
		environment.ApplicationProperties["customizer.resume.resumePath"] = r.ResumePath
		environment.ApplicationProperties["customizer.resume.resumeServer"] = r.ResumeServer
		environment.ApplicationProperties["customizer.resume.cacheFillPolicy"] = r.CacheFillPolicy
	}

	return nil
}
