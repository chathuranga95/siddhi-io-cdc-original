/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.extension.siddhi.io.cdc.source;

import io.debezium.config.Configuration;
import io.debezium.embedded.spi.OffsetCommitPolicy;

import java.time.Duration;

/**
 * Let the offsets to commit with the WSO2 SP's periodic snapshot.
 * Offsets are always committed, thus all offsets will be flushed with the periodic snapshot.
 */
public class PeriodicSnapshotCommitOffsetPolicy implements OffsetCommitPolicy {

    // TODO: 10/8/18 talk to Tishan ayiya, without below constructor, it is not working
    public PeriodicSnapshotCommitOffsetPolicy(Configuration config) {
    }

    @Override
    public boolean performCommit(long l, Duration duration) {
        return true;
    }
}