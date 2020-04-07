/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.mxb.flink.sql.cluster;

import lombok.Builder;
import lombok.Data;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.TaskManagerOptions;

/**
 * Description of the resource to start by the {@link ClusterDescriptor}.
 */
@Data
@Builder
public final class ClusterSpecification {
	private final int masterMemoryMB;
	private final int taskManagerMemoryMB;
	private final int numberTaskManagers;
	private final int slotsPerTaskManager;

	private ClusterSpecification(int masterMemoryMB, int taskManagerMemoryMB, int numberTaskManagers, int slotsPerTaskManager) {
		this.masterMemoryMB = masterMemoryMB;
		this.taskManagerMemoryMB = taskManagerMemoryMB;
		this.numberTaskManagers = numberTaskManagers;
		this.slotsPerTaskManager = slotsPerTaskManager;
	}

	public int getMasterMemoryMB() {
		return masterMemoryMB;
	}

	public int getTaskManagerMemoryMB() {
		return taskManagerMemoryMB;
	}

	public int getNumberTaskManagers() {
		return numberTaskManagers;
	}

	public int getSlotsPerTaskManager() {
		return slotsPerTaskManager;
	}

	@Override
	public String toString() {
		return "ClusterSpecification{" +
			"masterMemoryMB=" + masterMemoryMB +
			", taskManagerMemoryMB=" + taskManagerMemoryMB +
			", numberTaskManagers=" + numberTaskManagers +
			", slotsPerTaskManager=" + slotsPerTaskManager +
			'}';
	}

	public static ClusterSpecification fromConfiguration(Configuration configuration) {
		int slots = configuration.getInteger(TaskManagerOptions.NUM_TASK_SLOTS, 1);

		int jobManagerMemoryMb = Integer.parseInt(configuration.getString(JobManagerOptions.JOB_MANAGER_HEAP_MEMORY));
		int taskManagerMemoryMb = Integer.parseInt(configuration.getString(TaskManagerOptions.TASK_MANAGER_HEAP_MEMORY));

		return new ClusterSpecificationBuilder()
			.masterMemoryMB(jobManagerMemoryMb)
			.taskManagerMemoryMB(taskManagerMemoryMb)
			.numberTaskManagers(1)
			.slotsPerTaskManager(slots)
			.build();
	}
}
