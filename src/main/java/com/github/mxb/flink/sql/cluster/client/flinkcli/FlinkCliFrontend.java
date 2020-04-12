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

package com.github.mxb.flink.sql.cluster.client.flinkcli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.flink.api.common.*;
import org.apache.flink.api.common.accumulators.AccumulatorHelper;
import org.apache.flink.client.cli.*;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.*;
import org.apache.flink.configuration.*;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.costs.DefaultCostEstimator;
import org.apache.flink.optimizer.plan.FlinkPlan;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plan.StreamingPlan;
import org.apache.flink.optimizer.plandump.PlanJSONDumpGenerator;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.duration.FiniteDuration;

import java.io.File;
import java.io.FileNotFoundException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * 改造自 org.apache.flink.client.cli.CliFrontend
 * 修改: cancel savepoint stop 在执行后返回对应的savepoint位置
 * Implementation of a simple command line frontend for executing programs.
 *
 * @auther          moxianbin
 * @create          2020-04-12 11:36:09
 */
public class FlinkCliFrontend {

	private static final Logger LOG = LoggerFactory.getLogger(FlinkCliFrontend.class);

	// actions
	private static final String ACTION_RUN = "run";
	private static final String ACTION_INFO = "info";
	private static final String ACTION_CANCEL = "cancel";
	private static final String ACTION_STOP = "stop";
	private static final String ACTION_SAVEPOINT = "savepoint";

	// configuration dir parameters
	private static final String CONFIG_DIRECTORY_FALLBACK_1 = "../conf";
	private static final String CONFIG_DIRECTORY_FALLBACK_2 = "conf";

	// --------------------------------------------------------------------------------------------

	private final Configuration configuration;

	private final List<CustomCommandLine<?>> customCommandLines;

	private final Options customCommandLineOptions;

	private final FiniteDuration clientTimeout;

	private final int defaultParallelism;

	public FlinkCliFrontend(
			Configuration configuration,
			List<CustomCommandLine<?>> customCommandLines) {
		this.configuration = Preconditions.checkNotNull(configuration);
		this.customCommandLines = Preconditions.checkNotNull(customCommandLines);

		FileSystem.initialize(configuration, PluginUtils.createPluginManagerFromRootFolder(configuration));

		this.customCommandLineOptions = new Options();

		for (CustomCommandLine<?> customCommandLine : customCommandLines) {
			customCommandLine.addGeneralOptions(customCommandLineOptions);
			customCommandLine.addRunOptions(customCommandLineOptions);
		}

		this.clientTimeout = AkkaUtils.getClientTimeout(this.configuration);
		this.defaultParallelism = configuration.getInteger(CoreOptions.DEFAULT_PARALLELISM);
	}

	private static FlinkCliFrontend getFlinkCliExecutor() {
		// 1. init configuration
		final Configuration configuration = new Configuration();

		// 2. load the custom command lines
		final List<CustomCommandLine<?>> customCommandLines = loadCustomCommandLines(
				configuration);

		FlinkCliFrontend cli = null;
		try {
			cli = new FlinkCliFrontend(
					configuration,
					customCommandLines);
		} catch (Exception e) {
			LOG.error("Fatal error while running command line interface.", e);
		}
		return cli;
	}

	// --------------------------------------------------------------------------------------------
	//  Getter & Setter
	// --------------------------------------------------------------------------------------------

	/**
	 * Getter which returns a copy of the associated configuration.
	 *
	 * @return Copy of the associated configuration
	 */
	public Configuration getConfiguration() {
		Configuration copiedConfiguration = new Configuration();

		copiedConfiguration.addAll(configuration);

		return copiedConfiguration;
	}

	public Options getCustomCommandLineOptions() {
		return customCommandLineOptions;
	}

	public static String savepointHandle(String jobManagerUrl, String jobId, String savePointPath) throws FlinkException {
		final FlinkCliFrontend cli = getFlinkCliExecutor();

		String savePointRealPath = "";
		try {
//            SecurityUtils.install(new SecurityConfiguration(cli.configuration));
			savePointRealPath = SecurityUtils.getInstalledContext()
					.runSecured(() -> cli.savepoint(jobManagerUrl,jobId,savePointPath));
		}
		catch (Exception e) {
			LOG.error("Fatal error while running command line interface.", e);
			throw new FlinkException(e);
		}

		return savePointRealPath;
	}

	public static String cancelHandle(String jobManagerUrl, String jobId, String savePointPath) throws FlinkException {
		final FlinkCliFrontend cli = getFlinkCliExecutor();

		String savePointRealPath = "";
		try {
			savePointRealPath = SecurityUtils.getInstalledContext()
					.runSecured(() -> cli.cancel(jobManagerUrl,jobId,savePointPath));
		}
		catch (Exception e) {
			LOG.error("Fatal error while running command line interface.", e);
			throw new FlinkException(e);
		}

		return savePointRealPath;
	}

	public static String stopHandle(String jobManagerUrl, String jobId, String savePointPath) throws FlinkException {
		final FlinkCliFrontend cli = getFlinkCliExecutor();

		String savePointRealPath = "";
		try {
			savePointRealPath = SecurityUtils.getInstalledContext()
					.runSecured(() -> cli.stop(jobManagerUrl,jobId,savePointPath));
		}
		catch (Exception e) {
			LOG.error("Fatal error while running command line interface.", e);
			throw new FlinkException(e);
		}

		return savePointRealPath;
	}

	public String savepoint(String jobManagerUrl, String jobId, String savePointDir) throws Exception {
		String[] args;
		if (StringUtils.isNullOrWhitespaceOnly(savePointDir)){
			args = new String[]{"-m", jobManagerUrl, jobId};
		} else {
			args = new String[]{"-m", jobManagerUrl, jobId, savePointDir};
		}

		return savepoint(args);
	}

	public String cancel(String jobManagerUrl, String jobId, String savePointDir) throws Exception {
		String[] args;
		if (StringUtils.isNullOrWhitespaceOnly(savePointDir)){
			args = new String[]{"-m", jobManagerUrl , jobId};
		} else {
			args = new String[]{"-m", jobManagerUrl, "-s" ,savePointDir, jobId};
		}

		return cancel(args);
	}

	public String stop(String jobManagerUrl, String jobId, String savePointDir) throws Exception {
		String[] args;
		if (StringUtils.isNullOrWhitespaceOnly(savePointDir)){
			args = new String[]{"-m", jobManagerUrl , jobId};
		} else {
			args = new String[]{"-m", jobManagerUrl, "-p" ,savePointDir, jobId};
		}

		return stop(args);
	}

	// --------------------------------------------------------------------------------------------
	//  Execute Actions
	// --------------------------------------------------------------------------------------------

	/**
	 * Executions the run action.
	 *
	 * @param args Command line arguments for the run action.
	 */
	protected void run(String[] args) throws Exception {
		LOG.info("Running 'run' command.");

		final Options commandOptions = FlinkCliFrontendParser.getRunCommandOptions();

		final Options commandLineOptions = FlinkCliFrontendParser.mergeOptions(commandOptions, customCommandLineOptions);

		final CommandLine commandLine = FlinkCliFrontendParser.parse(commandLineOptions, args, true);

		final RunOptions runOptions = new RunOptions(commandLine);

		// evaluate help flag
		if (runOptions.isPrintHelp()) {
			FlinkCliFrontendParser.printHelpForRun(customCommandLines);
			return;
		}

		if (!runOptions.isPython()) {
			// Java program should be specified a JAR file
			if (runOptions.getJarFilePath() == null) {
				throw new CliArgsException("Java program should be specified a JAR file.");
			}
		}

		final PackagedProgram program;
		try {
			LOG.info("Building program from JAR file");
			program = buildProgram(runOptions);
		}
		catch (FileNotFoundException e) {
			throw new CliArgsException("Could not build the program from JAR file.", e);
		}

		final CustomCommandLine<?> customCommandLine = getActiveCustomCommandLine(commandLine);

		try {
			runProgram(customCommandLine, commandLine, runOptions, program);
		} finally {
			program.deleteExtractedLibraries();
		}
	}

	private <T> void runProgram(
			CustomCommandLine<T> customCommandLine,
			CommandLine commandLine,
			RunOptions runOptions,
			PackagedProgram program) throws ProgramInvocationException, FlinkException {
		final ClusterDescriptor<T> clusterDescriptor = customCommandLine.createClusterDescriptor(commandLine);

		try {
			final T clusterId = customCommandLine.getClusterId(commandLine);

			final ClusterClient<T> client;

			// directly deploy the job if the cluster is started in job mode and detached
			if (clusterId == null && runOptions.getDetachedMode()) {
				int parallelism = runOptions.getParallelism() == -1 ? defaultParallelism : runOptions.getParallelism();

				final JobGraph jobGraph = PackagedProgramUtils.createJobGraph(program, configuration, parallelism);

				final ClusterSpecification clusterSpecification = customCommandLine.getClusterSpecification(commandLine);
				client = clusterDescriptor.deployJobCluster(
					clusterSpecification,
					jobGraph,
					runOptions.getDetachedMode());

				logAndSysout("Job has been submitted with JobID " + jobGraph.getJobID());

				try {
					client.shutdown();
				} catch (Exception e) {
					LOG.info("Could not properly shut down the client.", e);
				}
			} else {
				final Thread shutdownHook;
				if (clusterId != null) {
					client = clusterDescriptor.retrieve(clusterId);
					shutdownHook = null;
				} else {
					// also in job mode we have to deploy a session cluster because the job
					// might consist of multiple parts (e.g. when using collect)
					final ClusterSpecification clusterSpecification = customCommandLine.getClusterSpecification(commandLine);
					client = clusterDescriptor.deploySessionCluster(clusterSpecification);
					// if not running in detached mode, add a shutdown hook to shut down cluster if client exits
					// there's a race-condition here if cli is killed before shutdown hook is installed
					if (!runOptions.getDetachedMode() && runOptions.isShutdownOnAttachedExit()) {
						shutdownHook = ShutdownHookUtil.addShutdownHook(client::shutDownCluster, client.getClass().getSimpleName(), LOG);
					} else {
						shutdownHook = null;
					}
				}

				try {
					client.setPrintStatusDuringExecution(runOptions.getStdoutLogging());
					client.setDetached(runOptions.getDetachedMode());

					LOG.debug("{}", runOptions.getSavepointRestoreSettings());

					int userParallelism = runOptions.getParallelism();
					LOG.debug("User parallelism is set to {}", userParallelism);
					if (ExecutionConfig.PARALLELISM_DEFAULT == userParallelism) {
						userParallelism = defaultParallelism;
					}

					executeProgram(program, client, userParallelism);
				} finally {
					if (clusterId == null && !client.isDetached()) {
						// terminate the cluster only if we have started it before and if it's not detached
						try {
							client.shutDownCluster();
						} catch (final Exception e) {
							LOG.info("Could not properly terminate the Flink cluster.", e);
						}
						if (shutdownHook != null) {
							// we do not need the hook anymore as we have just tried to shutdown the cluster.
							ShutdownHookUtil.removeShutdownHook(shutdownHook, client.getClass().getSimpleName(), LOG);
						}
					}
					try {
						client.shutdown();
					} catch (Exception e) {
						LOG.info("Could not properly shut down the client.", e);
					}
				}
			}
		} finally {
			try {
				clusterDescriptor.close();
			} catch (Exception e) {
				LOG.info("Could not properly close the cluster descriptor.", e);
			}
		}
	}

	/**
	 * Executes the info action.
	 *
	 * @param args Command line arguments for the info action.
	 */
	protected void info(String[] args) throws CliArgsException, FileNotFoundException, ProgramInvocationException {
		LOG.info("Running 'info' command.");

		final Options commandOptions = FlinkCliFrontendParser.getInfoCommandOptions();

		final CommandLine commandLine = FlinkCliFrontendParser.parse(commandOptions, args, true);

		InfoOptions infoOptions = new InfoOptions(commandLine);

		// evaluate help flag
		if (infoOptions.isPrintHelp()) {
			FlinkCliFrontendParser.printHelpForInfo();
			return;
		}

		if (infoOptions.getJarFilePath() == null) {
			throw new CliArgsException("The program JAR file was not specified.");
		}

		// -------- build the packaged program -------------

		LOG.info("Building program from JAR file");
		final PackagedProgram program = buildProgram(infoOptions);

		try {
			int parallelism = infoOptions.getParallelism();
			if (ExecutionConfig.PARALLELISM_DEFAULT == parallelism) {
				parallelism = defaultParallelism;
			}

			LOG.info("Creating program plan dump");

			Optimizer compiler = new Optimizer(new DataStatistics(), new DefaultCostEstimator(), configuration);
			FlinkPlan flinkPlan = ClusterClient.getOptimizedPlan(compiler, program, parallelism);

			String jsonPlan = null;
			if (flinkPlan instanceof OptimizedPlan) {
				jsonPlan = new PlanJSONDumpGenerator().getOptimizerPlanAsJSON((OptimizedPlan) flinkPlan);
			} else if (flinkPlan instanceof StreamingPlan) {
				jsonPlan = ((StreamingPlan) flinkPlan).getStreamingPlanAsJSON();
			}

			if (jsonPlan != null) {
				System.out.println("----------------------- Execution Plan -----------------------");
				System.out.println(jsonPlan);
				System.out.println("--------------------------------------------------------------");
			}
			else {
				System.out.println("JSON plan could not be generated.");
			}

			String description = program.getDescription();
			if (description != null) {
				System.out.println();
				System.out.println(description);
			}
			else {
				System.out.println();
				System.out.println("No description provided.");
			}
		}
		finally {
			program.deleteExtractedLibraries();
		}
	}

	private <T> void listJobs(
			ClusterClient<T> clusterClient,
			boolean showRunning,
			boolean showScheduled,
			boolean showAll) throws FlinkException {
		Collection<JobStatusMessage> jobDetails;
		try {
			CompletableFuture<Collection<JobStatusMessage>> jobDetailsFuture = clusterClient.listJobs();

			logAndSysout("Waiting for response...");
			jobDetails = jobDetailsFuture.get();

		} catch (Exception e) {
			Throwable cause = ExceptionUtils.stripExecutionException(e);
			throw new FlinkException("Failed to retrieve job list.", cause);
		}

		LOG.info("Successfully retrieved list of jobs");

		final List<JobStatusMessage> runningJobs = new ArrayList<>();
		final List<JobStatusMessage> scheduledJobs = new ArrayList<>();
		final List<JobStatusMessage> terminatedJobs = new ArrayList<>();
		jobDetails.forEach(details -> {
			if (details.getJobState() == JobStatus.CREATED) {
				scheduledJobs.add(details);
			} else if (!details.getJobState().isGloballyTerminalState()) {
				runningJobs.add(details);
			} else {
				terminatedJobs.add(details);
			}
		});

		if (showRunning || showAll) {
			if (runningJobs.size() == 0) {
				System.out.println("No running jobs.");
			}
			else {
				System.out.println("------------------ Running/Restarting Jobs -------------------");
				printJobStatusMessages(runningJobs);
				System.out.println("--------------------------------------------------------------");
			}
		}
		if (showScheduled || showAll) {
			if (scheduledJobs.size() == 0) {
				System.out.println("No scheduled jobs.");
			}
			else {
				System.out.println("----------------------- Scheduled Jobs -----------------------");
				printJobStatusMessages(scheduledJobs);
				System.out.println("--------------------------------------------------------------");
			}
		}
		if (showAll) {
			if (terminatedJobs.size() != 0) {
				System.out.println("---------------------- Terminated Jobs -----------------------");
				printJobStatusMessages(terminatedJobs);
				System.out.println("--------------------------------------------------------------");
			}
		}
	}

	private static void printJobStatusMessages(List<JobStatusMessage> jobs) {
		SimpleDateFormat dateFormat = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss");
		Comparator<JobStatusMessage> startTimeComparator = (o1, o2) -> (int) (o1.getStartTime() - o2.getStartTime());
		Comparator<Map.Entry<JobStatus, List<JobStatusMessage>>> statusComparator =
			(o1, o2) -> String.CASE_INSENSITIVE_ORDER.compare(o1.getKey().toString(), o2.getKey().toString());

		Map<JobStatus, List<JobStatusMessage>> jobsByState = jobs.stream().collect(Collectors.groupingBy(JobStatusMessage::getJobState));
		jobsByState.entrySet().stream()
			.sorted(statusComparator)
			.map(Map.Entry::getValue).flatMap(List::stream).sorted(startTimeComparator)
			.forEachOrdered(job ->
				System.out.println(dateFormat.format(new Date(job.getStartTime()))
					+ " : " + job.getJobId() + " : " + job.getJobName()
					+ " (" + job.getJobState() + ")"));
	}

	/**
	 * Executes the STOP action.
	 *
	 * @param args Command line arguments for the stop action.
	 */
	protected String stop(String[] args) throws Exception {
		LOG.info("Running 'stop-with-savepoint' command.");

		final Options commandOptions = FlinkCliFrontendParser.getStopCommandOptions();
		final Options commandLineOptions = FlinkCliFrontendParser.mergeOptions(commandOptions, customCommandLineOptions);
		final CommandLine commandLine = FlinkCliFrontendParser.parse(commandLineOptions, args, false);

		final StopOptions stopOptions = new StopOptions(commandLine);
		if (stopOptions.isPrintHelp()) {
			FlinkCliFrontendParser.printHelpForStop(customCommandLines);
			return "";
		}

		final String[] cleanedArgs = stopOptions.getArgs();

		final String targetDirectory = stopOptions.hasSavepointFlag() && cleanedArgs.length > 0
				? stopOptions.getTargetDirectory()
				: null; // the default savepoint location is going to be used in this case.

		final JobID jobId = cleanedArgs.length != 0
				? parseJobId(cleanedArgs[0])
				: parseJobId(stopOptions.getTargetDirectory());

		final boolean advanceToEndOfEventTime = stopOptions.shouldAdvanceToEndOfEventTime();

		logAndSysout((advanceToEndOfEventTime ? "Draining job " : "Suspending job ") + "\"" + jobId + "\" with a savepoint.");

		final CustomCommandLine<?> activeCommandLine = getActiveCustomCommandLine(commandLine);
		return runClusterAction(
			activeCommandLine,
			commandLine,
			clusterClient -> {
				final String savepointPath;
				try {
					savepointPath = clusterClient.stopWithSavepoint(jobId, advanceToEndOfEventTime, targetDirectory);
				} catch (Exception e) {
					throw new FlinkException("Could not stop with a savepoint job \"" + jobId + "\".", e);
				}
				logAndSysout("Savepoint completed. Path: " + savepointPath);
				return savepointPath;
			});
	}

	/**
	 * Executes the CANCEL action.
	 *
	 * @param args Command line arguments for the cancel action.
	 */
	protected String cancel(String[] args) throws Exception {
		LOG.info("Running 'cancel' command.");

		final Options commandOptions = FlinkCliFrontendParser.getCancelCommandOptions();

		final Options commandLineOptions = FlinkCliFrontendParser.mergeOptions(commandOptions, customCommandLineOptions);

		final CommandLine commandLine = FlinkCliFrontendParser.parse(commandLineOptions, args, false);

		CancelOptions cancelOptions = new CancelOptions(commandLine);

		// evaluate help flag
		if (cancelOptions.isPrintHelp()) {
			FlinkCliFrontendParser.printHelpForCancel(customCommandLines);
			return "";
		}

		final CustomCommandLine<?> activeCommandLine = getActiveCustomCommandLine(commandLine);

		final String[] cleanedArgs = cancelOptions.getArgs();

		if (cancelOptions.isWithSavepoint()) {

			logAndSysout("DEPRECATION WARNING: Cancelling a job with savepoint is deprecated. Use \"stop\" instead.");

			final JobID jobId;
			final String targetDirectory;

			if (cleanedArgs.length > 0) {
				jobId = parseJobId(cleanedArgs[0]);
				targetDirectory = cancelOptions.getSavepointTargetDirectory();
			} else {
				jobId = parseJobId(cancelOptions.getSavepointTargetDirectory());
				targetDirectory = null;
			}

			if (targetDirectory == null) {
				logAndSysout("Cancelling job " + jobId + " with savepoint to default savepoint directory.");
			} else {
				logAndSysout("Cancelling job " + jobId + " with savepoint to " + targetDirectory + '.');
			}

			return runClusterAction(
				activeCommandLine,
				commandLine,
				clusterClient -> {
					final String savepointPath;
					try {
						savepointPath = clusterClient.cancelWithSavepoint(jobId, targetDirectory);
					} catch (Exception e) {
						throw new FlinkException("Could not cancel job " + jobId + '.', e);
					}
					logAndSysout("Cancelled job " + jobId + ". Savepoint stored in " + savepointPath + '.');
					return savepointPath;
				});
		} else {
			final JobID jobId;

			if (cleanedArgs.length > 0) {
				jobId = parseJobId(cleanedArgs[0]);
			} else {
				throw new CliArgsException("Missing JobID. Specify a JobID to cancel a job.");
			}

			logAndSysout("Cancelling job " + jobId + '.');

			runClusterAction(
				activeCommandLine,
				commandLine,
				clusterClient -> {
					try {
						clusterClient.cancel(jobId);
					} catch (Exception e) {
						throw new FlinkException("Could not cancel job " + jobId + '.', e);
					}
					return "";
				});

			logAndSysout("Cancelled job " + jobId + '.');
		}

		return "";
	}

	/**
	 * Executes the SAVEPOINT action.
	 *
	 * @param args Command line arguments for the savepoint action.
	 */
	protected String savepoint(String[] args) throws Exception {
		LOG.info("Running 'savepoint' command.");

		final Options commandOptions = FlinkCliFrontendParser.getSavepointCommandOptions();

		final Options commandLineOptions = FlinkCliFrontendParser.mergeOptions(commandOptions, customCommandLineOptions);

		final CommandLine commandLine = FlinkCliFrontendParser.parse(commandLineOptions, args, false);

		final SavepointOptions savepointOptions = new SavepointOptions(commandLine);

		// evaluate help flag
		if (savepointOptions.isPrintHelp()) {
			FlinkCliFrontendParser.printHelpForSavepoint(customCommandLines);
			return "";
		}

		final CustomCommandLine<?> activeCommandLine = getActiveCustomCommandLine(commandLine);
		String savePointPath = "";

		if (savepointOptions.isDispose()) {
			savePointPath = runClusterAction(
				activeCommandLine,
				commandLine,
				clusterClient -> disposeSavepoint(clusterClient, savepointOptions.getSavepointPath()));
		} else {
			String[] cleanedArgs = savepointOptions.getArgs();

			final JobID jobId;

			if (cleanedArgs.length >= 1) {
				String jobIdString = cleanedArgs[0];

				jobId = parseJobId(jobIdString);
			} else {
				throw new CliArgsException("Missing JobID. " +
					"Specify a Job ID to trigger a savepoint.");
			}

			final String savepointDirectory;
			if (cleanedArgs.length >= 2) {
				savepointDirectory = cleanedArgs[1];
			} else {
				savepointDirectory = null;
			}

			// Print superfluous arguments
			if (cleanedArgs.length >= 3) {
				logAndSysout("Provided more arguments than required. Ignoring not needed arguments.");
			}

			savePointPath = runClusterAction(
				activeCommandLine,
				commandLine,
				clusterClient -> triggerSavepoint(clusterClient, jobId, savepointDirectory));
		}

		return savePointPath;
	}

	/**
	 * Sends a SavepointTriggerMessage to the job manager.
	 */
	private String triggerSavepoint(ClusterClient<?> clusterClient, JobID jobId, String savepointDirectory) throws FlinkException {
		logAndSysout("Triggering savepoint for job " + jobId + '.');
		CompletableFuture<String> savepointPathFuture = clusterClient.triggerSavepoint(jobId, savepointDirectory);

		logAndSysout("Waiting for response...");

		final String savepointPath;

		try {
			savepointPath = savepointPathFuture.get();
		}
		catch (Exception e) {
			Throwable cause = ExceptionUtils.stripExecutionException(e);
			throw new FlinkException("Triggering a savepoint for the job " + jobId + " failed.", cause);
		}

		logAndSysout("Savepoint completed. Path: " + savepointPath);
		logAndSysout("You can resume your program from this savepoint with the run command.");

		return savepointPath;
	}

	/**
	 * Sends a SavepointDisposalRequest to the job manager.
	 */
	private String disposeSavepoint(ClusterClient<?> clusterClient, String savepointPath) throws FlinkException {
		Preconditions.checkNotNull(savepointPath, "Missing required argument: savepoint path. " +
			"Usage: bin/flink savepoint -d <savepoint-path>");

		logAndSysout("Disposing savepoint '" + savepointPath + "'.");

		final CompletableFuture<Acknowledge> disposeFuture = clusterClient.disposeSavepoint(savepointPath);

		logAndSysout("Waiting for response...");

		try {
			disposeFuture.get(clientTimeout.toMillis(), TimeUnit.MILLISECONDS);
		} catch (Exception e) {
			throw new FlinkException("Disposing the savepoint '" + savepointPath + "' failed.", e);
		}

		logAndSysout("Savepoint '" + savepointPath + "' disposed.");
		return savepointPath;
	}

	// --------------------------------------------------------------------------------------------
	//  Interaction with programs and JobManager
	// --------------------------------------------------------------------------------------------

	protected void executeProgram(PackagedProgram program, ClusterClient<?> client, int parallelism) throws ProgramMissingJobException, ProgramInvocationException {
		logAndSysout("Starting execution of program");

		final JobSubmissionResult result = client.run(program, parallelism);

		if (null == result) {
			throw new ProgramMissingJobException("No JobSubmissionResult returned, please make sure you called " +
				"ExecutionEnvironment.execute()");
		}

		if (result.isJobExecutionResult()) {
			logAndSysout("Program execution finished");
			JobExecutionResult execResult = result.getJobExecutionResult();
			System.out.println("Job with JobID " + execResult.getJobID() + " has finished.");
			System.out.println("Job Runtime: " + execResult.getNetRuntime() + " ms");
			Map<String, Object> accumulatorsResult = execResult.getAllAccumulatorResults();
			if (accumulatorsResult.size() > 0) {
				System.out.println("Accumulator Results: ");
				System.out.println(AccumulatorHelper.getResultsFormatted(accumulatorsResult));
			}
		} else {
			logAndSysout("Job has been submitted with JobID " + result.getJobID());
		}
	}

	/**
	 * Creates a Packaged program from the given command line options.
	 *
	 * @return A PackagedProgram (upon success)
	 */
	PackagedProgram buildProgram(ProgramOptions options) throws FileNotFoundException, ProgramInvocationException {
		String[] programArgs = options.getProgramArgs();
		String jarFilePath = options.getJarFilePath();
		List<URL> classpaths = options.getClasspaths();

		// Get assembler class
		String entryPointClass = options.getEntryPointClassName();
		File jarFile = null;
		if (options.isPython()) {
			// If the job is specified a jar file
			if (jarFilePath != null) {
				jarFile = getJarFile(jarFilePath);
			}

			// If the job is Python Shell job, the entry point class name is PythonGateWayServer.
			// Otherwise, the entry point class of python job is PythonDriver
			if (entryPointClass == null) {
				entryPointClass = "org.apache.flink.client.python.PythonDriver";
			}
		} else {
			if (jarFilePath == null) {
				throw new IllegalArgumentException("Java program should be specified a JAR file.");
			}
			jarFile = getJarFile(jarFilePath);
		}

		PackagedProgram program = entryPointClass == null ?
				new PackagedProgram(jarFile, classpaths, programArgs) :
				new PackagedProgram(jarFile, classpaths, entryPointClass, programArgs);

		program.setSavepointRestoreSettings(options.getSavepointRestoreSettings());

		return program;
	}

	/**
	 * Gets the JAR file from the path.
	 *
	 * @param jarFilePath The path of JAR file
	 * @return The JAR file
	 * @throws FileNotFoundException The JAR file does not exist.
	 */
	private File getJarFile(String jarFilePath) throws FileNotFoundException {
		File jarFile = new File(jarFilePath);
		// Check if JAR file exists
		if (!jarFile.exists()) {
			throw new FileNotFoundException("JAR file does not exist: " + jarFile);
		}
		else if (!jarFile.isFile()) {
			throw new FileNotFoundException("JAR file is not a file: " + jarFile);
		}
		return jarFile;
	}

	// --------------------------------------------------------------------------------------------
	//  Logging and Exception Handling
	// --------------------------------------------------------------------------------------------

	/**
	 * Displays an exception message for incorrect command line arguments.
	 *
	 * @param e The exception to display.
	 * @return The return code for the process.
	 */
	private static int handleArgException(CliArgsException e) {
		LOG.error("Invalid command line arguments.", e);

		System.out.println(e.getMessage());
		System.out.println();
		System.out.println("Use the help option (-h or --help) to get help on the command.");
		return 1;
	}

	/**
	 * Displays an optional exception message for incorrect program parametrization.
	 *
	 * @param e The exception to display.
	 * @return The return code for the process.
	 */
	private static int handleParametrizationException(ProgramParametrizationException e) {
		LOG.error("Program has not been parametrized properly.", e);
		System.err.println(e.getMessage());
		return 1;
	}

	/**
	 * Displays a message for a program without a job to execute.
	 *
	 * @return The return code for the process.
	 */
	private static int handleMissingJobException() {
		System.err.println();
		System.err.println("The program didn't contain a Flink job. " +
			"Perhaps you forgot to call execute() on the execution environment.");
		return 1;
	}

	/**
	 * Displays an exception message.
	 *
	 * @param t The exception to display.
	 * @return The return code for the process.
	 */
	private static int handleError(Throwable t) {
		LOG.error("Error while running the command.", t);

		System.err.println();
		System.err.println("------------------------------------------------------------");
		System.err.println(" The program finished with the following exception:");
		System.err.println();

		if (t.getCause() instanceof InvalidProgramException) {
			System.err.println(t.getCause().getMessage());
			StackTraceElement[] trace = t.getCause().getStackTrace();
			for (StackTraceElement ele: trace) {
				System.err.println("\t" + ele);
				if (ele.getMethodName().equals("main")) {
					break;
				}
			}
		} else {
			t.printStackTrace();
		}
		return 1;
	}

	private static void logAndSysout(String message) {
		LOG.info(message);
		System.out.println(message);
	}

	// --------------------------------------------------------------------------------------------
	//  Internal methods
	// --------------------------------------------------------------------------------------------

	private JobID parseJobId(String jobIdString) throws CliArgsException {
		if (jobIdString == null) {
			throw new CliArgsException("Missing JobId");
		}

		final JobID jobId;
		try {
			jobId = JobID.fromHexString(jobIdString);
		} catch (IllegalArgumentException e) {
			throw new CliArgsException(e.getMessage());
		}
		return jobId;
	}

	/**
	 * Retrieves the {@link ClusterClient} from the given {@link CustomCommandLine} and runs the given
	 * {@link ClusterAction} against it.
	 *
	 * @param activeCommandLine to create the {@link ClusterDescriptor} from
	 * @param commandLine containing the parsed command line options
	 * @param clusterAction the cluster action to run against the retrieved {@link ClusterClient}.
	 * @param <T> type of the cluster id
	 * @throws FlinkException if something goes wrong
	 */
	private <T> String runClusterAction(CustomCommandLine<T> activeCommandLine, CommandLine commandLine, ClusterAction<T> clusterAction) throws FlinkException {
		final ClusterDescriptor<T> clusterDescriptor = activeCommandLine.createClusterDescriptor(commandLine);

		final T clusterId = activeCommandLine.getClusterId(commandLine);
		String result = "";
		if (clusterId == null) {
			throw new FlinkException("No cluster id was specified. Please specify a cluster to which " +
				"you would like to connect.");
		} else {
			try {
				final ClusterClient<T> clusterClient = clusterDescriptor.retrieve(clusterId);

				try {
					result = clusterAction.runAction(clusterClient);
				} finally {
					try {
						clusterClient.shutdown();
					} catch (Exception e) {
						LOG.info("Could not properly shut down the cluster client.", e);
					}
				}
			} finally {
				try {
					clusterDescriptor.close();
				} catch (Exception e) {
					LOG.info("Could not properly close the cluster descriptor.", e);
				}
			}
		}
		return result;
	}

	/**
	 * Internal interface to encapsulate cluster actions which are executed via
	 * the {@link ClusterClient}.
	 *
	 * @param <T> type of the cluster id
	 */
	@FunctionalInterface
	private interface ClusterAction<T> {

		/**
		 * Run the cluster action with the given {@link ClusterClient}.
		 *
		 * @param clusterClient to run the cluster action against
		 * @throws FlinkException if something goes wrong
		 */
		String runAction(ClusterClient<T> clusterClient) throws FlinkException;
	}

	// --------------------------------------------------------------------------------------------
	//  Entry point for executable
	// --------------------------------------------------------------------------------------------

	/**
	 * Parses the command line arguments and starts the requested action.
	 *
	 * @param args command line arguments of the client.
	 * @return The return code of the program
	 */
	public int parseParameters(String[] args) {

		// check for action
		if (args.length < 1) {
			FlinkCliFrontendParser.printHelp(customCommandLines);
			System.out.println("Please specify an action.");
			return 1;
		}

		// get action
		String action = args[0];

		// remove action from parameters
		final String[] params = Arrays.copyOfRange(args, 1, args.length);

		try {
			// do action
			switch (action) {
				case ACTION_RUN:
					run(params);
					return 0;
				case ACTION_INFO:
					info(params);
					return 0;
				case ACTION_CANCEL:
					cancel(params);
					return 0;
				case ACTION_STOP:
					stop(params);
					return 0;
				case ACTION_SAVEPOINT:
					savepoint(params);
					return 0;
				case "-h":
				case "--help":
					FlinkCliFrontendParser.printHelp(customCommandLines);
					return 0;
				case "-v":
				case "--version":
					String version = EnvironmentInformation.getVersion();
					String commitID = EnvironmentInformation.getRevisionInformation().commitId;
					System.out.print("Version: " + version);
					System.out.println(commitID.equals(EnvironmentInformation.UNKNOWN) ? "" : ", Commit ID: " + commitID);
					return 0;
				default:
					System.out.printf("\"%s\" is not a valid action.\n", action);
					System.out.println();
					System.out.println("Valid actions are \"run\", \"list\", \"info\", \"savepoint\", \"stop\", or \"cancel\".");
					System.out.println();
					System.out.println("Specify the version option (-v or --version) to print Flink version.");
					System.out.println();
					System.out.println("Specify the help option (-h or --help) to get help on the command.");
					return 1;
			}
		} catch (CliArgsException ce) {
			return handleArgException(ce);
		} catch (ProgramParametrizationException ppe) {
			return handleParametrizationException(ppe);
		} catch (ProgramMissingJobException pmje) {
			return handleMissingJobException();
		} catch (Exception e) {
			return handleError(e);
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Miscellaneous Utilities
	// --------------------------------------------------------------------------------------------

	public static String getConfigurationDirectoryFromEnv() {
		String location = System.getenv(ConfigConstants.ENV_FLINK_CONF_DIR);

		if (location != null) {
			if (new File(location).exists()) {
				return location;
			}
			else {
				throw new RuntimeException("The configuration directory '" + location + "', specified in the '" +
					ConfigConstants.ENV_FLINK_CONF_DIR + "' environment variable, does not exist.");
			}
		}
		else if (new File(CONFIG_DIRECTORY_FALLBACK_1).exists()) {
			location = CONFIG_DIRECTORY_FALLBACK_1;
		}
		else if (new File(CONFIG_DIRECTORY_FALLBACK_2).exists()) {
			location = CONFIG_DIRECTORY_FALLBACK_2;
		}
		else {
			throw new RuntimeException("The configuration directory was not specified. " +
					"Please specify the directory containing the configuration file through the '" +
				ConfigConstants.ENV_FLINK_CONF_DIR + "' environment variable.");
		}
		return location;
	}

	/**
	 * Writes the given job manager address to the associated configuration object.
	 *
	 * @param address Address to write to the configuration
	 * @param config The configuration to write to
	 */
	static void setJobManagerAddressInConfig(Configuration config, InetSocketAddress address) {
		config.setString(JobManagerOptions.ADDRESS, address.getHostString());
		config.setInteger(JobManagerOptions.PORT, address.getPort());
		config.setString(RestOptions.ADDRESS, address.getHostString());
		config.setInteger(RestOptions.PORT, address.getPort());
	}

	public static List<CustomCommandLine<?>> loadCustomCommandLines(Configuration configuration) {
		List<CustomCommandLine<?>> customCommandLines = new ArrayList<>(2);
		customCommandLines.add(new DefaultCLI(configuration));
		return customCommandLines;
	}

	public static List<CustomCommandLine<?>> loadCustomCommandLines(Configuration configuration, String configurationDirectory) {
		List<CustomCommandLine<?>> customCommandLines = new ArrayList<>(2);

		//	Command line interface of the YARN session, with a special initialization here
		//	to prefix all options with y/yarn.
		//	Tips: DefaultCLI must be added at last, because getActiveCustomCommandLine(..) will get the
		//	      active CustomCommandLine in order and DefaultCLI isActive always return true.
		final String flinkYarnSessionCLI = "org.apache.flink.yarn.cli.FlinkYarnSessionCli";
		try {
			customCommandLines.add(
				loadCustomCommandLine(flinkYarnSessionCLI,
					configuration,
					configurationDirectory,
					"y",
					"yarn"));
		} catch (NoClassDefFoundError | Exception e) {
			LOG.warn("Could not load CLI class {}.", flinkYarnSessionCLI, e);
		}

		customCommandLines.add(new DefaultCLI(configuration));

		return customCommandLines;
	}

	// --------------------------------------------------------------------------------------------
	//  Custom command-line
	// --------------------------------------------------------------------------------------------

	/**
	 * Gets the custom command-line for the arguments.
	 * @param commandLine The input to the command-line.
	 * @return custom command-line which is active (may only be one at a time)
	 */
	public CustomCommandLine<?> getActiveCustomCommandLine(CommandLine commandLine) {
		for (CustomCommandLine<?> cli : customCommandLines) {
			if (cli.isActive(commandLine)) {
				return cli;
			}
		}
		throw new IllegalStateException("No command-line ran.");
	}

	/**
	 * Loads a class from the classpath that implements the CustomCommandLine interface.
	 * @param className The fully-qualified class name to load.
	 * @param params The constructor parameters
	 */
	private static CustomCommandLine<?> loadCustomCommandLine(String className, Object... params) throws IllegalAccessException, InvocationTargetException, InstantiationException, ClassNotFoundException, NoSuchMethodException {

		Class<? extends CustomCommandLine> customCliClass =
			Class.forName(className).asSubclass(CustomCommandLine.class);

		// construct class types from the parameters
		Class<?>[] types = new Class<?>[params.length];
		for (int i = 0; i < params.length; i++) {
			Preconditions.checkNotNull(params[i], "Parameters for custom command-lines may not be null.");
			types[i] = params[i].getClass();
		}

		Constructor<? extends CustomCommandLine> constructor = customCliClass.getConstructor(types);

		return constructor.newInstance(params);
	}

}
