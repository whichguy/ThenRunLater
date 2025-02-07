/***********************
 * GOOGLE APPS SCRIPT JOB SCHEDULER
 * Full implementation with concurrency control, error handling, and debugging
 * 
 ***********************/

/**
 * Global variable tracking the currently active trigger ID.
 * @type {string|null}
 */
var _ActiveTriggerId = null;

/**
 * Global variable tracking the start time (milliseconds since epoch)
 * of the currently active trigger.
 * @type {number|null}
 */
var _ActiveTriggerStartTime = null;

/**
 * Checks whether the current trigger has been marked for cancellation.
 * 
 * @throws {Error} If arguments are provided (this function takes no parameters).
 * @returns {boolean} True if cancellation was requested, otherwise false.
 */
function isCancelRequested() {
  console.log('isCancelRequested() called');

  if (arguments.length > 0) {
    throw new Error(
      `isCancelRequested() does not accept arguments. Received: ${JSON.stringify(Array.from(arguments))}`
    );
  }

  if (!_ActiveTriggerId) {
    // No active trigger => not inside a scheduled run
    return false;
  }
  // Forward to the scheduler’s method
  const scheduler = new JobScheduler({ debug: false });
  return scheduler.isTriggerCancellationRequested(_ActiveTriggerId);
}

/**
 * Returns how many milliseconds of execution time remain before
 * this trigger run is forcibly stopped by Apps Script quotas.
 *
 * @throws {Error} If arguments are provided (this function takes no parameters).
 * @returns {number|null} Remaining time in ms, or null if not tracked.
 */
function timeRemaining() {
  console.log('timeRemaining() called');

  if (arguments.length > 0) {
    throw new Error(
      `timeRemaining() does not accept arguments. Received: ${JSON.stringify(Array.from(arguments))}`
    );
  }

  if (!_ActiveTriggerId || _ActiveTriggerStartTime == null) {
    // Not in a triggered run
    return null;
  }
  const scheduler = new JobScheduler({ debug: false });
  return scheduler.timeRemaining(_ActiveTriggerId, _ActiveTriggerStartTime);
}

/**
 * Conditionally reschedules the current job if we are close to the
 * Apps Script time limit. 
 *
 * @param {boolean|Array} newArgs - 
 *   If `true`, reuse the same arguments for this step.  
 *   If an array, use the new array of arguments for this step.
 * @throws {Error} If more than one argument is provided or if the argument is not boolean/Array.
 * @returns {[boolean, (string|null)]}
 *  - [true, <jobFileName>] if the job was rescheduled
 *  - [false, null] if enough time remains to continue
 */
function rescheduleCurrentJobIfNeed(newArgs) {
  console.log('rescheduleCurrentJobIfNeed() called');

  is_not_null("_ActiveTriggerId", _ActiveTriggerId);

  const scheduler = new JobScheduler({ debug: true });
  
  // 1) Check if cancellation is requested first
  if (scheduler.isTriggerCancellationRequested(_ActiveTriggerId)) {
    console.log(`Cancellation was requested for triggerId=${_ActiveTriggerId}. Stopping job now.`);
    // Return [true, null] or any other indicator you prefer
    return [true, null];
  }

  // 2) Normal time-based reschedule check
  const remaining = scheduler.timeRemaining(_ActiveTriggerId, _ActiveTriggerStartTime);
  // Adjust threshold as needed
  const TIME_THRESHOLD_MS = 5000;
  if (remaining <= TIME_THRESHOLD_MS) {
    scheduler.log(
      `Time remaining (${remaining} ms) <= threshold (${TIME_THRESHOLD_MS} ms). Rescheduling job now...`,
      'info'
    );
    const newFile = scheduler._internalReschedule(_ActiveTriggerId, newArgs);
    return [true, newFile];
  } else {
    scheduler.log(`Sufficient time remains (${remaining} ms). Continuing...`);
  }
  return [false, null];
}

/**
 * Requests cancellation of a trigger job by its ID.
 * 
 * @param {string} triggerId - The unique ID of the Apps Script trigger.
 * @throws {Error} If triggerId is not provided or not a string.
 * @returns {boolean} Always returns true when the request is set.
 */
function cancelTriggerById(triggerId = is_string("triggerId")) {
  console.log('cancelTriggerById() called');

  if (arguments.length !== 1) {
    throw new Error(
      `cancelTriggerById() expects exactly 1 argument. Received: ${JSON.stringify(Array.from(arguments))}`
    );
  }
  if (typeof triggerId !== 'string') {
    throw new Error(
      `cancelTriggerById() argument must be a string. Received: ${JSON.stringify(triggerId)}`
    );
  }

  const scheduler = new JobScheduler({ debug: false });
  return scheduler.requestTriggerCancellation(triggerId);
}


/**
 * The main JobScheduler class that coordinates job creation, storage, execution, and cleanup.
 */
class JobScheduler {

  /**
   * Constructs a new job scheduler instance.
   * 
   * @param {Object} [config] - Configuration options.
   * @param {string} [config.folderPrefix='ScheduledScript_'] - Folder name prefix for jobs.
   * @param {number} [config.maxTriggers=15] - Maximum concurrent triggers.
   * @param {number} [config.maxRuntime=450000] - Max runtime per trigger (in ms). Default = 7.5 minutes.
   * @param {boolean} [config.debug=true] - Enable debug logging.
   * @throws {Error} If any invalid config values are provided.
   */
  constructor(config = {}) {
    console.log('JobScheduler constructor called with config:', JSON.stringify(config));
    
    config = config ?? {};

    is_not_null("config", config, c => typeof c === "object" && !Array.isArray(c));
     // Validate known fields if present:
    if (config.folderPrefix !== undefined) {
      is_string("folderPrefix", config.folderPrefix);
    }
    if (config.maxTriggers !== undefined) {
      is_num("maxTriggers", config.maxTriggers);
    }
    if (config.maxRuntime !== undefined) {
      is_num("maxRuntime", config.maxRuntime);
    }
    if (config.debug !== undefined) {
      is_boolean("debug", config.debug);
    }

    this.folderPrefix = config.folderPrefix || 'ScheduledScript_';
    this.maxTriggers = config.maxTriggers || 15;
    this.maxRuntime = config.maxRuntime || 450000; // 7.5 minutes
    this.debug = config.debug !== undefined ? config.debug : true;

    // State
    this.activeTriggers = new Set();
    this.initialized = false;

    // Folders
    this.rootFolderName = 'ScheduledScripts';
    this.folders = {
      root: this.rootFolderName,
      jobs: `${this.rootFolderName}/Jobs`,
      locks: `${this.rootFolderName}/Locks`,
      results: `${this.rootFolderName}/Results`,
      deadLetters: `${this.rootFolderName}/DeadLetters`
    };

    // Initialize system
    this.initialize();
  }


  /**
   * Returns how many ms remain in this trigger's allocated runtime.
   * 
   * @param {string} [triggerId=_ActiveTriggerId] - Trigger ID to check.
   * @param {number} [activeTriggerStartTime=_ActiveTriggerStartTime] - Start time in ms since epoch.
   * @returns {number} Milliseconds remaining, or 0 if none.
   */
  timeRemaining(triggerId = _ActiveTriggerId, activeTriggerStartTime = _ActiveTriggerStartTime) {
    console.log('JobScheduler.timeRemaining() called');

    if (triggerId != null) {
      is_string("triggerId", triggerId);
    }
    if (activeTriggerStartTime != null) {
      is_num("activeTriggerStartTime", activeTriggerStartTime);
    }

    if (triggerId == null || activeTriggerStartTime == null) {
      return 0;
    }
    const elapsed = Date.now() - activeTriggerStartTime;
    return Math.max(0, this.maxRuntime - elapsed);
  }

  /**
   * INTERNAL method used by rescheduleCurrentJobIfNeed(...) to reinsert a job.
   * 
   * @param {string} triggerId - The ID of the trigger being rescheduled.
   * @param {boolean|Array} newArgsOrFlag - If `true`, reuse the same arguments, else replace them with the provided array.
   * @throws {Error} If job record or lock file is not found.
   * @returns {string} Name of newly created job file.
   * @private
   */
  _internalReschedule(triggerId = is_string("triggerId"), newArgsOrFlag) {
    console.log(`_internalReschedule() called with triggerId=${triggerId}`);

    is_string("triggerId", triggerId) ;

    const registry = this.getActiveJobsRegistry();
    const jobRecord = registry[triggerId];
    if (!jobRecord) {
      throw new Error(`No active job found for triggerId=${triggerId}`);
    }

    // The job is presumably locked in the 'locks' folder
    const lockFolder = this.getFolder('locks');
    const lockFiles = lockFolder.getFilesByName(jobRecord.jobFileName);
    if (!lockFiles.hasNext()) {
      throw new Error(`Unable to locate lock file: ${jobRecord.jobFileName}`);
    }
    const lockFile = lockFiles.next();

    // Parse the job
    const job = JSON.parse(lockFile.getBlob().getDataAsString());
    const currentIndex = job.metadata.resumeIndex || 0;

    // If user provided an array, replace the step arguments
    if (Array.isArray(newArgsOrFlag)) {
      console.log(
        `_internalReschedule: Replacing step #${currentIndex} args with: ${JSON.stringify(newArgsOrFlag)}`,
        'info'
      );
      job.steps[currentIndex].parameters = newArgsOrFlag;
    } else {
      console.log(
        `_internalReschedule: Reusing existing step #${currentIndex} arguments...`,
        'info'
      );
    }

    // Update job metadata
    job.metadata.created = new Date().toISOString();

    // Insert updated job into the jobs folder
    const newFileName = `${this.folderPrefix}Job_${job.metadata.created.replace(/[:.]/g, '-')}.json`;
    this.getFolder('jobs').createFile(newFileName, JSON.stringify(job));
    console.log(`Rescheduled current job at step=${currentIndex}, new file: ${newFileName}`);

    // Create a near-immediate trigger for re-run
    this.createProcessingTrigger({ scheduleType: 'after', offsetMs: 1000 });

    // Trash the old lock file and unregister the old job
    console.log(`Trashing old lock file: ${lockFile.getName()} for job re-scheduling`);
    lockFile.setTrashed(true);
    this.unregisterActiveJob(triggerId);

    return newFileName;
  }

  /**
   * Picks up a completed result or error from the results or deadLetters folder.
   * 
   * @param {string} functionName - The name/path of the function we expect to have produced the result.
   * @param {string} [tag] - Optional tag to match in the job metadata.
   * @param {boolean} [keepFile=false] - If true, do not trash the file after picking up.
   * @throws {NoResultsFoundError} If no matching result is found.
   * @returns {[any, Object]} The result (success or error) and associated metadata.
   */
  pickup(functionName = is_string("functionName"), tag, keepFile = false) {
    
    is_function("functionName",functionName);

    console.log('JobScheduler.pickup() called');
    if (tag !== undefined && tag !== null) {
      is_string("tag", tag);
    }
    is_boolean("keepFile", keepFile) ;

    const searchOrder = [
      { folder: 'results', prefix: 'Success_' },
      { folder: 'deadLetters', prefix: 'Failed_' }
    ];

    for (const { folder, prefix } of searchOrder) {
      const files = this.getFolder(folder).getFiles();
      while (files.hasNext()) {
        const file = files.next();
        if (!file.getName().startsWith(prefix)) continue;

        const content = this.parseFileContent(file);
        if (content && this.matchesCriteria(content, functionName, tag)) {
          const [actualResult, metadata] = this.formatResult(
            content,
            folder === 'results',
            file.getName()
          );

          if (!keepFile) {
            try {
              file.setTrashed(true);
              console.log(`Deleted result file: ${file.getName()}`);
            } catch (e) {
              console.log(`Failed to delete ${file.getName()}: ${e.message}`);
            }
          }
          return [actualResult, metadata];
        }
      }
    }

    throw new NoResultsFoundError(functionName, tag);
  }

  /**
   * Retrieves a result without removing the file from the system.
   * 
   * @param {string} functionName - The name/path of the function we expect.
   * @param {string} [tag] - An optional tag used when the job was created.
   * @returns {[any, Object] | [null, null]}
   *   - The result and metadata if found, or [null, null] if not found.
   */
  peek(functionName = is_string("functionName"), tag) {
    console.log('JobScheduler.peek() called');

    is_function("functionName",functionName);
    
    try {
      return this.pickup(functionName, tag, true);
    } catch (e) {
      if (e instanceof NoResultsFoundError) {
        return [null, null];
      }
      throw e;
    }
  }

  /**
   * Initializes folder structure and sets up a watchdog trigger.
   * 
   * @private
   * @throws {Error} If folder initialization fails.
   */
  initialize() {
    console.log('JobScheduler.initialize() called');

    try {
      this.setupFolders();
      this.createWatchdogTrigger();
      this.initialized = true;
      console.log('System initialized successfully');
    } catch (e) {
      this.initialized = false;
      throw new Error(`Initialization failed: ${e.message}`);
    }
  }

  /**
   * Ensures the main folders exist (root, jobs, locks, results, deadLetters).
   * 
   * @private
   */
  setupFolders() {
    console.log('JobScheduler.setupFolders() called');

    let rootFolder;
    const rootFolders = DriveApp.getFoldersByName(this.rootFolderName);
    if (!rootFolders.hasNext()) {
      rootFolder = DriveApp.createFolder(this.rootFolderName);
    } else {
      rootFolder = rootFolders.next();
    }
    this.folders.root = rootFolder;

    const subfolders = {
      jobs: 'Jobs',
      locks: 'Locks',
      results: 'Results',
      deadLetters: 'DeadLetters'
    };

    Object.entries(subfolders).forEach(([type, name]) => {
      const folders = rootFolder.getFoldersByName(name);
      if (!folders.hasNext()) {
        this.folders[type] = rootFolder.createFolder(name);
      } else {
        this.folders[type] = folders.next();
      }
    });
  }

  /**
   * Creates a watchdog cleanup trigger if it does not already exist.
   * 
   * @private
   */
  createWatchdogTrigger() {
    console.log('JobScheduler.createWatchdogTrigger() called');

    if (!ScriptApp.getProjectTriggers().some(t => t.getHandlerFunction() === 'watchdogCleanup')) {
      ScriptApp.newTrigger('watchdogCleanup')
        .timeBased()
        .everyHours(6)
        .create();
    }
  }

  /**
   * Starts building a new job for execution.
   * 
   * @param {string} functionPath - Dot/bracket path to the function to be executed.
   * @param {...any} args - Arguments to pass to that function.
   * @throws {FunctionPathError} If the function path is invalid or not a function.
   * @returns {JobBuilder} A builder for adding steps/options.
   */
  create(functionPath = is_string("functionPath"), ...args) {
    console.log(`JobScheduler.create() called with functionPath=${functionPath}`);

    // Validate the top-level function path first
    this.validateFunctionPath(functionPath);

    // Attempt to evaluate any "$expr:..." arguments immediately
    // If evaluation fails, we throw with details.
    const evaluatedArgs = args.map((arg, index) => {
      if (typeof arg === 'string' && arg.startsWith('$expr:')) {
        const expr = arg.slice(6);
        try {
          // Evaluate with an empty context for now
          return this.evaluateExpression(expr, null, {});
        } catch (err) {
          throw new Error(
            `Failed to evaluate expression for argument #${index}:\n` +
            `  Expression: ${expr}\n` +
            `  Reason: ${err.message}`
          );
        }
      }
      // Otherwise, leave the argument as-is
      return arg;
    });

    // Now proceed to the JobBuilder with the arguments
    return new JobBuilder(this).thenAfter(functionPath, ...args);
  }

  /**
   * Validates that a function path is correct and resolves to a function in the global scope.
   * 
   * @param {string} path - Dot/bracket path (e.g. "myObject.myFunc" or "myObject['myFunc']")
   * @throws {FunctionPathError} If the path is malformed or does not resolve to a function.
   */
  validateFunctionPath(path) {
    console.log(`JobScheduler.validateFunctionPath() called for path=${path}`);

    try {
      const fn = this.resolveFunction(path);
      if (typeof fn !== 'function') {
        throw new FunctionPathError(`Path '${path}' does not resolve to a function`);
      }
    } catch (e) {
      throw new FunctionPathError(`Invalid function path '${path}': ${e.message}`);
    }
  }

  /**
   * Resolves a dot/bracket notation path to an actual function in the globalThis object.
   * 
   * @param {string} path - The path to resolve.
   * @throws {FunctionPathError} If any part of the path is missing or invalid.
   * @returns {Function} The resolved function.
   */
  resolveFunction(path = is_string("path")) {
    console.log(`JobScheduler.resolveFunction() called for path=${path}`);

    const validPathRegex = /^([\w$]+(\.[\w$]+|\[\'[\w$]+\'\]|\[\d+\])*)+$/;
    if (!validPathRegex.test(path)) {
      throw new FunctionPathError(`Malformed function path: ${path}`);
    }

    const parts = [];
    const tokenRegex = /([\w$]+)|\[(['"])(.+?)\2\]|\[(\d+)\]/g;
    let match;
    while ((match = tokenRegex.exec(path)) !== null) {
      if (match[1]) {
        parts.push(match[1]);
      } else if (match[3]) {
        parts.push(match[3]);
      } else if (match[4]) {
        parts.push(match[4]);
      }
    }
    if (parts.length === 0) {
      throw new FunctionPathError(`Empty function path: ${path}`);
    }

    let current = globalThis;
    for (const [index, part] of parts.entries()) {
      if (current[part] === undefined) {
        const missingPath = parts.slice(0, index + 1).join('.');
        throw new FunctionPathError(
          `Missing '${part}' in path '${path}' (failed at ${missingPath})`
        );
      }
      current = current[part];
    }
    if (typeof current !== 'function') {
      throw new FunctionPathError(
        `Path '${path}' resolves to non-function (${typeof current})`
      );
    }
    return current;
  }

  /**
   * Main queue processing function. Called by a time-based trigger.  
   * Processes one or more jobs from the queue until out of time or no jobs remain.
   * 
   * @param {string} triggerId - Unique ID of the time-based trigger calling this function.
   * @throws {Error} If the scheduler is not initialized or any job fails to process.
   */
  processQueue(triggerId = is_string("triggerId")) {
    console.log(`JobScheduler.processQueue() called with triggerId=${triggerId}`);

    // Track active trigger + time
    _ActiveTriggerId = triggerId;
    _ActiveTriggerStartTime = Date.now();

    if (!this.initialized) throw new Error('Scheduler not initialized');
    const startTime = Date.now();
    console.log(`Starting queue at ${startTime} (max ${this.maxRuntime} ms)`);

    try {
      this.deleteCurrentTrigger(triggerId);

      let processedCount = 0;
      let skippedAll = true;

      while (Date.now() - startTime < this.maxRuntime) {
        const lockFile = this.getNextJobFile(triggerId);
        if (!lockFile) {
          break; // no more jobs
        }
        let skipThisJob = false, jobContent = null;
        try {
          jobContent = JSON.parse(lockFile.getBlob().getDataAsString());
          if (jobContent?.metadata?.startEarliestTime) {
            const earliest = new Date(jobContent.metadata.startEarliestTime).getTime();
            if (Date.now() < earliest) {
              skipThisJob = true;
            }
          }
        } catch (err) {
          console.log(`Could not parse job ${lockFile.getName()} for time check: ${err.message}`);
        }

        if (skipThisJob && jobContent) {
          this.getFolder('jobs').createFile(lockFile.getName(), JSON.stringify(jobContent));
          lockFile.setTrashed(true);
          continue;
        }
        skippedAll = false;
        processedCount++;
        this.registerActiveJob(triggerId, lockFile.getName(), new Date());

        console.log(`Processing job file ${lockFile.getName()}`);
        try {
          this.processJobFile(lockFile, triggerId);
        } finally {
          console.log(`Finished job file ${lockFile.getName()}`);
          this.unregisterActiveJob(triggerId);
          this.clearTriggerCancellation(triggerId);
        }

        if (Date.now() - startTime >= this.maxRuntime) {
          break;
        }
      }
      console.log(`Processed ${processedCount} jobs in ${Date.now() - startTime} ms`);

      // If everything was skipped, schedule next run at earliest future time
      if (skippedAll && this.hasPendingJobs()) {
        const earliestFuture = this.findEarliestFutureJobTime();
        if (earliestFuture) {
          console.log(`All jobs are in the future. Earliest is ${earliestFuture.toISOString()}`);
          this.createProcessingTrigger({ method: 'at', param: earliestFuture });
          return;
        }
      }
      if (this.hasPendingJobs()) {
        this.createProcessingTrigger();
      }
    } catch (err) {
      console.log(`Queue processing failed: ${err.message}`);
      throw err;
    } finally {
      // Clear the global tracking
      _ActiveTriggerId = null;
      _ActiveTriggerStartTime = null;
    }
  }

  /**
   * Processes a single job from its lock file.
   * 
   * @private
   * @param {GoogleAppsScript.Drive.File} lockFile - The lock file representing the job.
   * @param {string} triggerId - The ID of the trigger that is running this job.
   */
  processJobFile(lockFile = is_not_null("lockFile"), triggerId ) {
    console.log(`Processing lock file: ${lockFile.getName()}`);

    is_non_null_string("triggerId", triggerId) ;

    const startTime = new Date().toISOString();
    let duration;
    let job;

    try {
      job = JSON.parse(lockFile.getBlob().getDataAsString());
      this.validateJobStructure(job);

      // Weekly scheduling check
      if (job.metadata.weeklySchedule) {
        const { daysOfWeek } = job.metadata.weeklySchedule;
        const dayNum = new Date().getDay();
        if (!daysOfWeek.includes(dayNum)) {
          console.log(`Skipping job on day=${dayNum}, not in [${daysOfWeek}]`);
          this.handleRepeat(job);
          lockFile.setTrashed(true);
          return;
        }
      }

      let prevResult = null;
      const results = [];
      let errorOccurred = false;
      const startStep = job.metadata.resumeIndex || 0;

      const context = {} ;

      for (let index = startStep; index < job.steps.length; index++) {
        job.metadata.resumeIndex = index;
        if (this.isTriggerCancellationRequested(triggerId)) {
          console.log(`Cancellation requested for trigger=${triggerId}; aborting job early.`);
          break;
        }
        if (errorOccurred) break;

        try {
          const step = job.steps[index];
          this.updateActiveJobFunction(triggerId, step.functionPath);
          console.log(`Executing step ${index + 1}/${job.steps.length}: ${step.functionPath}`);

          prevResult = this.executeStep(step, prevResult, context); // pass context between calls
          results.push(prevResult);

          if (job.metadata.storeIntermediate) {
            this.saveIntermediateResult(lockFile, step, prevResult);
          }
        } catch (e) {
          errorOccurred = true;
          duration = Date.now() - new Date(startTime).getTime();
          this.handleJobError(lockFile, job, e, index, startTime, duration);
        }
      }
      if (!errorOccurred) {
        duration = Date.now() - new Date(startTime).getTime();
        this.saveFinalResult(lockFile, results, job, startTime, duration);
        this.handleRepeat(job);
        console.log(`Job completed successfully: ${lockFile.getName()}`);
      }
    } catch (e) {
      duration = Date.now() - new Date(startTime).getTime();
      const errorToHandle = e instanceof Error ? e : new Error(`Unexpected error: ${e}`);
      if (job) {
        this.handleJobError(lockFile, job, errorToHandle, -1, startTime, duration);
      } else {
        console.log(`Invalid job file ${lockFile.getName()}: ${errorToHandle.message}`);
        this.handleInvalidJobError(lockFile, errorToHandle, startTime, duration);
      }
      throw errorToHandle;
    } finally {
      this.cleanupLock(lockFile);
    }
  }

  /**
   * Determines if the job should repeat (interval or weekly) and if so, recreates the job file.
   * 
   * @private
   * @param {Object} job - The job object.
   */
  handleRepeat(job = is_not_null("job")) {
    
    is_not_null("job", job) ;

    if (!job.metadata.repeat && !job.metadata.weeklySchedule) return;
    console.log('Repeating job detected - rescheduling next run');

    if (job.metadata.repeat && typeof job.metadata.repeat.intervalMs === 'number') {
      const now = Date.now();
      job.metadata.startEarliestTime = new Date(now + job.metadata.repeat.intervalMs).toISOString();
    } else if (job.metadata.weeklySchedule) {
      const { timeOfDay } = job.metadata.weeklySchedule;
      job.metadata.startEarliestTime = this._computeNextDayAtTime(timeOfDay);
    }

    job.metadata.created = new Date().toISOString();
    const newFileName = `${this.folderPrefix}Job_${job.metadata.created.replace(/[:.]/g, '-')}.json`;
    this.getFolder('jobs').createFile(newFileName, JSON.stringify(job));
    console.log(`Re-scheduled repeating job as ${newFileName}`);
  }

  /**
   * Computes a Date string for the next day (24 hours) at a specific time.
   * 
   * @private
   * @param {string} timeStr - HH:MM string.
   * @returns {string} An ISO date string for tomorrow at the given time.
   */
  _computeNextDayAtTime(timeStr) {
    is_non_null_string("timeStr", timeStr) ;
    
    const [hh, mm] = timeStr.split(':');
    const now = new Date();
    const tomorrow = new Date(now.getTime() + 86400000);
    tomorrow.setHours(parseInt(hh, 10), parseInt(mm, 10), 0, 0);
    return tomorrow.toISOString();
  }

  /**
   * Handles an invalid job file by placing it in deadLetters.
   * 
   * @private
   * @param {GoogleAppsScript.Drive.File} lockFile 
   * @param {Error} error 
   * @param {string} startTime 
   * @param {number} duration 
   */
  handleInvalidJobError(lockFile, error, startTime, duration) {

    is_not_null("lockFile", lockFile);
    is_not_null("error", error ) ;

    const content = {
      success: false,
      error: {
        message: error.message,
        stack: error.stack,
        step: -1
      },
      metadata: {
        started: startTime,
        failedAt: new Date().toISOString(),
        duration: duration,
        fileName: lockFile.getName(),
        jobId: 'N/A'
      }
    };
    this.getFolder('deadLetters').createFile(`Failed_${lockFile.getName()}`, JSON.stringify(content));
    lockFile.setTrashed(true);
  }
  /**
   * Evaluates an expression before passing it to a function.
   * 
   * @param {string} expression - The string representation of the expression.
   * @param {any} prevResult - The result of the previous function execution.
   * @param {Object} context - Shared variables/functions accessible across chained calls.
   * @returns {any} The evaluated result.
   */
  evaluateExpression(expression, prev, context) {
    const safeContext = context || {};

    // We can also just add `prev` directly into the safeContext
    const sandboxKeys   = ['prev', ...Object.keys(safeContext), 'Math', 'JSON'];
    const sandboxValues = [prev, ...Object.values(safeContext), Math, JSON];

    const fnBody = `return (${expression});`;
    const fn = new Function(...sandboxKeys, fnBody);
    return fn(...sandboxValues);
  }


  /**
   * Executes a single step of a job.
   * 
   * @private
   * @param {Object} step - The step object containing functionPath and parameters.
   * @param {*} prevResult - The previous step's result.
   * @returns {*} The result of this step.
   * @throws {JobExecutionError} If the function call fails.
   */
  executeStep(step, prevResult, context = {} ) {

    is_not_null("step", step );
    is_not_null("context", context) ;

    try {
      const fn = this.resolveFunction(step.functionPath);
      
       // Evaluate arguments before function call
      const resolvedArgs = step.parameters.map(arg => {
        if (typeof arg === 'string' && arg.startsWith('$expr:')) {
          return this.evaluateExpression(arg.slice(6), null, context);
        }
        // Otherwise a literal
        return arg;
      });

      console.log(`Calling ${step.functionPath} with ${this.safeStringify(resolvedArgs)} arguments`);
      
      //////
      // Execute the user function
      //////
      const result = fn(...resolvedArgs);

      console.log(`Step ${step.functionPath} returned: ${this.safeStringify(result)}` );
      
      return result;

    } catch (e) {
      throw new JobExecutionError(step.functionPath, e);
    }
  }

  /**
   * Acquires a lock on the given job file by moving it to the locks folder.
   * 
   * @private
   * @param {GoogleAppsScript.Drive.File} jobFile - The job file to lock.
   * @param {string} triggerId - The ID of the current trigger.
   * @returns {GoogleAppsScript.Drive.File|null} The new lock file, or null if lock failed.
   */
  acquireLock(jobFile, triggerId) {
    try {
      const lockName = `lock_${triggerId}_${jobFile.getName()}`;
      const lockFile = jobFile.makeCopy(lockName, this.getFolder('locks'));
      jobFile.setTrashed(true);
      console.log(`Acquired lock: ${lockFile.getName()}`);
      return lockFile;
    } catch (e) {
      console.log(`Lock acquisition failed: ${e.message}`);
      return null;
    }
  }

  /**
   * Cleans up a lock file (trashed).
   * 
   * @private
   * @param {GoogleAppsScript.Drive.File} lockFile - The lock file to trash.
   */
  cleanupLock(lockFile) {
    is_not_null("lockFile",lockFile) ;

    try {
      lockFile.setTrashed(true);
      console.log(`Released lock: ${lockFile.getName()}`);
    } catch (e) {
      console.log(`Lock cleanup failed: ${e.message}`);
    }
  }

  /**
   * Retrieves the next available job file, acquires a lock on it, and returns the lock file.
   * 
   * @private
   * @param {string} triggerId - The ID of the current trigger.
   * @returns {GoogleAppsScript.Drive.File|null} A lock file, or null if none available.
   */
  getNextJobFile(triggerId) {
    is_non_null_string("triggerId", triggerId);

    const scriptLock = LockService.getScriptLock();
    try {
      scriptLock.waitLock(30000);
      const jobsFolder = this.getFolder('jobs');
      const files = jobsFolder.getFiles();
      while (files.hasNext()) {
        const file = files.next();
        if (!this.isLocked(file)) {
          const lockFile = this.acquireLock(file, triggerId);
          if (lockFile) {
            console.log(`Atomic lock acquired for: ${file.getName()}`);
            return lockFile;
          }
        }
      }
      return null;
    } catch (e) {
      console.log(`Script lock error: ${e.message}`);
      return null;
    } finally {
      scriptLock.releaseLock();
    }
  }

  /**
   * Checks if a job file is already locked by looking for a file with the same name in the locks folder.
   * 
   * @private
   * @param {GoogleAppsScript.Drive.File} file
   * @returns {boolean} True if locked, false otherwise.
   */
  isLocked(file) {
    is_not_null("file",file);

    return this.getFolder('locks').getFilesByName(file.getName()).hasNext();
  }

  /**
   * Finds the earliest future job time (based on `metadata.startEarliestTime`) among all jobs.
   * 
   * @private
   * @returns {Date|null} The earliest future time, or null if none found.
   */
  findEarliestFutureJobTime() {
    let earliest = null;
    const files = this.getFolder('jobs').getFiles();
    while (files.hasNext()) {
      const file = files.next();
      try {
        const content = JSON.parse(file.getBlob().getDataAsString());
        if (content?.metadata?.startEarliestTime) {
          const ms = Date.parse(content.metadata.startEarliestTime);
          if (!isNaN(ms)) {
            if (earliest === null || ms < earliest) {
              earliest = ms;
            }
          }
        }
      } catch(e) {
        // parse error, ignore
      }
    }
    return earliest ? new Date(earliest) : null;
  }

  /**
   * Creates a time-based trigger to process the queue. Various scheduling modes:
   * 
   * @param {Object} [options] - Scheduling options.
   * @param {string} [options.scheduleType='after'] - 'once', 'weekly', or 'after'.
   * @param {number} [options.offsetMs=1000] - When scheduleType='after', schedule in N ms.
   * @param {Date|string} [options.isoTime] - When scheduleType='once', the exact time to run.
   * @returns {GoogleAppsScript.Script.Trigger|null} The created trigger or null if creation failed.
   */
  createProcessingTrigger(options) {
    console.log(`JobScheduler.createProcessingTrigger() called with options=${JSON.stringify(options)}`);

    if (!options) {
      options = { scheduleType: 'after', offsetMs: 1000 };
    } else if (typeof options !== 'object') {
      throw new Error(
        `createProcessingTrigger(): options must be an object. Received: ${JSON.stringify(options)}`
      );
    }

    try {
      const currentTriggers = ScriptApp.getProjectTriggers()
        .filter(t => t.getHandlerFunction() === 'processQueue');
      if (currentTriggers.length >= this.maxTriggers) {
        console.log(`Trigger limit reached (${this.maxTriggers})`, 'warning');
        return null;
      }
      let builder = ScriptApp.newTrigger('processQueue').timeBased();
      switch (options.scheduleType) {
        case 'once':
          builder = builder.at(
            typeof options.isoTime === 'string' ? new Date(options.isoTime) : options.isoTime
          );
          break;
        case 'weekly':
          builder = builder.after(1000); 
          break;
        case 'after':
          builder = builder.after(options.offsetMs || 1000);
          break;
        default:
          builder = builder.after(1000);
          break;
      }
      const trigger = builder.create();
      console.log(`Created new trigger: scheduleType=${options.scheduleType}`);
      return trigger;
    } catch (err) {
      console.log(`Trigger creation failed: ${err.message}`);
      return null;
    }
  }

  /**
   * Deletes the current time-based trigger from the project, if found.
   * 
   * @private
   * @param {string} triggerId - The ID of the trigger to delete.
   */
  deleteCurrentTrigger(triggerId) {
    is_non_null_string("triggerId",triggerId);
    
    console.log(`Attempting to delete current trigger ${triggerId}...`);
    try {
      ScriptApp.getProjectTriggers().forEach(trigger => {
        if (trigger.getUniqueId() === triggerId) {
          ScriptApp.deleteTrigger(trigger);
          this.activeTriggers.delete(triggerId);
          console.log(`Deleted current trigger ${triggerId}`);
        }
      });
    } catch (e) {
      console.log(`Trigger deletion failed: ${e.message}`);
    } finally {
      console.log('Finished attempt to delete current trigger');
    }
  }
/**
   * A helper that acquires a script lock, runs a callback (passing it the
   * ScriptProperties object), and releases the lock at the end.
   *
   * @param {Function} callback - A function(props) => any
   * @returns {*} Whatever `callback` returns.
   */
  _withScriptPropertiesLock(callback) {
    const lock = LockService.getScriptLock();
    lock.waitLock(30000); // wait up to 30 seconds
    try {
      const props = PropertiesService.getScriptProperties();
      return callback(props);
    } finally {
      lock.releaseLock();
    }
  }

  /**
   * Sets a script property requesting that a specific trigger be canceled,
   * and also marks `cancelRequested` in the ActiveJobs registry if present.
   *
   * @param {string} triggerId
   * @returns {boolean} Always returns true once set.
   */
  requestTriggerCancellation(triggerId) {
    is_non_null_string("triggerId", triggerId);
    
    return this._withScriptPropertiesLock((props) => {
      // Write "cancel_{triggerId}" property
      props.setProperty(`cancel_${triggerId}`, 'true');
      console.log(`Cancellation requested for triggerId=${triggerId}`);

      // Update the registry if that job is active
      const registry = this._getActiveJobsRegistryInternal(props);
      if (registry[triggerId]) {
        registry[triggerId].cancelRequested = true;
        this._saveActiveJobsRegistryInternal(props, registry);
      }
      return true;
    });
  }

  /**
   * Checks if the script property indicates the trigger has been canceled.
   * We also do this under a lock for consistency, though read-only could be
   * done without a lock if desired.
   *
   * @param {string} triggerId
   * @returns {boolean} True if cancellation is requested, otherwise false.
   */
  isTriggerCancellationRequested(triggerId) {
    is_non_null_string("triggerId", triggerId);

    return this._withScriptPropertiesLock((props) => {
      return props.getProperty(`cancel_${triggerId}`) === 'true';
    });
  }

  /**
   * Clears the 'cancel_{triggerId}' property in script properties.
   *
   * @param {string} triggerId
   */
  clearTriggerCancellation(triggerId) {
    is_non_null_string("triggerId", triggerId);

    this._withScriptPropertiesLock((props) => {
      props.deleteProperty(`cancel_${triggerId}`);
      console.log(`Cleared cancellation for triggerId=${triggerId}`);
    });
  }

  /**
   * Public method to read the "ActiveJobs" registry, all under a lock.
   * Returns an object: { [triggerId]: { jobFileName, ... }, ... }
   */
  getActiveJobsRegistry() {
    return this._withScriptPropertiesLock((props) => {
      return this._getActiveJobsRegistryInternal(props);
    });
  }

  /**
   * Private method that actually fetches and parses the 'ActiveJobs' property.
   * @param {Properties} props - The ScriptProperties object (already locked).
   * @returns {Object}
   */
  _getActiveJobsRegistryInternal(props) {
    const raw = props.getProperty('ActiveJobs');
    if (!raw) return {};
    try {
      return JSON.parse(raw);
    } catch (e) {
      console.log(`Error parsing ActiveJobs JSON: ${e}`);
      return {};
    }
  }

  /**
   * Public method to write the "ActiveJobs" registry under a lock.
   */
  saveActiveJobsRegistry(registry) {
    return this._withScriptPropertiesLock((props) => {
      this._saveActiveJobsRegistryInternal(props, registry);
    });
  }

  /**
   * Private method that writes the 'ActiveJobs' property (assumes lock is held).
   * @param {Properties} props - The ScriptProperties object (already locked).
   * @param {Object} registry - The updated registry to be saved as JSON.
   */
  _saveActiveJobsRegistryInternal(props, registry) {
    is_not_null("registry", registry);
    const freshRaw = props.getProperty('ActiveJobs');
    let freshRegistry = {};
    if (freshRaw) {
      try {
        freshRegistry = JSON.parse(freshRaw);
      } catch (_) {}
    }
    // Overwrite with new
    freshRegistry = registry;
    props.setProperty('ActiveJobs', JSON.stringify(freshRegistry));
  }

  /**
   * Registers a job as active for the specified trigger, storing in the registry
   * (and checking if cancellation was already requested).
   */
  registerActiveJob(triggerId, jobFileName, startTime) {
    is_non_null_string("triggerId", triggerId);
    is_non_null_string("jobFileName", jobFileName);

    this._withScriptPropertiesLock((props) => {
      const registry = this._getActiveJobsRegistryInternal(props);

      registry[triggerId] = {
        jobFileName,
        startedTime: (startTime instanceof Date)
          ? startTime.toISOString()
          : String(startTime),
        currentFunction: null,
        cancelRequested: props.getProperty(`cancel_${triggerId}`) === 'true'
      };

      this._saveActiveJobsRegistryInternal(props, registry);
    });
  }

  /**
   * Updates the currently running function name for a job in the registry.
   * Also re-checks if cancellation was requested.
   */
  updateActiveJobFunction(triggerId, functionName) {
    is_non_null_string("triggerId", triggerId);
    is_non_null_string("functionName", functionName);

    this._withScriptPropertiesLock((props) => {
      const registry = this._getActiveJobsRegistryInternal(props);
      if (!registry[triggerId]) return;

      registry[triggerId].currentFunction = functionName;
      registry[triggerId].cancelRequested = (props.getProperty(`cancel_${triggerId}`) === 'true');
      this._saveActiveJobsRegistryInternal(props, registry);
    });
  }

  /**
   * Removes a job from the active registry (after it completes).
   */
  unregisterActiveJob(triggerId) {
    is_non_null_string("triggerId", triggerId);

    this._withScriptPropertiesLock((props) => {
      const registry = this._getActiveJobsRegistryInternal(props);
      if (registry[triggerId]) {
        delete registry[triggerId];
        this._saveActiveJobsRegistryInternal(props, registry);
      }
    });
  }
  /**
   * Parses the JSON content of a file, returning null on error.
   * 
   * @private
   * @param {GoogleAppsScript.Drive.File} file
   * @returns {Object|null}
   */
  parseFileContent(file) {
    is_non_null_("file",file) ;

    try {
      return JSON.parse(file.getBlob().getDataAsString());
    } catch (e) {
      console.log(`Failed to parse ${file.getName()}: ${e.message}`);
      return null;
    }
  }

  /**
   * Checks if a file's content matches the specified functionName and optional tag.
   * 
   * @private
   * @param {Object} content - Parsed JSON content from the file.
   * @param {string} functionName
   * @param {string} tag
   * @returns {boolean}
   */
  matchesCriteria(content, functionName, tag) {
    if (!content?.metadata?.originalJob) return false;
    const jobMeta = content.metadata.originalJob;
    return (
      jobMeta.steps.some(step => step.functionPath === functionName) &&
      (!tag || (jobMeta.tags && jobMeta.tags.includes(tag)))
    );
  }

  /**
   * Formats the result object for final pickup or peeking.
   * 
   * @private
   * @param {Object} content - The parsed JSON from results/deadLetters.
   * @param {boolean} isSuccess - Whether we are in the results folder.
   * @param {string} fileName - The name of the file.
   * @returns {[any, Object]} [actualResult, metadata]
   */
  formatResult(content, isSuccess, fileName) {
    const metadata = {
      ...content.metadata,
      fileName: fileName,
      jobId: content.metadata.originalJob?.jobId || 'N/A',
      success: isSuccess
    };
    const actualResult = isSuccess ? content.results : content.error;
    return [actualResult, metadata];
  }

  /**
   * Validates the structure of a job object, ensuring it has a steps array with functionPath + parameters.
   * 
   * @private
   * @param {Object} job
   * @throws {JobValidationError} If the structure is invalid.
   */
  validateJobStructure(job) {
    if (job == null )
      return ;

    if (!job.steps || !Array.isArray(job.steps)) {
      throw new JobValidationError('Invalid job structure: missing steps array');
    }
    job.steps.forEach(step => {
      if (!step.functionPath || typeof step.functionPath !== 'string') {
        throw new JobValidationError('Invalid step structure: missing functionPath');
      }
      if (!step.parameters || !Array.isArray(step.parameters)) {
        throw new JobValidationError('Invalid step structure: missing parameters array');
      }
    });
  }

  /**
   * Appends a log message with a given severity, optionally suppressed if debug=false.
   * 
   * @param {string} message - The message to log.
   * @param {'debug'|'info'|'warning'|'error'} [level='info'] - Severity level.
   */
  log(message, level = 'info') {
    if (!this.debug && level === 'debug') return;
    const timestamp = new Date().toISOString();
    console.log(`[${timestamp}] [${level.toUpperCase()}] ${message}`);
  }

  /**
   * Safely stringifies an object for logging, handling circular references and functions.
   * 
   * @private
   * @param {any} obj
   * @returns {string}
   */
  safeStringify(obj) {
    try {
      return JSON.stringify(obj, (key, value) => {
        if (typeof value === 'function') return '<function>';
        if (value instanceof Error) return `<Error: ${value.message}>`;
        return value;
      });
    } catch (e) {
      return '<unserializable object>';
    }
  }

  /**
   * Retrieves one of the known folders (root, jobs, locks, results, deadLetters).
   * 
   * @private
   * @param {string} type - The folder key in this.folders.
   * @throws {Error} If the folder is not initialized.
   * @returns {GoogleAppsScript.Drive.Folder}
   */
  getFolder(type) {
    const folder = this.folders[type];
    if (!folder) {
      throw new Error(`Folder not initialized: ${type}`);
    }
    return folder;
  }

  /**
   * Checks if there are any pending jobs in the "jobs" folder.
   * 
   * @returns {boolean}
   */
  hasPendingJobs() {
    const files = this.getFolder('jobs').getFiles();
    return files.hasNext();
  }

  /**
   * Handles general job error: writes file to deadLetters, includes error info, etc.
   * 
   * @private
   * @param {GoogleAppsScript.Drive.File} lockFile - The lock file representing the job.
   * @param {Object} job - The job object.
   * @param {Error} error - The error thrown during execution.
   * @param {number} stepIndex - The step index at which it failed.
   * @param {string} startTime - When the job started (ISO).
   * @param {number} duration - How long it ran (ms).
   */
  handleJobError(lockFile, job, error, stepIndex, startTime, duration) {
    const baseName = lockFile.getName().replace(/^lock_[^_]+_/, '');
  
    const content = {
      success: false,
      error: {
        message: error.message,
        stack: error.stack,
        step: stepIndex
      },
      metadata: {
        started: startTime,
        failedAt: new Date().toISOString(),
        duration: duration,
        originalJob: job,
        fileName: lockFile.getName()
      }
    };
    this.getFolder('deadLetters').createFile(`Failed_${baseName}`, JSON.stringify(content));
  }

  /**
   * Saves intermediate step result if storeIntermediate is set.
   * 
   * @private
   * @param {GoogleAppsScript.Drive.File} lockFile 
   * @param {Object} step 
   * @param {*} prevResult 
   */
  saveIntermediateResult(lockFile, step, prevResult) {
    // Implementation could store partial results in a separate location
    console.log(`Storing intermediate result for step: ${step.functionPath}`);
  }

  /**
   * Saves final results to the results folder on successful completion.
   * 
   * @private
   * @param {GoogleAppsScript.Drive.File} lockFile
   * @param {Array<any>} results 
   * @param {Object} job 
   * @param {string} startTime 
   * @param {number} duration 
   */
  saveFinalResult(lockFile, results, job, startTime, duration) {
    const baseName = lockFile.getName().replace(/^lock_[^_]+_/, '');
    const content = {
      success: true,
      results: results,
      metadata: {
        started: startTime,
        finishedAt: new Date().toISOString(),
        duration: duration,
        originalJob: job,
        fileName: lockFile.getName()
      }
    };
    this.getFolder('results').createFile(`Success_${baseName}`, JSON.stringify(content));
  }

  /**
   * Checks if the required script/drive permissions are granted.
   * 
   * @throws {Error} If the permissions are not granted.
   */
  verifyPermissions() {
    try {
      ScriptApp.getProjectTriggers();
      DriveApp.getRootFolder();
    } catch (e) {
      throw new Error(`Missing required permissions:\n${e.message}`);
    }
  }
}

// ========== ERROR CLASSES ==========

/**
 * Thrown when no results are found for pickup/peek.
 */
class NoResultsFoundError extends Error {
  /**
   * @param {string} functionName 
   * @param {string} [tag]
   */
  constructor(functionName, tag) {

    super(`No results found for ${functionName}${tag ? ` with tag "${tag}"` : ''}`);
    this.name = 'NoResultsFoundError';
    this.functionName = functionName;
    this.tag = tag;
  }
}

/**
 * Thrown for invalid job structure or step definitions.
 */
class JobValidationError extends Error {
  /**
   * @param {string} message 
   */
  constructor(message) {
    super(`Job validation error: ${message}`);
    this.name = 'JobValidationError';
  }
}

/**
 * Thrown when a function path is invalid or does not resolve to a function.
 */
class FunctionPathError extends Error {
  /**
   * @param {string} message 
   */
  constructor(message) {
    super(`Function path error: ${message}`);
    this.name = 'FunctionPathError';
  }
}

/**
 * Thrown when execution of a job step function fails.
 */
class JobExecutionError extends Error {
  /**
   * @param {string} path 
   * @param {Error} originalError 
   */
  constructor(path, originalError) {
    super(`Job execution failed at ${path}: ${originalError.message}`);
    this.name = 'JobExecutionError';
    this.stack = originalError.stack;
    this.stepPath = path;
    this.originalError = originalError;
  }
}

/**
 * The JobBuilder class is used to create multi-step jobs
 * with optional repeats, advanced scheduling, etc.
 */
class JobBuilder {
  /**
   * @param {JobScheduler} scheduler - An instance of the JobScheduler.
   */
  constructor(scheduler) {
    if (!(scheduler instanceof JobScheduler)) {
      throw new Error(
        `JobBuilder constructor expects a JobScheduler instance. Received: ${JSON.stringify(scheduler)}`
      );
    }
    this.scheduler = scheduler;
    this.job = {
      steps: [],
      metadata: {
        created: new Date().toISOString(),
        maxRetries: 3,
        storeIntermediate: false,
        tags: []
      }
    };
  }

  /**
   * Adds a step to the job which executes `functionPath` with `...args`.
   * 
   * @param {string} functionPath - Path to the function to call.
   * @param {...any} args - Arguments for that function.
   * @returns {JobBuilder} The builder for chaining.
   */
  thenAfter(functionPath, ...args) {
    console.log(`JobBuilder.thenAfter() called with functionPath=${functionPath}`);

    this.scheduler.validateFunctionPath(functionPath);
    const processedArgs = this.processArgs(args);
    this.job.steps.push({
      functionPath: functionPath,
      parameters: processedArgs
    });
    return this;
  }

  /**
   * Internal helper for argument serialization (handles PREV, Date, etc.).
   * 
   * @private
   * @param {Array<any>} args 
   * @returns {Array<any>}
   */
  processArgs(args) {
    return args.map(arg => {
      if (arg === this.PREV ) {
        return this.PREV ;
      }
      if (arg instanceof Date) {
        return { __type: 'Date', value: arg.toISOString() };
      }
      if (typeof arg === 'function') {
        throw new JobValidationError('Cannot serialize functions as parameters');
      }
      return arg;
    });
  }

  /**
   * Sets additional metadata options for this job.
   * 
   * @param {Object} options
   * @returns {JobBuilder}
   */
  withOptions(options) {
    console.log(`JobBuilder.withOptions() called with options=${JSON.stringify(options)}`);
    Object.assign(this.job.metadata, options);
    console.log(`Set job options: ${JSON.stringify(options)}`);
    return this;
  }

  /**
   * Finalizes the job and schedules it for execution. 
   * 
   * @param {Object} [clockSchedule] - If provided, used by createProcessingTrigger to set timing.
   * @returns {[string, GoogleAppsScript.Script.Trigger|null]} 
   *  An array: [fileName, trigger], where fileName is the newly created job file,
   *  and trigger is the new or existing time-based trigger (or null).
   * @throws {JobValidationError} If job scheduling fails or job is invalid.
   */
  schedule(clockSchedule) {
    console.log(`JobBuilder.schedule() called with clockSchedule=${JSON.stringify(clockSchedule)}`);

    this.validateJob();
    const fileName = `${this.scheduler.folderPrefix}Job_${this.job.metadata.created.replace(/[:.]/g, '-')}.json`;
    let trigger = null;
    try {
      this.getFolder('jobs').createFile(fileName, JSON.stringify(this.job));
      Logger.log(`Created job file ${fileName}: ${JSON.stringify(this.job)}`);
      trigger = this.scheduler.createProcessingTrigger(clockSchedule);
      return [fileName, trigger];
    } catch (err) {
      throw new JobValidationError(`Job scheduling failed: ${err.message}`);
    }
  }

  /**
   * Returns a folder reference from the scheduler.
   * 
   * @private
   * @param {string} type 
   * @returns {GoogleAppsScript.Drive.Folder}
   */
  getFolder(type) {
    return this.scheduler.getFolder(type);
  }

  /**
   * Ensures this job is valid (has steps, valid function paths, etc.).
   * 
   * @private
   */
  validateJob() {
    if (this.job.steps.length === 0) {
      throw new JobValidationError('Job must contain at least one step');
    }
    this.job.steps.forEach(step => {
      if (!step.functionPath || typeof step.functionPath !== 'string') {
        throw new JobValidationError(`Invalid function path in step: ${JSON.stringify(step)}`);
      }
    });
    if (this.job.metadata.maxRetries < 0 || this.job.metadata.maxRetries > 10) {
      throw new JobValidationError(
        `maxRetries must be between 0-10, got ${this.job.metadata.maxRetries}`
      );
    }
  }
}

// ========== GLOBAL HANDLERS ==========

/**
 * Runs when the script is installed. Initializes a new JobScheduler instance.
 */
function onInstall() {
  console.log('onInstall() called');
  const scheduler = new JobScheduler();
  scheduler.log('Installation completed');
}

/**
 * Trigger handler for processing the queue of jobs.  
 * If not triggered by ScriptApp, sets triggerUid to "manual-execution".
 * 
 * @param {Object} e - Event data provided by the Apps Script trigger.
 */
function processQueue(e) {
  console.log('processQueue() top-level function called');
  const triggerUid = (e && e.triggerUid) ? e.triggerUid : 'manual-execution';
  let scheduler;

  Logger.log('processQueue started...');
  try {
    scheduler = new JobScheduler();
    scheduler.processQueue(triggerUid);
  } catch (err) {
    if (scheduler) {
      scheduler.log(`Queue processing failed: ${err.message}`);
    } else {
      console.error(`Critical failure before scheduler initialization: ${err.message}`);
    }
    throw err;
  } finally {
    Logger.log('processQueue ended');
  }
}

/**
 * Periodic cleanup job (every 6 hours) to remove stale locks and old results.
 */
function watchdogCleanup() {
  console.log('watchdogCleanup() called');
  const scheduler = new JobScheduler();
  try {
    scheduler.log('Starting watchdog cleanup');
    // Clean up old locks (> 15 minutes old)
    const lockCutoff = new Date(Date.now() - 15 * 60 * 1000);
    scheduler.getFolder('locks').getFiles().forEach(file => {
      if (file.getDateCreated() < lockCutoff) {
        file.setTrashed(true);
        scheduler.log(`Cleaned stale lock: ${file.getName()}`);
      }
    });
    // Clean up old results (> 30 days old)
    const resultCutoff = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000);
    ['results', 'deadLetters'].forEach(type => {
      scheduler.getFolder(type).getFiles().forEach(file => {
        if (file.getDateCreated() < resultCutoff) {
          file.setTrashed(true);
          scheduler.log(`Cleaned old ${type} file: ${file.getName()}`);
        }
      });
    });
    // Check for orphaned jobs (jobs with no triggers)
    const hasPendingJobs = scheduler.hasPendingJobs();
    const currentTriggers = ScriptApp.getProjectTriggers()
      .filter(t => t.getHandlerFunction() === 'processQueue').length;
    scheduler.log(`Trigger check - Jobs: ${hasPendingJobs}, Triggers: ${currentTriggers}/${scheduler.maxTriggers}`);
    if (hasPendingJobs) {
      if (currentTriggers === 0) {
        scheduler.log('Found pending jobs with no triggers - restarting processing');
        scheduler.createProcessingTrigger();
      } else if (currentTriggers < scheduler.maxTriggers) {
        const needed = scheduler.maxTriggers - currentTriggers;
        scheduler.log(`Adding ${needed} triggers to reach capacity`);
        for (let i = 0; i < needed; i++) {
          scheduler.createProcessingTrigger();
        }
      }
    }
    scheduler.log('Watchdog cleanup completed');
  } catch (e) {
    scheduler.log(`Watchdog cleanup failed: ${e.message}`);
  }
}
JobScheduler.PREV = '__PREV__';

////////////////////////////////
//
// Argument Validation Functions
//
////////////////////////////////
////////////////////////////////
//
// Updated Argument Validation Functions
//
////////////////////////////////

/**
 * Internal helper that parses up to three arguments for a validator:
 *   1. If `args[0]` is a string, we treat that as `varName`.
 *      - `value` = `args[1]` (or undefined if not present)
 *      - `customValidator` = `args[2]` or a no-op validator
 *   2. Otherwise, `varName = "Value"`,
 *      - `value` = `args[0]`
 *      - `customValidator` = `args[1]` or a no-op validator
 *
 * @param {Array<*>} args - The raw arguments (spread).
 * @returns {{ varName: string, value: *, customValidator: Function }}
 */
function parseValidatorArgs(args) {
  if (!args || args.length === 0) {
    throw new Error("No arguments provided to validator.");
  }
  let varName = "Value";
  let value, customValidator;
  
  if (typeof args[0] === "string") {
    // (varName[, value[, customValidator]])
    varName = args[0];
    value = args.length > 1 ? args[1] : undefined;
    customValidator = args.length > 2 ? args[2] : (x => true);
  } else {
    // ([value[, customValidator]])
    value = args[0];
    customValidator = args.length > 1 ? args[1] : (x => true);
  }
  return { varName, value, customValidator };
}

/**
 * Validates that a value is not null or undefined.
 */
function is_not_null(...args) {
  const { varName, value, customValidator } = parseValidatorArgs(args);
  if (value === undefined) {
    throw new Error(`Argument ${varName} was not defined but is required (not null/undefined).`);
  }
  if (value === null) {
    throw new Error(`Argument ${varName} must not be null.`);
  }
  if (!customValidator(value)) {
    throw new Error(`Argument ${varName} failed custom validation.`);
  }
  return value;
}

/**
 * Validates that a value is a valid number (not NaN).
 */
function is_num(...args) {
  const { varName, value, customValidator } = parseValidatorArgs(args);
  if (value === undefined) {
    throw new Error(`Argument ${varName} was not defined but must be a valid number.`);
  }
  if (typeof value !== "number" || isNaN(value)) {
    throw new Error(`Argument ${varName} must be a valid number.`);
  }
  if (!customValidator(value)) {
    throw new Error(`Argument ${varName} failed custom validation.`);
  }
  return value;
}

/**
 * Validates that a number is within the [min, max] range.
 */
function is_num_between(...args) {
  if (args.length < 3) {
    throw new Error("Insufficient arguments for is_num_between. Need (value, min, max).");
  }
  
  let varName, value, min, max, customValidator;
  if (typeof args[0] === "string") {
    [varName, value, min, max, customValidator] = args;
  } else {
    varName = "Value";
    [value, min, max, customValidator] = args;
  }
  customValidator = customValidator || (x => true);

  is_num(varName, value);
  if (value < min || value > max) {
    throw new Error(`Argument ${varName} must be a number between ${min} and ${max}.`);
  }
  if (!customValidator(value)) {
    throw new Error(`Argument ${varName} failed custom validation.`);
  }
  return value;
}

/**
 * Validates that a value is a string.
 */
function is_string(...args) {
  const { varName, value, customValidator } = parseValidatorArgs(args);
  if (value === undefined) {
    throw new Error(`Argument ${varName} was not defined but must be a string.`);
  }
  if (typeof value !== "string") {
    throw new Error(`Argument ${varName} must be a string.`);
  }
  if (!customValidator(value)) {
    throw new Error(`Argument ${varName} failed custom validation.`);
  }
  return value;
}

/**
 * Validates that a value is a non-null string.
 */
function is_non_null_string(...args) {
  const { varName, value, customValidator } = parseValidatorArgs(args);
  is_not_null(varName, value); // ensures not undefined or null
  if (typeof value !== "string") {
    throw new Error(`Argument ${varName} must be a non-null string.`);
  }
  if (!customValidator(value)) {
    throw new Error(`Argument ${varName} failed custom validation.`);
  }
  return value;
}

/**
 * Validates that a value is a non-empty string.
 */
function is_not_empty(...args) {
  const { varName, value, customValidator } = parseValidatorArgs(args);
  is_non_null_string(varName, value); // must be non-null, and string
  if (value.length === 0) {
    throw new Error(`Argument ${varName} must not be an empty string.`);
  }
  if (!customValidator(value)) {
    throw new Error(`Argument ${varName} failed custom validation.`);
  }
  return value;
}

/**
 * Validates that a value is an array.
 */
function is_array(...args) {
  const { varName, value, customValidator } = parseValidatorArgs(args);
  if (value === undefined) {
    throw new Error(`Argument ${varName} was not defined but must be an array.`);
  }
  if (!Array.isArray(value)) {
    throw new Error(`Argument ${varName} must be an array.`);
  }
  if (!customValidator(value)) {
    throw new Error(`Argument ${varName} failed custom validation.`);
  }
  return value;
}

/**
 * Validates that a value is an array of arrays (2D).
 */
function is_double_array(...args) {
  const { varName, value, customValidator } = parseValidatorArgs(args);
  is_array(varName, value); // ensure it's an array first
  for (let i = 0; i < value.length; i++) {
    if (!Array.isArray(value[i])) {
      throw new Error(
        `Argument ${varName} must be an array of arrays. Element at index ${i} is not an array.`
      );
    }
  }
  if (!customValidator(value)) {
    throw new Error(`Argument ${varName} failed custom validation.`);
  }
  return value;
}

/**
 * Validates that a value is a valid Date.
 */
function is_date(...args) {
  const { varName, value, customValidator } = parseValidatorArgs(args);
  if (value === undefined) {
    throw new Error(`Argument ${varName} was not defined but must be a valid Date object.`);
  }
  if (!(value instanceof Date) || isNaN(value.getTime())) {
    throw new Error(`Argument ${varName} must be a valid Date object.`);
  }
  if (!customValidator(value)) {
    throw new Error(`Argument ${varName} failed custom validation.`);
  }
  return value;
}

/**
 * Validates that a value is a boolean.
 */
function is_boolean(...args) {
  const { varName, value, customValidator } = parseValidatorArgs(args);
  if (value === undefined) {
    throw new Error(`Argument ${varName} was not defined but must be a boolean.`);
  }
  if (typeof value !== "boolean") {
    throw new Error(`Argument ${varName} must be a boolean.`);
  }
  if (!customValidator(value)) {
    throw new Error(`Argument ${varName} failed custom validation.`);
  }
  return value;
}
function is_bool(...args) { return is_boolean(...args) ;}

/**
 * Validates that a value is a function.
 * - If `value` is a string, we interpret it as the name of a function on `globalThis`,
 *   and confirm that `globalThis[value]` is indeed a function.
 * - Otherwise, we confirm that `typeof value === 'function'`.
 *
 * @example
 *    // 1) Checking a string reference:
 *    is_function("myVarName", "myGlobalFunction");
 *
 *    // 2) Checking a direct function reference:
 *    is_function("myFuncVar", someFunctionReference);
 *
 * @throws {Error} If the string does not resolve to a function, or if the value is neither a string nor a function.
 * @returns {Function} The resolved function (if a string was provided, returns the globalThis lookup).
 */
function is_function(...args) {
  const { varName, value, customValidator } = parseValidatorArgs(args);

  let resolvedFn;

  if (typeof value === 'string') {
    // Look up on globalThis
    resolvedFn = globalThis[value];
    if (typeof resolvedFn !== 'function') {
      throw new Error(
        `Argument ${varName} references a globalThis property (${value}) that is not a function.`
      );
    }
  } else if (typeof value === 'function') {
    // Already a function object
    resolvedFn = value;
  } else {
    throw new Error(
      `Argument ${varName} must be either a string referencing a global function or a function object.`
    );
  }

  if (!customValidator(resolvedFn)) {
    throw new Error(`Argument ${varName} failed custom validation.`);
  }

  // Return the function in case you want to use it directly
  return resolvedFn;
}

////////////////////////////////
//
// TEST FUNCTIONS
//
////////////////////////////////
/**
 * Example function that sends an email notification via Gmail.
 * 
 * @param {string} email - Recipient's email address.
 * @param {string} message - The body of the email message.
 */
function sendNotification(email, message) {
  console.log(`sendNotification() called with email=${email}, message=${message}`);

  if (typeof email !== 'string') {
    throw new Error(
      `sendNotification() first argument must be a string (email). Received: ${JSON.stringify(email)}`
    );
  }
  if (typeof message !== 'string') {
    throw new Error(
      `sendNotification() second argument must be a string (message). Received: ${JSON.stringify(message)}`
    );
  }

  GmailApp.sendEmail(email, 'Notification', message);
}

/**
 * Example function to show how to schedule a job that makes an HTTP call (dummy example).
 */
function testProcessUrl() {
  console.log('testProcessUrl() called');
  const scheduler = new JobScheduler();
  scheduler.create('PROCESS_RECEIPTS2', 'https://example.com').schedule();
}

/**
 * Example function to show how to pick up results from a named function.
 */
function testPickupMethod() {
  console.log('testPickupMethod() called');
  const scheduler = new JobScheduler();
  const functionName = 'PROCESS_RECEIPTS2';
  try {
    const [result, metaData] = scheduler.pickup(functionName);
    Logger.log(`Picked up: ${JSON.stringify(result)}, metaData: ${JSON.stringify(metaData)}`);
    return [result, metaData];
  } catch (e) {
    Logger.log(`Exception: ${e.message}`);
  }
}

/**
 * Example function to show how to schedule a job with multiple steps and options.
 */
function testScheduleExample() {
  console.log('testScheduleExample() called');
  const scheduler = new JobScheduler({ maxTriggers: 10 });
  scheduler
    .create('sendNotification', 'someone@example.com', 'Hello World')
    .thenAfter('Utilities.sleep', 10)
    .withOptions({ maxRetries: 5, tags: ['urgent'] })
    .schedule();
}

function returnArgs( arg ) { return arg ; }

function testChainedFunctions()
{
  const scheduler = new JobScheduler();

  scheduler.create("returnArgs", "Test12")
    .thenAfter("sendNotification", "jim@fortifiedstrength.org", "__PREV__")
    .schedule() ;
}
