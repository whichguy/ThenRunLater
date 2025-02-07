/**
 * doGet: Serves the UI from 'Index.html'
 */
function doGet() {
  return HtmlService.createHtmlOutputFromFile('index')
    //.evaluate()
    .setTitle('Job Scheduler Dashboard');
}
function getResultsData() {
  const scheduler = new JobScheduler();
  const results = [];

  // "Success_" files in results
  const resFolder = scheduler.getFolder('results');
  const files = resFolder.getFiles();
  while (files.hasNext()) {
    const file = files.next();
    if (!file.getName().startsWith("Success_")) continue;
    const fileId = file.getId();
    // parse minimal metadata
    const content = JSON.parse(file.getBlob().getDataAsString());

    const jobSteps = content?.metadata?.originalJob?.steps || [];
    const functionNames = jobSteps.map(step => step.functionPath);

    results.push({
      fileName: file.getName(),
      fileId: fileId,
      success: true,
      metadata: content.metadata || {},
      functionNames: functionNames,
    });
  }

  // "Failed_" in deadLetters
  const deadFolder = scheduler.getFolder('deadLetters');
  const deadFiles = deadFolder.getFiles();
  while (deadFiles.hasNext()) {
    const f = deadFiles.next();
    if (!f.getName().startsWith("Failed_")) continue;
    const fileId = f.getId();
    const content = JSON.parse(f.getBlob().getDataAsString());
    results.push({
      fileName: f.getName(),
      fileId: fileId,
      success: false,
      metadata: content.metadata || {},
    });
  }

  return results;
}


/**
 * Return the user's role object to the client
 */
function getUserRoleServer() {
  const email = Session.getActiveUser().getEmail();
  return getUserRole(email);
}

function getAllActiveJobs() {
  const scheduler = new JobScheduler({ debug: false });
  return scheduler.getActiveJobsRegistry();
}

function cancelActiveJob(triggerId) {
  const email = Session.getActiveUser().getEmail();
  // Check permission or roles if needed
  const scheduler = new JobScheduler() ;
  return scheduler.requestTriggerCancellation(triggerId) ;
}

/**
 * Return all pending queue items (requires VIEW_QUEUE)
 */
function getQueueData() {
  const email = Session.getActiveUser().getEmail();
  if (!userCanView(email)) {
    throw new Error("You do not have permission to view the queue.");
  }

  const scheduler = new JobScheduler();
  const result = [];
  const jobFiles = scheduler.getFolder('jobs').getFiles();
  while (jobFiles.hasNext()) {
    const file = jobFiles.next();
    try {
      const jobContent = JSON.parse(file.getBlob().getDataAsString());
      const createdDate = jobContent.metadata.created || 'Unknown';
      const tags = jobContent.metadata.tags || [];
      const functionNames = jobContent.steps.map(s => s.functionPath);
      const fileId = file.getId(); 

      result.push({
        fileName: file.getName(),
        fileId: fileId,  
        createdDate,
        functionNames,
        tags
      });
    } catch(e) {
      result.push({
        fileName: file.getName(),
        createdDate: 'Corrupt File',
        fileId: file.getId(),
        functionNames: [],
        tags: [],
        error: e.message
      });
    }
  }
  return result;
}

/**
 * Delete a job by file name (requires DELETE_QUEUE)
 */
function deleteQueueItem(fileName) {
  const email = Session.getActiveUser().getEmail();
  if (!userCanDelete(email)) {
    throw new Error('You do not have permission to delete queue items.');
  }

  const scheduler = new JobScheduler();
  const folder = scheduler.getFolder('jobs');
  const files = folder.getFilesByName(fileName);
  if (!files.hasNext()) {
    throw new Error(`File not found: ${fileName}`);
  }
  const file = files.next();
  file.setTrashed(true);
  return `Deleted ${fileName}`;
}

/**
 * Peek at results (requires VIEW_QUEUE)
 */
function peekResults(functionName, tag) {
  const email = Session.getActiveUser().getEmail();
  if (!userCanView(email)) {
    throw new Error("You do not have permission to peek results.");
  }

  const scheduler = new JobScheduler();
  const [result, metadata] = scheduler.peek(functionName, tag);
  return { result, metadata };
}

/**
 * List triggers (requires VIEW_QUEUE)
 */
function getActiveTriggers() {
  const email = Session.getActiveUser().getEmail();
  if (!userCanView(email)) {
    throw new Error("You do not have permission to view triggers.");
  }

  const triggers = ScriptApp.getProjectTriggers();
  return triggers
    .filter(t => t.getHandlerFunction() === 'processQueue')
    .map(t => ({
      id: t.getUniqueId(),
      functionName: t.getHandlerFunction(),
      type: t.getEventType()
    }));
}

/**
 * List "exposed" global functions (requires SCHEDULE_JOBS)
 * This is purely an example approach. In reality, you might define a known list or filter more carefully.
 */
function listAvailableGlobalFunctions() {
  const email = Session.getActiveUser().getEmail();
  if (!userCanSchedule(email)) {
    throw new Error("You do not have permission to list functions for scheduling.");
  }

  // We'll enumerate the keys of globalThis and return only those which are top-level functions
  const fnNames = [];
  for (let key in globalThis) {
    try {
      if (typeof globalThis[key] === 'function') {
        // skip built-in top-level for brevity, or define your own filtering
        // e.g. skip /^(on[A-Z]|doGet|processQueue|watchdogCleanup|...)/
        fnNames.push(key);
      }
    } catch (e) {
      // ignore
    }
  }
  // Sort them
  fnNames.sort();
  return fnNames;
}

/**
 * @description Invoked by the client to schedule a multi-step job. 
 *              Directly calls JobScheduler.create(), thenAfter(), withOptions().
 * @param {Object[]} steps - Array of objects describing each step.
 * @param {Object} [schedulerOptions={}] - Configuration for JobScheduler constructor.
 * @returns {{ status: string, fileName: string, triggerId: string|null }}
 */
function scheduleJobChain(steps, schedulerOptions) {
  Logger.log('scheduleJobChain() called');
  // Basic validation
  if (!Array.isArray(steps)) {
    throw new Error(`Expected steps array, got: ${JSON.stringify(steps)}`);
  }
  if (schedulerOptions == null) {
    schedulerOptions = {};
  } else if (typeof schedulerOptions !== 'object') {
    throw new Error(`schedulerOptions must be an object. Received: ${JSON.stringify(schedulerOptions)}`);
  }

  const scheduler = new JobScheduler(schedulerOptions);

  let chainBuilder = null; // Will hold the JobBuilder chain
  steps.forEach((step, idx) => {
    if (!step.type) {
      throw new Error(`Step #${idx} missing 'type': ${JSON.stringify(step)}`);
    }
    switch (step.type) {
      case 'create':
        if (!step.functionPath) {
          throw new Error(`'create' step missing functionPath: ${JSON.stringify(step)}`);
        }
        chainBuilder = scheduler.create(step.functionPath, ...(step.args || []));
        break;

      case 'thenAfter':
        if (!chainBuilder) {
          throw new Error(`'thenAfter' step requires a preceding create(). Step #${idx}`);
        }
        if (!step.functionPath) {
          throw new Error(`'thenAfter' step missing functionPath: ${JSON.stringify(step)}`);
        }
        chainBuilder = chainBuilder.thenAfter(step.functionPath, ...(step.args || []));
        break;

      case 'withOptions':
        if (!chainBuilder) {
          throw new Error(`'withOptions' step requires a preceding create(). Step #${idx}`);
        }
        if (!step.options || typeof step.options !== 'object') {
          throw new Error(`'withOptions' step missing options object: ${JSON.stringify(step)}`);
        }
        chainBuilder = chainBuilder.withOptions(step.options);
        break;

      default:
        throw new Error(`Unrecognized step type="${step.type}" at index #${idx}`);
    }
  });

  if (!chainBuilder) {
    throw new Error(`No valid steps found. Steps: ${JSON.stringify(steps)}`);
  }

  // Now schedule the chain
  const [fileName, trigger] = chainBuilder.schedule();
  const triggerId = trigger ? trigger.getUniqueId() : null;

  return {
    status: 'success',
    fileName,
    triggerId
  };
}


function deleteResultItem(fileName) {
  const email = Session.getActiveUser().getEmail();
  if (!userCanDelete(email)) {
    throw new Error('You do not have permission to delete result items.');
  }

  const scheduler = new JobScheduler();
  // We'll check both 'results' and 'deadLetters' folders
  const folders = [scheduler.getFolder('results'), scheduler.getFolder('deadLetters')];
  let found = false;
  for (const folder of folders) {
    const files = folder.getFilesByName(fileName);
    while (files.hasNext()) {
      const file = files.next();
      file.setTrashed(true);
      found = true;
    }
  }
  if (!found) {
    throw new Error(`Result file not found: ${fileName}`);
  }
  return `Deleted result file: ${fileName}`;
}
