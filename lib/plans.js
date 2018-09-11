'use strict';

var states = {
  created: 'created',
  retry: 'retry',
  active: 'active',
  complete: 'complete',
  expired: 'expired',
  cancelled: 'cancelled',
  failed: 'failed'
};

var stateJobDelimiter = '__state__';
var expiredJobSuffix = stateJobDelimiter + states.expired;
var completedJobSuffix = stateJobDelimiter + states.complete;
var failedJobSuffix = stateJobDelimiter + states.failed;

module.exports = {
  create: create,
  insertVersion: insertVersion,
  getVersion: getVersion,
  versionTableExists: versionTableExists,
  fetchNextJob: fetchNextJob,
  completeJob: completeJob,
  completeJobs: completeJobs,
  cancelJob: cancelJob,
  cancelJobs: cancelJobs,
  failJob: failJob,
  failJobs: failJobs,
  insertJob: insertJob,
  expire: expire,
  archive: archive,
  purge: purge,
  retryFailed: retryFailed,
  countStates: countStates,
  states: states,
  stateJobDelimiter: stateJobDelimiter,
  expiredJobSuffix: expiredJobSuffix,
  completedJobSuffix: completedJobSuffix,
  failedJobSuffix: failedJobSuffix
};

function create(schema) {
  return [createSchema(schema), createVersionTable(schema), createJobStateEnum(schema), createJobTable(schema), cloneJobTableForArchive(schema), addArchivedOnToArchive(schema), createIndexJobFetch(schema), createIndexSingletonOn(schema), createIndexSingletonKeyOn(schema), createIndexSingletonKey(schema)];
}

function createSchema(schema) {
  return '\n    CREATE SCHEMA IF NOT EXISTS ' + schema + '\n  ';
}

function createVersionTable(schema) {
  return '\n    CREATE TABLE IF NOT EXISTS ' + schema + '.version (\n      version text primary key\n    )\n  ';
}

function createJobStateEnum(schema) {
  // ENUM definition order is important
  // base type is numeric and first values are less than last values
  return '\n    CREATE TYPE ' + schema + '.job_state AS ENUM (\n      \'' + states.created + '\',\n      \'' + states.retry + '\',\n      \'' + states.active + '\',\t\n      \'' + states.complete + '\',\n      \'' + states.expired + '\',\n      \'' + states.cancelled + '\',\n      \'' + states.failed + '\'\n    )\n  ';
}

function createJobTable(schema) {
  return '\n    CREATE TABLE IF NOT EXISTS ' + schema + '.job (\n      id uuid primary key not null,\n      name text not null,\n      priority integer not null default(0),\n      data jsonb,\n      state ' + schema + '.job_state not null default(\'' + states.created + '\'),\n      retryLimit integer not null default(0),\n      retryCount integer not null default(0),\n      startIn interval not null default(interval \'0\'),\n      startedOn timestamp with time zone,\n      singletonKey text,\n      singletonOn timestamp without time zone,\n      expireIn interval,\n      createdOn timestamp with time zone not null default now(),\n      completedOn timestamp with time zone\n    )\n  ';
}

function cloneJobTableForArchive(schema) {
  return 'CREATE TABLE IF NOT EXISTS ' + schema + '.archive (LIKE ' + schema + '.job)';
}

function addArchivedOnToArchive(schema) {
  return 'ALTER TABLE ' + schema + '.archive ADD archivedOn timestamptz NOT NULL DEFAULT now()';
}

function createIndexSingletonKey(schema) {
  // anything with singletonKey means "only 1 job can be queued or active at a time"
  return '\n    CREATE UNIQUE INDEX job_singletonKey ON ' + schema + '.job (name, singletonKey) WHERE state < \'' + states.complete + '\' AND singletonOn IS NULL\n  ';
}

function createIndexSingletonOn(schema) {
  // anything with singletonOn means "only 1 job within this time period, queued, active or completed"
  return '\n    CREATE UNIQUE INDEX job_singletonOn ON ' + schema + '.job (name, singletonOn) WHERE state < \'' + states.expired + '\' AND singletonKey IS NULL\n  ';
}

function createIndexSingletonKeyOn(schema) {
  // anything with both singletonOn and singletonKey means "only 1 job within this time period with this key, queued, active or completed"
  return '\n    CREATE UNIQUE INDEX job_singletonKeyOn ON ' + schema + '.job (name, singletonOn, singletonKey) WHERE state < \'' + states.expired + '\'\n  ';
}

function createIndexJobFetch(schema) {
  return '\n    CREATE INDEX job_fetch ON ' + schema + '.job (priority desc, createdOn, id) WHERE state < \'' + states.active + '\'\n  ';
}

function getVersion(schema) {
  return '\n    SELECT version from ' + schema + '.version\n  ';
}

function versionTableExists(schema) {
  return '\n    SELECT to_regclass(\'' + schema + '.version\') as name\n  ';
}

function insertVersion(schema) {
  return '\n    INSERT INTO ' + schema + '.version(version) VALUES ($1)\n  ';
}

function fetchNextJob(schema) {
  return '\n    WITH nextJob as (\n      SELECT id\n      FROM ' + schema + '.job\n      WHERE state < \'' + states.active + '\'\n        AND name = $1\n        AND (createdOn + startIn) < now()\n      ORDER BY priority desc, createdOn, id\n      LIMIT $2\n      FOR UPDATE SKIP LOCKED\n    )\n    UPDATE ' + schema + '.job SET\n      state = \'' + states.active + '\',\n      startedOn = now(),\n      retryCount = CASE WHEN state = \'' + states.retry + '\' THEN retryCount + 1 ELSE retryCount END\n    FROM nextJob\n    WHERE ' + schema + '.job.id = nextJob.id\n    RETURNING ' + schema + '.job.id, $1 as name, ' + schema + '.job.data\n  ';
}

function completeJob(schema) {
  return '\n    UPDATE ' + schema + '.job\n    SET completedOn = now(),\n      state = \'' + states.complete + '\'\n    WHERE id = $1\n      AND state = \'' + states.active + '\'\n    RETURNING id, name, data\n  ';
}

function completeJobs(schema) {
  return '\n    UPDATE ' + schema + '.job\n    SET completedOn = now(),\n      state = \'' + states.complete + '\'\n    WHERE id = ANY($1)\n      AND state = \'' + states.active + '\'\n  ';
}

function cancelJob(schema) {
  return '\n    UPDATE ' + schema + '.job\n    SET completedOn = now(),\n      state = \'' + states.cancelled + '\'\n    WHERE id = $1\n      AND state < \'' + states.complete + '\'\n    RETURNING id, name, data\n  ';
}

function cancelJobs(schema) {
  return '\n    UPDATE ' + schema + '.job\n    SET completedOn = now(),\n      state = \'' + states.cancelled + '\'\n    WHERE id = ANY($1)\n      AND state < \'' + states.complete + '\'\n  ';
}

function failJob(schema) {
  return '\n    UPDATE ' + schema + '.job\n    SET completedOn = now(),\n      state = \'' + states.failed + '\'\n    WHERE id = $1\n      AND state < \'' + states.complete + '\'\n    RETURNING id, name, data\n  ';
}

function failJobs(schema) {
  return '\n    UPDATE ' + schema + '.job\n    SET completedOn = now(),\n      state = \'' + states.failed + '\'\n    WHERE id = ANY($1)\n      AND state < \'' + states.complete + '\'\n  ';
}

function insertJob(schema) {
  return '\n    INSERT INTO ' + schema + '.job (id, name, priority, state, retryLimit, startIn, expireIn, data, singletonKey, singletonOn)\n    VALUES (\n      $1, $2, $3, \'' + states.created + '\', $4, CAST($5 as interval), CAST($6 as interval), $7, $8,\n      CASE WHEN $9::integer IS NOT NULL THEN \'epoch\'::timestamp + \'1 second\'::interval * ($9 * floor((date_part(\'epoch\', now()) + $10) / $9)) ELSE NULL END\n    )\n    ON CONFLICT DO NOTHING\n  ';
}

function expire(schema) {
  return '\n    WITH expired AS (\n      UPDATE ' + schema + '.job\n      SET state = CASE WHEN retryCount < retryLimit THEN \'' + states.retry + '\'::' + schema + '.job_state ELSE \'' + states.expired + '\'::' + schema + '.job_state END,        \n        completedOn = CASE WHEN retryCount < retryLimit THEN NULL ELSE now() END\n      WHERE state = \'' + states.active + '\'\n        AND (startedOn + expireIn) < now()    \n      RETURNING id, name, state, data\n    )\n    SELECT id, name, data FROM expired WHERE state = \'' + states.expired + '\'\n  ';
}

function purge(schema) {
  return '\n    DELETE FROM ' + schema + '.archive\n    WHERE (archivedOn < now() - CAST($1 as interval))\n  ';
}

function archive(schema) {
  return '\n    WITH archived_rows AS (\n      DELETE FROM ' + schema + '.job\n      WHERE\n        (completedOn + CAST($1 as interval) < now())\n        OR (\n          state = \'' + states.created + '\'\n          AND name LIKE \'%' + stateJobDelimiter + '%\'\n          AND createdOn + CAST($1 as interval) < now()\n        )\n      RETURNING *\n    )\n    INSERT INTO ' + schema + '.archive\n    SELECT * FROM archived_rows\n  ';
}

function retryFailed(schema) {
  return '\n    UPDATE ' + schema + '.job\n    SET state = \'' + states.retry + '\'::' + schema + '.job_state,\n      completedOn = NULL\n    WHERE state = \'' + states.failed + '\'\n      AND retryCount < retryLimit\n  ';
}

function countStates(schema) {
  return '\n    SELECT name, state, count(*) size\n    FROM ' + schema + '.job\n    WHERE name NOT LIKE \'%' + stateJobDelimiter + '%\'\n    GROUP BY rollup(name), rollup(state)\n  ';
}