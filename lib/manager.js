'use strict';

var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }();

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

function _possibleConstructorReturn(self, call) { if (!self) { throw new ReferenceError("this hasn't been initialised - super() hasn't been called"); } return call && (typeof call === "object" || typeof call === "function") ? call : self; }

function _inherits(subClass, superClass) { if (typeof superClass !== "function" && superClass !== null) { throw new TypeError("Super expression must either be null or a function, not " + typeof superClass); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, enumerable: false, writable: true, configurable: true } }); if (superClass) Object.setPrototypeOf ? Object.setPrototypeOf(subClass, superClass) : subClass.__proto__ = superClass; }

var assert = require('assert');
var EventEmitter = require('events');

var Worker = require('./worker');
var plans = require('./plans');
var Attorney = require('./attorney');

var stateJobDelimiter = plans.stateJobDelimiter;
var expiredJobSuffix = plans.expiredJobSuffix;
var completedJobSuffix = plans.completedJobSuffix;
var failedJobSuffix = plans.failedJobSuffix;

var events = {
  job: 'job',
  failed: 'failed',
  error: 'error'
};

var Manager = function (_EventEmitter) {
  _inherits(Manager, _EventEmitter);

  function Manager(db, config) {
    _classCallCheck(this, Manager);

    var _this = _possibleConstructorReturn(this, (Manager.__proto__ || Object.getPrototypeOf(Manager)).call(this));

    _this.config = config;
    _this.db = db;

    _this.events = events;
    _this.subscriptions = {};

    _this.nextJobCommand = plans.fetchNextJob(config.schema);
    _this.insertJobCommand = plans.insertJob(config.schema);
    _this.completeJobCommand = plans.completeJob(config.schema);
    _this.completeJobsCommand = plans.completeJobs(config.schema);
    _this.cancelJobCommand = plans.cancelJob(config.schema);
    _this.cancelJobsCommand = plans.cancelJobs(config.schema);
    _this.failJobCommand = plans.failJob(config.schema);
    _this.failJobsCommand = plans.failJobs(config.schema);

    // exported api to index
    _this.functions = [_this.fetch, _this.complete, _this.cancel, _this.fail, _this.publish, _this.subscribe, _this.unsubscribe, _this.onComplete, _this.offComplete, _this.onExpire, _this.offExpire, _this.onFail, _this.offFail, _this.fetchFailed, _this.fetchExpired, _this.fetchCompleted];
    return _this;
  }

  _createClass(Manager, [{
    key: 'stop',
    value: function stop() {
      var _this2 = this;

      Object.keys(this.subscriptions).forEach(function (name) {
        return _this2.unsubscribe(name);
      });
      this.subscriptions = {};
      return Promise.resolve(true);
    }
  }, {
    key: 'subscribe',
    value: function subscribe(name) {
      var _this3 = this;

      for (var _len = arguments.length, args = Array(_len > 1 ? _len - 1 : 0), _key = 1; _key < _len; _key++) {
        args[_key - 1] = arguments[_key];
      }

      return Attorney.checkSubscribeArgs(name, args).then(function (_ref) {
        var options = _ref.options,
            callback = _ref.callback;
        return _this3.watch(name, options, callback);
      });
    }
  }, {
    key: 'onExpire',
    value: function onExpire(name) {
      var _this4 = this;

      for (var _len2 = arguments.length, args = Array(_len2 > 1 ? _len2 - 1 : 0), _key2 = 1; _key2 < _len2; _key2++) {
        args[_key2 - 1] = arguments[_key2];
      }

      // unwrapping job in callback here because we love our customers
      return Attorney.checkSubscribeArgs(name, args).then(function (_ref2) {
        var options = _ref2.options,
            callback = _ref2.callback;
        return _this4.watch(name + expiredJobSuffix, options, function (job) {
          return callback(job.data);
        });
      });
    }
  }, {
    key: 'onComplete',
    value: function onComplete(name) {
      var _this5 = this;

      for (var _len3 = arguments.length, args = Array(_len3 > 1 ? _len3 - 1 : 0), _key3 = 1; _key3 < _len3; _key3++) {
        args[_key3 - 1] = arguments[_key3];
      }

      return Attorney.checkSubscribeArgs(name, args).then(function (_ref3) {
        var options = _ref3.options,
            callback = _ref3.callback;
        return _this5.watch(name + completedJobSuffix, options, callback);
      });
    }
  }, {
    key: 'onFail',
    value: function onFail(name) {
      var _this6 = this;

      for (var _len4 = arguments.length, args = Array(_len4 > 1 ? _len4 - 1 : 0), _key4 = 1; _key4 < _len4; _key4++) {
        args[_key4 - 1] = arguments[_key4];
      }

      return Attorney.checkSubscribeArgs(name, args).then(function (_ref4) {
        var options = _ref4.options,
            callback = _ref4.callback;
        return _this6.watch(name + failedJobSuffix, options, callback);
      });
    }
  }, {
    key: 'watch',
    value: function watch(name, options, callback) {
      var _this7 = this;

      assert(!(name in this.subscriptions), 'this job has already been subscribed on this instance.');

      options.batchSize = options.batchSize || options.teamSize;

      if ('newJobCheckInterval' in options || 'newJobCheckIntervalSeconds' in options) options = Attorney.applyNewJobCheckInterval(options);else options.newJobCheckInterval = this.config.newJobCheckInterval;

      var subscription = this.subscriptions[name] = { worker: null };

      var onError = function onError(error) {
        return _this7.emit(events.error, error);
      };

      var complete = function complete(job, error, response) {
        if (!error) return _this7.complete(job.id, response);

        return _this7.fail(job.id, error).then(function () {
          return _this7.emit(events.failed, { job: job, error: error });
        });
      };

      var respond = function respond(jobs) {
        if (!jobs) return;

        if (!Array.isArray(jobs)) jobs = [jobs];

        setImmediate(function () {
          jobs.forEach(function (job) {
            _this7.emit(events.job, job);
            job.done = function (error, response) {
              return complete(job, error, response);
            };

            try {
              callback(job, job.done);
            } catch (error) {
              _this7.emit(events.failed, { job: job, error: error });
            }
          });
        });
      };

      var fetch = function fetch() {
        return _this7.fetch(name, options.batchSize);
      };

      var workerConfig = {
        name: name,
        fetch: fetch,
        respond: respond,
        onError: onError,
        interval: options.newJobCheckInterval
      };

      var worker = new Worker(workerConfig);
      worker.start();
      subscription.worker = worker;

      return Promise.resolve(true);
    }
  }, {
    key: 'unsubscribe',
    value: function unsubscribe(name) {
      if (!this.subscriptions[name]) return Promise.reject('No subscriptions for ' + name + ' were found.');

      this.subscriptions[name].worker.stop();
      delete this.subscriptions[name];

      return Promise.resolve(true);
    }
  }, {
    key: 'offFail',
    value: function offFail(name) {
      return this.unsubscribe(name + failedJobSuffix);
    }
  }, {
    key: 'offExpire',
    value: function offExpire(name) {
      return this.unsubscribe(name + expiredJobSuffix);
    }
  }, {
    key: 'offComplete',
    value: function offComplete(name) {
      return this.unsubscribe(name + completedJobSuffix);
    }
  }, {
    key: 'publish',
    value: function publish() {
      var _this8 = this;

      for (var _len5 = arguments.length, args = Array(_len5), _key5 = 0; _key5 < _len5; _key5++) {
        args[_key5] = arguments[_key5];
      }

      return Attorney.checkPublishArgs(args).then(function (_ref5) {
        var name = _ref5.name,
            data = _ref5.data,
            options = _ref5.options;
        return _this8.createJob(name, data, options);
      });
    }
  }, {
    key: 'expired',
    value: function expired(job) {
      return this.publish(job.name + expiredJobSuffix, job);
    }
  }, {
    key: 'createJob',
    value: function createJob(name, data, options, singletonOffset) {
      var _this9 = this;

      var startIn = options.startIn > 0 ? '' + options.startIn : typeof options.startIn === 'string' ? options.startIn : '0';

      var singletonSeconds = options.singletonSeconds > 0 ? options.singletonSeconds : options.singletonMinutes > 0 ? options.singletonMinutes * 60 : options.singletonHours > 0 ? options.singletonHours * 60 * 60 : options.singletonDays > 0 ? options.singletonDays * 60 * 60 * 24 : null;

      var id = require('uuid/' + this.config.uuid)(),
          retryLimit = options.retryLimit || 0,
          expireIn = options.expireIn || '15 minutes',
          priority = options.priority || 0;

      var singletonKey = options.singletonKey || null;

      singletonOffset = singletonOffset || 0;

      var values = [id, name, priority, retryLimit, startIn, expireIn, data, singletonKey, singletonSeconds, singletonOffset];

      return this.db.executeSql(this.insertJobCommand, values).then(function (result) {
        if (result.rowCount === 1) return id;

        if (!options.singletonNextSlot) return null;

        // delay starting by the offset to honor throttling config
        options.startIn = singletonSeconds;
        // toggle off next slot config for round 2
        options.singletonNextSlot = false;

        var singletonOffset = singletonSeconds;

        return _this9.createJob(name, data, options, singletonOffset);
      });
    }
  }, {
    key: 'fetch',
    value: function fetch(name, batchSize) {
      var _this10 = this;

      return Attorney.checkFetchArgs(name, batchSize).then(function () {
        return _this10.db.executeSql(_this10.nextJobCommand, [name, batchSize || 1]);
      }).then(function (result) {
        return result.rows.length === 0 ? null : result.rows.length === 1 && !batchSize ? result.rows[0] : result.rows;
      });
    }
  }, {
    key: 'fetchFailed',
    value: function fetchFailed(name, batchSize) {
      return this.fetch(name + failedJobSuffix, batchSize);
    }
  }, {
    key: 'fetchExpired',
    value: function fetchExpired(name, batchSize) {
      var _this11 = this;

      return this.fetch(name + expiredJobSuffix, batchSize).then(function (result) {
        return Array.isArray(result) ? result.map(_this11.unwrapStateJob) : _this11.unwrapStateJob(result);
      });
    }
  }, {
    key: 'fetchCompleted',
    value: function fetchCompleted(name, batchSize) {
      return this.fetch(name + completedJobSuffix, batchSize);
    }
  }, {
    key: 'unwrapStateJob',
    value: function unwrapStateJob(job) {
      return job.data;
    }
  }, {
    key: 'setStateForJob',
    value: function setStateForJob(id, data, actionName, command, stateSuffix, bypassNotify) {
      var _this12 = this;

      var job = void 0;

      return this.db.executeSql(command, [id]).then(function (result) {
        assert(result.rowCount === 1, actionName + '(): Job ' + id + ' could not be updated.');

        job = result.rows[0];

        if (!bypassNotify) bypassNotify = job.name.indexOf(stateJobDelimiter) >= 0;

        return bypassNotify ? null : _this12.publish(job.name + stateSuffix, { request: job, response: data || null });
      }).then(function () {
        return job;
      });
    }
  }, {
    key: 'setStateForJobs',
    value: function setStateForJobs(ids, actionName, command) {
      return this.db.executeSql(command, [ids]).then(function (result) {
        assert(result.rowCount === ids.length, actionName + '(): ' + ids.length + ' jobs submitted, ' + result.rowCount + ' updated');
      });
    }
  }, {
    key: 'setState',
    value: function setState(config) {
      var _this13 = this;

      var id = config.id,
          data = config.data,
          actionName = config.actionName,
          command = config.command,
          batchCommand = config.batchCommand,
          stateSuffix = config.stateSuffix,
          bypassNotify = config.bypassNotify;


      return Attorney.assertAsync(id, actionName + '() requires id argument').then(function () {
        var ids = Array.isArray(id) ? id : [id];

        assert(ids.length, actionName + '() requires at least 1 item in an array argument');

        return ids.length === 1 ? _this13.setStateForJob(ids[0], data, actionName, command, stateSuffix, bypassNotify) : _this13.setStateForJobs(ids, actionName, batchCommand);
      });
    }
  }, {
    key: 'complete',
    value: function complete(id, data) {
      var config = {
        id: id,
        data: data,
        actionName: 'complete',
        command: this.completeJobCommand,
        batchCommand: this.completeJobsCommand,
        stateSuffix: completedJobSuffix
      };

      return this.setState(config);
    }
  }, {
    key: 'fail',
    value: function fail(id, data) {
      var config = {
        id: id,
        data: data,
        actionName: 'fail',
        command: this.failJobCommand,
        batchCommand: this.failJobsCommand,
        stateSuffix: failedJobSuffix
      };

      return this.setState(config);
    }
  }, {
    key: 'cancel',
    value: function cancel(id) {
      var config = {
        id: id,
        actionName: 'cancel',
        command: this.cancelJobCommand,
        batchCommand: this.cancelJobsCommand,
        bypassNotify: true
      };

      return this.setState(config);
    }
  }]);

  return Manager;
}(EventEmitter);

module.exports = Manager;