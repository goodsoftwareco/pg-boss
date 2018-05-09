'use strict';

var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }();

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

function _possibleConstructorReturn(self, call) { if (!self) { throw new ReferenceError("this hasn't been initialised - super() hasn't been called"); } return call && (typeof call === "object" || typeof call === "function") ? call : self; }

function _inherits(subClass, superClass) { if (typeof superClass !== "function" && superClass !== null) { throw new TypeError("Super expression must either be null or a function, not " + typeof superClass); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, enumerable: false, writable: true, configurable: true } }); if (superClass) Object.setPrototypeOf ? Object.setPrototypeOf(subClass, superClass) : subClass.__proto__ = superClass; }

var EventEmitter = require('events');
var plans = require('./plans');

var events = {
  archived: 'archived',
  deleted: 'deleted',
  monitorStates: 'monitor-states',
  expiredCount: 'expired-count',
  expiredJob: 'expired-job',
  error: 'error'
};

var Boss = function (_EventEmitter) {
  _inherits(Boss, _EventEmitter);

  function Boss(db, config) {
    _classCallCheck(this, Boss);

    var _this = _possibleConstructorReturn(this, (Boss.__proto__ || Object.getPrototypeOf(Boss)).call(this));

    _this.db = db;
    _this.config = config;

    _this.timers = {};
    _this.events = events;

    _this.expireCommand = plans.expire(config.schema);
    _this.archiveCommand = plans.archive(config.schema);
    _this.purgeCommand = plans.purge(config.schema);
    _this.retryFailedCommand = plans.retryFailed(config.schema);
    _this.countStatesCommand = plans.countStates(config.schema);
    return _this;
  }

  _createClass(Boss, [{
    key: 'supervise',
    value: function supervise() {
      var self = this;

      // todo: add query that calcs avg start time delta vs. creation time

      return Promise.all([monitor(this.expire, this.config.expireCheckInterval), monitor(this.archive, this.config.archiveCheckInterval), monitor(this.purge, this.config.deleteCheckInterval), this.config.failedCheckInterval ? monitor(this.retryFailed, this.config.failedCheckInterval) : null, this.config.monitorStateInterval ? monitor(this.countStates, this.config.monitorStateInterval) : null]);

      function monitor(func, interval) {

        return exec().then(repeat);

        function exec() {
          return func.call(self).catch(function (error) {
            return self.emit(events.error, error);
          });
        }

        function repeat() {
          if (self.stopped) return;
          self.timers[func.name] = setTimeout(function () {
            return exec().then(repeat);
          }, interval);
        }
      }
    }
  }, {
    key: 'stop',
    value: function stop() {
      var _this2 = this;

      this.stopped = true;
      Object.keys(this.timers).forEach(function (key) {
        return clearTimeout(_this2.timers[key]);
      });
      return Promise.resolve();
    }
  }, {
    key: 'countStates',
    value: function countStates() {
      var _this3 = this;

      var stateCountDefault = Object.assign({}, plans.states);

      Object.keys(stateCountDefault).forEach(function (key) {
        return stateCountDefault[key] = 0;
      });

      return this.db.executeSql(this.countStatesCommand).then(function (counts) {

        var states = counts.rows.reduce(function (acc, item) {
          if (item.name) acc.queues[item.name] = acc.queues[item.name] || Object.assign({}, stateCountDefault);

          var queue = item.name ? acc.queues[item.name] : acc;
          var state = item.state || 'all';

          // parsing int64 since pg returns it as string
          queue[state] = parseFloat(item.size);

          return acc;
        }, Object.assign({}, stateCountDefault, { queues: {} }));

        _this3.emit(events.monitorStates, states);

        return states;
      });
    }
  }, {
    key: 'expire',
    value: function expire() {
      var _this4 = this;

      return this.db.executeSql(this.expireCommand).then(function (result) {
        if (result.rows.length) {
          _this4.emit(events.expiredCount, result.rows.length);
          return Promise.all(result.rows.map(function (job) {
            return _this4.emit(events.expiredJob, job);
          }));
        }
      });
    }
  }, {
    key: 'archive',
    value: function archive() {
      var _this5 = this;

      return this.db.executeSql(this.archiveCommand, [this.config.archiveCompletedJobsEvery]).then(function (result) {
        if (result.rowCount) _this5.emit(events.archived, result.rowCount);
      });
    }
  }, {
    key: 'purge',
    value: function purge() {
      var _this6 = this;

      return this.db.executeSql(this.purgeCommand, [this.config.deleteArchivedJobsEvery]).then(function (result) {
        if (result.rowCount) _this6.emit(events.deleted, result.rowCount);
      });
    }
  }, {
    key: 'retryFailed',
    value: function retryFailed() {
      return this.db.executeSql(this.retryFailedCommand);
    }
  }]);

  return Boss;
}(EventEmitter);

module.exports = Boss;