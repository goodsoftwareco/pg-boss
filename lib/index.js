'use strict';

var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }();

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

function _possibleConstructorReturn(self, call) { if (!self) { throw new ReferenceError("this hasn't been initialised - super() hasn't been called"); } return call && (typeof call === "object" || typeof call === "function") ? call : self; }

function _inherits(subClass, superClass) { if (typeof superClass !== "function" && superClass !== null) { throw new TypeError("Super expression must either be null or a function, not " + typeof superClass); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, enumerable: false, writable: true, configurable: true } }); if (superClass) Object.setPrototypeOf ? Object.setPrototypeOf(subClass, superClass) : subClass.__proto__ = superClass; }

var EventEmitter = require('events');
var Attorney = require('./attorney');
var Contractor = require('./contractor');
var Manager = require('./manager');
var Boss = require('./boss');
var Db = require('./db');

var notReadyErrorMessage = 'boss ain\'t ready.  Use start() or connect() to get started.';
var startInProgressErrorMessage = 'boss is starting up. Please wait for the previous start() to finish.';
var notStartedErrorMessage = 'boss ain\'t started.  Use start().';

var PgBoss = function (_EventEmitter) {
  _inherits(PgBoss, _EventEmitter);

  _createClass(PgBoss, null, [{
    key: 'getConstructionPlans',
    value: function getConstructionPlans(schema) {
      return Contractor.constructionPlans(schema);
    }
  }, {
    key: 'getMigrationPlans',
    value: function getMigrationPlans(schema, version, uninstall) {
      return Contractor.migrationPlans(schema, version, uninstall);
    }
  }]);

  function PgBoss(config) {
    _classCallCheck(this, PgBoss);

    config = Attorney.applyConfig(config);

    var _this = _possibleConstructorReturn(this, (PgBoss.__proto__ || Object.getPrototypeOf(PgBoss)).call(this));

    var db = getDb(config);

    if (db.isOurs) promoteEvent.call(_this, db, 'error');

    var manager = new Manager(db, config);
    Object.keys(manager.events).forEach(function (event) {
      return promoteEvent.call(_this, manager, manager.events[event]);
    });

    manager.functions.forEach(function (func) {
      return promoteFunction.call(_this, manager, func);
    });

    var boss = new Boss(db, config);
    Object.keys(boss.events).forEach(function (event) {
      return promoteEvent.call(_this, boss, boss.events[event]);
    });
    boss.on(boss.events.expiredJob, function (job) {
      return manager.expired(job);
    });

    _this.config = config;
    _this.db = db;
    _this.boss = boss;
    _this.contractor = new Contractor(db, config);
    _this.manager = manager;

    function getDb(config) {
      var db = void 0;

      if (config.db) {
        db = config.db;
      } else {
        db = new Db(config);
        db.isOurs = true;
      }

      return db;
    }

    function promoteFunction(obj, func) {
      var _this2 = this;

      this[func.name] = function () {
        for (var _len = arguments.length, args = Array(_len), _key = 0; _key < _len; _key++) {
          args[_key] = arguments[_key];
        }

        if (!_this2.isReady) return Promise.reject(notReadyErrorMessage);
        return func.apply(obj, args);
      };
    }

    function promoteEvent(emitter, event) {
      var _this3 = this;

      emitter.on(event, function (arg) {
        return _this3.emit(event, arg);
      });
    }

    return _this;
  }

  _createClass(PgBoss, [{
    key: 'init',
    value: function init() {
      var _this4 = this;

      if (this.isReady) return Promise.resolve(this);

      return this.boss.supervise().then(function () {
        _this4.isReady = true;
        _this4.isStarted = true;
        return _this4;
      });
    }
  }, {
    key: 'start',
    value: function start() {
      var _this5 = this;

      if (this.isStarting) return Promise.reject(startInProgressErrorMessage);

      this.isStarting = true;

      for (var _len2 = arguments.length, args = Array(_len2), _key2 = 0; _key2 < _len2; _key2++) {
        args[_key2] = arguments[_key2];
      }

      var check = this.isStarted ? Promise.resolve(true) : this.contractor.start.apply(this.contractor, args);

      return check.then(function () {
        _this5.isStarting = false;
        return _this5.init();
      });
    }
  }, {
    key: 'stop',
    value: function stop() {
      var _this6 = this;

      if (!this.isStarted) return Promise.reject(notStartedErrorMessage);

      return Promise.all([this.manager.stop(), this.boss.stop()]).then(function () {
        return _this6.db.isOurs ? _this6.db.close() : null;
      }).then(function () {
        _this6.isReady = false;
        _this6.isStarted = false;
      });
    }
  }, {
    key: 'connect',
    value: function connect() {
      var _this7 = this;

      for (var _len3 = arguments.length, args = Array(_len3), _key3 = 0; _key3 < _len3; _key3++) {
        args[_key3] = arguments[_key3];
      }

      return this.contractor.connect.apply(this.contractor, args).then(function () {
        _this7.isReady = true;
        return _this7;
      });
    }
  }, {
    key: 'disconnect',
    value: function disconnect() {
      var _this8 = this;

      if (!this.isReady) return Promise.reject(notReadyErrorMessage);

      for (var _len4 = arguments.length, args = Array(_len4), _key4 = 0; _key4 < _len4; _key4++) {
        args[_key4] = arguments[_key4];
      }

      return this.manager.stop.apply(this.manager, args).then(function () {
        return _this8.db.isOurs ? _this8.db.close() : null;
      }).then(function () {
        return _this8.isReady = false;
      });
    }
  }]);

  return PgBoss;
}(EventEmitter);

module.exports = PgBoss;