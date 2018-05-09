'use strict';

var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }();

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

var assert = require('assert');
var plans = require('./plans');
var migrationStore = require('./migrationStore');
var schemaVersion = require('../version.json').schema;

var Contractor = function () {
  _createClass(Contractor, null, [{
    key: 'constructionPlans',
    value: function constructionPlans(schema) {
      var exportPlans = plans.create(schema);
      exportPlans.push(plans.insertVersion(schema).replace('$1', '\'' + schemaVersion + '\''));

      return exportPlans.join(';\n\n');
    }
  }, {
    key: 'migrationPlans',
    value: function migrationPlans(schema, version, uninstall) {
      var migration = migrationStore.get(schema, version, uninstall);
      assert(migration, 'migration not found from version ' + version + '. schema: ' + schema);
      return migration.commands.join(';\n\n');
    }
  }]);

  function Contractor(db, config) {
    _classCallCheck(this, Contractor);

    this.config = config;
    this.db = db;
    this.migrations = this.config.migrations || migrationStore.getAll(this.config.schema);
  }

  _createClass(Contractor, [{
    key: 'version',
    value: function version() {
      return this.db.executeSql(plans.getVersion(this.config.schema)).then(function (result) {
        return result.rows.length ? result.rows[0].version : null;
      });
    }
  }, {
    key: 'isCurrent',
    value: function isCurrent() {
      return this.version().then(function (version) {
        return version === schemaVersion;
      });
    }
  }, {
    key: 'isInstalled',
    value: function isInstalled() {
      return this.db.executeSql(plans.versionTableExists(this.config.schema)).then(function (result) {
        return result.rows.length ? result.rows[0].name : null;
      });
    }
  }, {
    key: 'ensureCurrent',
    value: function ensureCurrent() {
      var _this = this;

      return this.version().then(function (version) {
        if (schemaVersion !== version) return _this.update(version);
      });
    }
  }, {
    key: 'create',
    value: function create() {
      // use transaction, in case one query fails, it will automatically rollback to avoid inconsistency
      var queryInTransaction = '\n    BEGIN;\n    ' + plans.create(this.config.schema).join(';') + ';\n    ' + plans.insertVersion(this.config.schema).replace('$1', '\'' + schemaVersion + '\'') + ';\n    COMMIT;';
      return this.db.executeSql(queryInTransaction);
    }
  }, {
    key: 'update',
    value: function update(current) {
      var _this2 = this;

      if (current === '0.0.2') current = '0.0.1';

      return this.migrate(current).then(function (version) {
        if (version !== schemaVersion) return _this2.update(version);
      });
    }
  }, {
    key: 'start',
    value: function start() {
      var _this3 = this;

      return this.isInstalled().then(function (installed) {
        return installed ? _this3.ensureCurrent() : _this3.create();
      });
    }
  }, {
    key: 'connect',
    value: function connect() {
      var _this4 = this;

      var connectErrorMessage = 'this version of pg-boss does not appear to be installed in your database. I can create it for you via start().';

      return this.isInstalled().then(function (installed) {
        if (!installed) throw new Error(connectErrorMessage);

        return _this4.isCurrent();
      }).then(function (current) {
        if (!current) throw new Error(connectErrorMessage);
      });
    }
  }, {
    key: 'migrate',
    value: function migrate(version, uninstall) {
      var migration = migrationStore.get(this.config.schema, version, uninstall, this.migrations);

      if (!migration) {
        var errorMessage = 'Migration to version ' + version + ' failed because it could not be found.  Your database may have been upgraded by a newer version of pg-boss';
        return Promise.reject(new Error(errorMessage));
      }
      // use transaction, in case one query fails, it will automatically rollback to avoid inconsistency
      var queryInTransaction = '\n    BEGIN;\n    ' + migration.commands.join(';') + ';\n    COMMIT;';
      return this.db.executeSql(queryInTransaction).then(function () {
        return migration.version;
      });
    }
  }]);

  return Contractor;
}();

module.exports = Contractor;