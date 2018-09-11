'use strict';

var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }();

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

function _possibleConstructorReturn(self, call) { if (!self) { throw new ReferenceError("this hasn't been initialised - super() hasn't been called"); } return call && (typeof call === "object" || typeof call === "function") ? call : self; }

function _inherits(subClass, superClass) { if (typeof superClass !== "function" && superClass !== null) { throw new TypeError("Super expression must either be null or a function, not " + typeof superClass); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, enumerable: false, writable: true, configurable: true } }); if (superClass) Object.setPrototypeOf ? Object.setPrototypeOf(subClass, superClass) : subClass.__proto__ = superClass; }

var EventEmitter = require('events');
var pg = require('pg');
var url = require('url');

var Db = function (_EventEmitter) {
  _inherits(Db, _EventEmitter);

  function Db(config) {
    _classCallCheck(this, Db);

    var _this = _possibleConstructorReturn(this, (Db.__proto__ || Object.getPrototypeOf(Db)).call(this));

    _this.config = config;

    var poolConfig = config.connectionString ? parseConnectionString(config.connectionString) : config;

    _this.pool = new pg.Pool({
      user: poolConfig.user,
      password: poolConfig.password,
      host: poolConfig.host,
      port: poolConfig.port,
      database: poolConfig.database,
      application_name: poolConfig.application_name || 'pgboss',
      max: poolConfig.poolSize || poolConfig.max,
      ssl: !!poolConfig.ssl,
      Promise: Promise
    });

    _this.pool.on('error', function (error) {
      return _this.emit('error', error);
    });

    function parseConnectionString(connectionString) {
      var parseQuerystring = true;
      var params = url.parse(connectionString, parseQuerystring);
      var auth = params.auth.split(':');

      var parsed = {
        user: auth[0],
        host: params.hostname,
        port: params.port,
        database: params.pathname.split('/')[1],
        ssl: !!params.query.ssl
      };

      if (auth.length === 2) parsed.password = auth[1];

      return parsed;
    }

    return _this;
  }

  _createClass(Db, [{
    key: 'close',
    value: function close() {
      return !this.pool.ending ? this.pool.end() : Promise.resolve(true);
    }
  }, {
    key: 'executeSql',
    value: function executeSql(text, values) {
      return this.pool.query(text, values);
    }
  }]);

  return Db;
}(EventEmitter);

module.exports = Db;