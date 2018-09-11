"use strict";

var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }();

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

var Worker = function () {
  function Worker(config) {
    _classCallCheck(this, Worker);

    this.config = config;
  }

  _createClass(Worker, [{
    key: "start",
    value: function start() {
      var _this = this;

      if (this.stopped) return;

      this.config.fetch().then(this.config.respond).catch(this.config.onError).then(function () {
        return setTimeout(function () {
          return _this.start.apply(_this);
        }, _this.config.interval);
      });
    }
  }, {
    key: "stop",
    value: function stop() {
      this.stopped = true;
    }
  }]);

  return Worker;
}();

module.exports = Worker;