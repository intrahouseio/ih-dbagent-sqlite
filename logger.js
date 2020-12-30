/*
 *  logger.js
 */

const util = require('util');
const fs = require('fs');

const utils = require('./lib/utils');

module.exports = {
  fd: 0,
  loglevel: 0,

  start(logfileName, level) {
    // this.fd = fs.openSync(logfileName, 'a'); // добавляет
    this.fd = fs.openSync(logfileName, 'w'); // перезаписывает
    this.setLoglevel(level || 0);
  },

  // level: 0 - низкий уровень (пишется всегда), 1 -средний уровень, 2 - высокий уровень
  log(msg, level) {
    if (!this.fd) return;
    if (level && this.loglevel < level) return;

    const str = typeof msg == 'object' ? 'ERROR: ' + utils.getShortErrStr(msg) : msg;
    fs.write(this.fd, utils.getDateStr() + ' ' + str + '\n', err => {
      if (err) console.log('Log error:' + str + util.inspect(err));
    });
  },

  setLoglevel(level) {
    this.loglevel = level;
    this.log('Log level: '+level);
  }

};

