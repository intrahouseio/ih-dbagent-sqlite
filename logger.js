/*
 *  logger.js
 */

const util = require('util');
const fs = require('fs');
const path = require('path');

// const countOfNewest = 2;

module.exports = {
  loglevel: 0,
  currentFileSize: 0,
  stream: null,

  start(logfileName, level, sizeKB) {
    this.logfileName = logfileName;
    this.setLoglevel(level || 0);
    this.fileSize = (Number(sizeKB) > 0 ? Number(sizeKB) : 512) * 1024;

    this.currentFileSize = fs.existsSync(logfileName) ? this.getCurrentFileSize(logfileName) : 0;
    this.stream = createStream(logfileName);

    if (this.stream) {
      this.stream.write('\r\n');
    } else {
      console.log('Error stream creation for ' + logfileName);
      this.stream = null;
    }
  },

  setParams(params) {
    if (!params || typeof params != 'object') return;
    if (params.logsize > 0) this.fileSize = params.logsize *1024;
    // Есть плагины без этих параметров
    if (params.logrotate != undefined && params.logrotate >=0) this.logrotate = params.logrotate;
  },

  getCurrentFileSize(file) {
    let fileSize = 0;
    try {
      fileSize = fs.statSync(file).size;
    } catch (e) {
      console.log('Log file ' + file + ' stat error: ' + util.inspect(e));
    }
    return fileSize;
  },

  // level: 0 - низкий уровень (пишется всегда), 1 -средний уровень, 2 - высокий уровень
  log(msg, level, loglevel) {
    if (!this.stream) return;
    if (level && loglevel < level) return;

    const str = typeof msg == 'object' ? 'ERROR: ' + util.inspect(msg) : msg;
    this.write(getDateStr() + ' ' + str + '\r\n');
  },

  /**
   * Вывод в файл
   * Если размер превышен - записать в новый файл
   * @param {String} str - сообщение для вывода
   */
  write(str) {
    this.currentFileSize += str.length;
    if (this.currentFileSize <= this.fileSize) {
      this.stream.write(str);
    } else {
      this.writeToNewFile(str);
    }
  },

  writeToNewFile(str) {
    this.stream.end();
    this.stream = null;

    let dt = String(Date.now());
    this.currentFileSize = 0;

    fs.rename(this.logfileName, this.logfileName + '.' + dt, err => {
      if (err) {
        console.log('ERROR: Log file ' + this.logfileName + ' rename error!');
      } else {
        this.stream = createStream(this.logfileName);
        this.stream.write(str);

        // Удалить старые логи
        this.removeOldFiles();
      }
    });
  },

  setLoglevel(level) {
    this.loglevel = level;
    this.log('Log level: ' + level);
  },

  async removeOldFiles() {
    let warnMsg;
    const countOfNewest = this.logrotate != undefined ? this.logrotate : 2;
    try {
      const folder = path.dirname(this.logfileName);
      warnMsg = 'WARN: removeOldFiles from ' + folder + ' ERROR! ';

      const arr = await fs.promises.readdir(folder);
      if (!arr || !util.isArray(arr)) {
        this.log(warnMsg);
        return;
      }

      const name = path.basename(this.logfileName); // ih_emuls1.log
      // ih_emuls1.log.1726821367707
      let res = arr.filter(file => file.startsWith(name) && isStringMatch(getFileExt(file), /[0-9]/));
      if (res.length <= 0) return;

      res.sort(); // Упорядочить по времени (ts)

      // Оставить count самых последних файлов, остальные удалить
      if (!res || res.length <= countOfNewest) return;

      for (let i = 0; i < res.length - countOfNewest; i++) {
        fs.unlink(folder + '/' + res[i], err => {
          // Результата не ждем, но в случае ошибки запишем в Лог
          if (err) console.log(warnMsg + util.inspect(err) + ' File: ' + folder + '/' + res[i]);
        });
      }
    } catch (e) {
      console.log(warnMsg + util.inspect(e));
    }
  }
};

function createStream(logfile) {
  return fs.createWriteStream(logfile, { flags: 'a', encoding: 'utf-8', mode: 0o666 });
}

function getDateStr() {
  const dt = new Date();
  return (
    pad(dt.getDate()) +
    '.' +
    pad(dt.getMonth() + 1) +
    ' ' +
    pad(dt.getHours()) +
    ':' +
    pad(dt.getMinutes()) +
    ':' +
    pad(dt.getSeconds()) +
    '.' +
    pad(dt.getMilliseconds(), 3)
  );
}

function pad(str, len = 2) {
  return String(str).padStart(len, '0');
}

function isStringMatch(str, exp) {
  if (!str || typeof str != 'string') return false;
  if (isRegExp(exp)) return str.match(exp);
  if (typeof exp == 'string') return str == exp;
}

function isRegExp(obj) {
  return obj instanceof RegExp;
}

function getFileExt(filename) {
  let parts = filename.split('.');
  return parts.length > 1 ? parts.pop() : '';
}