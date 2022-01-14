/**
 * sqlclient.js
 *
 * Объект вызывается из другого дочернего процесса для чтения из БД
 *   (данные для отчетов)
 *  - Выполняет подключение к БД
 *  - Формирует sql запрос (при необходимости)
 *  - Выполняет запрос, возвращает массив данных
 */

const util = require('util');

const sqlite3 = require('sqlite3');

const utils = require('./utils');

// Входной параметр
//   dbPath: <полный путь к БД, включая имя файла>
class Sqlclient {
  constructor(opt) {
    this.opt = opt;
    this.pool = null;
  }

  connect() {
    const dbPath = this.opt.dbPath;
    return new Promise((resolve, reject) => {
      this.pool = new sqlite3.Database(dbPath, err => {
        if (err) {
          reject(err);
        } else {
          resolve();
        }
      });
    });
  }
  
  prepareQuery(queryObj) {
    let queryStr;
    if (typeof queryObj == 'string') {
      queryStr = queryObj;
    } else if (queryObj.sql) {
      queryStr = queryObj.sql;
    } else {
      if (!queryObj.dn_prop) return ''; // Нет запроса - просто пустая строка

      const dnarr = queryObj.dn_prop.split(',');
      queryStr = utils.getQueryStr(queryObj, dnarr);
    }

    console.log('SQLClient queryStr='+queryStr)
    return queryStr;
  }


  query(queryStr) {
   
    if (!queryStr) return Promise.reject('Empty queryStr! ');
    if (typeof queryStr != 'string') return Promise.reject('Expected query as SQL string! ');

    return new Promise((resolve, reject) => {
      this.pool.all(queryStr, (err, records) => {
        if (!err) {
          resolve(records);
        } else reject(err);
      });
    });
  }

  close() {
    if (this.pool) {
      this.pool.close();
      this.pool = null;
    }
  }
}

module.exports = Sqlclient;
