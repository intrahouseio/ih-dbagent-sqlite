/**
 * logagent.js
 *
 * Точка входа при запуске дочернего процесса для работы с логами
 * Входной параметр - конфигурация как строка JSON: {
 *   dbPath: <полный путь к БД, включая имя файла>
 *   logfile: <полный путь к файлу лога процесса>
 *   loglevel: <уровень логирования>
 * }
 */

const util = require('util');
const path = require('path');
// const schedule = require('node-schedule');
const { promises: fs } = require('fs');

const logger = require('./logger');
const client = require('./lib/client');
const utils = require('./lib/utils');

const tableNames = ['mainlog', 'pluginlog', 'devicelog', 'iseclog'];

let opt;
try {
  opt = JSON.parse(process.argv[2]);
} catch (e) {
  opt = {};
}

const logfile = opt.logfile || path.join(__dirname, 'ih_sqlite3_logagent.log');
const loglevel = opt.loglevel || 0;
const maxlogrecords = opt.maxlogrecords || 100000;
let stmtMainlog = {};
logger.start(logfile, loglevel);
logger.log('Start logagent sqlite3. Options: ' + JSON.stringify(opt));

sendProcessInfo();
setInterval(sendProcessInfo, 10000); // 10 сек

sendSettingsRequest();
setInterval(sendSettingsRequest, 10800000); // 3 часа = 10800 сек

main(process);

async function main(channel) {
  const initErr = client.init();
  if (initErr) processExit(0, initErr); // Модуль sqlite3 не установлен

  try {
    if (!opt.dbPath) throw { message: 'Missing dbPath for logs!' };

    await client.createPoolToDatabase(opt, logger);
    if (!client.pool) throw { message: 'Client creation Failed!' };

    await client.run('PRAGMA journal_mode = WAL;');
    await client.run('PRAGMA synchronous = NORMAL;');
    await client.run('PRAGMA auto_vacuum = FULL;');

    for (const name of tableNames) {
      try {
        await client.createTable(getCreateTableStr(name), name);
        await client.run('CREATE INDEX IF NOT EXISTS ' + name + '_ts ON ' + name + ' (tsid);');
      } catch (e) {
        logger.log('ERROR: ' + util.inspect(e));
      }
    }
    stmtMainlog = client.pool.prepare(
      'INSERT INTO mainlog (tags, did, location, txt, level, ts, tsid, sender) VALUES (?,?,?,?,?,?,?,?)'
    );
    sendDBSize(); // Отправить статистику первый раз
    setInterval(async () => sendDBSize(), 300000); // 300 сек = 5 мин

    checkTsidUnique('mainlog');

    channel.on('message', ({ id, type, query, payload }) => {
      if (type == 'write') return write(id, query, payload);
      if (type == 'read') return read(id, query);
      if (type == 'run') return run(id, query);
      if (type == 'settings') return del(payload);
      // exit agent
      if (type == 'stop') return processExit(0);
    });

    process.on('SIGTERM', () => {
      logger.log('Received SIGTERM');
      processExit(0);
    });

    if (logger.onStop) {
      logger.onStop(() => {
        processExit(0);
      });
    }

    process.on('exit', () => {
      if (client && client.pool) client.pool.close();
    });
  } catch (err) {
    processExit(1, err);
  }

  async function showGroups(name) {
    const result = await client.query(getGroupQuery(name));
    logger.log(name + ' group: ' + util.inspect(result));
  }

  async function checkTsidUnique(tableName) {
    try {
      const sql = 'SELECT Count (tsid) count, tsid from ' + tableName + ' group by tsid having Count (tsid)>1';
      const result = await client.query(sql);

      if (result.length > 0) {
        logger.log('checkTsidUnique result length = ' + result.length);
        for (let rec of result) {
          const selSql = `SELECT rowid, tsid, ts from ${tableName} WHERE tsid = '${rec.tsid}'`;
          logger.log(selSql);
          const resx = await client.query(selSql);
          logger.log('tsid = ' + rec.tsid + ' result=' + util.inspect(resx));
          if (!resx || !resx.length) continue;

          let idx = 1;
          for (let recx of resx) {
            let newTsid = String(recx.ts) + '_' + String(idx).padStart(5, '0');
            idx += 1;
            const upSql = `UPDATE ${tableName} SET tsid='${newTsid}' WHERE rowid = ${recx.rowid}`;
            await client.query(upSql);
            logger.log('Update ' + rec.tsid + 'to ' + newTsid);
          }
        }
      } else {
        logger.log('checkTsidUnique OK');
      }
    } catch (err) {
      logger.log('ERROR: checkTsidUnique ' + util.inspect(err));
    }
  }

  /**
   *
   * @param {String} id - request uuid
   * @param {Objects} queryObj - {table}
   * @param {Array of Objects} payload - [{ dn, prop, ts, val }]
   */
  async function write(id, queryObj, payload) {
    const table = queryObj && queryObj.table ? queryObj.table : 'mainlog';
    let changes = 0;
    try {
      if (table == 'mainlog') {
        if (!payload || !payload.length) return;
        client.pool.serialize(() => {
          client.pool.run('BEGIN');
          for (let i = 0; i < payload.length; i++) {
            let value = payload[i];
            stmtMainlog.run(value.tags, value.did, value.location, value.txt, value.level, value.ts, value.tsid, value.sender);
          }
          client.pool.run('COMMIT');
        });
      } else {
        const columns = getColumns(table);
        const values = utils.formValues(payload, columns);
        if (!values || !values.length) return;
        const values1 = values.map(i => `(${i})`).join(', ');
        const sql = 'INSERT INTO ' + table + ' (' + columns.join(',') + ') VALUES ' + values1;
        changes = await client.run(sql);
      }

      logger.log('Write query id=' + id + ', changes=' + changes, 2);
    } catch (err) {
      sendError(id, err);
    }
  }

  async function read(id, queryObj) {
    try {
      const sql = queryObj.sql ? queryObj.sql : '';
      if (!sql) throw { message: 'Missing query.sql in read query: ' + util.inspect(queryObj) };

      const result = await client.query(sql);
      logger.log(sql + ' Result length = ' + result.length, 1);

      send({ id, query: queryObj, payload: result });
    } catch (err) {
      sendError(id, err);
    }
  }

  async function run(id, queryObj) {
    try {
      const sql = queryObj.sql ? queryObj.sql : '';
      if (!sql) throw { message: 'Missing query.sql in run query: ' + util.inspect(queryObj) };

      const changes = await client.run(sql);
      logger.log(`${sql}  Row(s) affected: ${changes}`, 2);
    } catch (err) {
      sendError(id, err);
    }
  }

  // {devicelog:[{level, days},..], pluginlog:{level,days}}
  async function del(payload) {
    if (!payload || !payload.rp || typeof payload.rp != 'object') return;
    for (const name of Object.keys(payload.rp)) {
      // Если есть такая таблица - обработать
      if (tableNames.includes(name)) {
        const arr = payload.rp[name];
        for (const item of arr) {
          await deleteRecordsByLevel(name, item.days, item.level);
        }
      }
    }
  }

  async function deleteRecordsByLevel(tableName, archDay, level) {
    const archDepth = archDay * 86400000;
    const delTime = Date.now() - archDepth;

    const sql = `DELETE FROM ${tableName} WHERE level = ${level} AND ts<${delTime}`;

    try {
      const changes = await client.run(sql);
      logger.log(`${tableName}  Level=${level} Archday=${archDay}  Row(s) deleted ${changes}`, 1);
    } catch (err) {
      sendError('delete', err);
    }
  }

  async function deleteRecordsMax(tableName) {
    // Оставляем только данные за 1 день
    const archDepth = 1 * 86400000;
    const delTime = Date.now() - archDepth;

    const sql = `DELETE FROM ${tableName} WHERE ts<${delTime}`;
    const mes = 'Number of records exceeded ' + maxlogrecords + '!! All except the last day data was deleted!!';

    try {
      const changes = await client.run(sql);
      logger.log(`${tableName}  ${mes} Row(s) deleted ${changes}`, 1);
    } catch (err) {
      sendError('delete', err);
    }
  }

  function send(message) {
    channel.send(message);
  }

  function sendError(id, err) {
    logger.log(err);
    send({ id, error: utils.getShortErrStr(err) });
  }

  function processExit(code, err) {
    let msg = '';
    if (err) msg = 'ERROR: ' + utils.getShortErrStr(err) + ' ';
    stmtMainlog.finalize();
    if (client && client.pool) {
      client.pool.close(err => {
        if (err) {
          msg += util.inspect(err, null, 4);
        } else {
          msg += 'Close connection pool.';
        }
        client.pool = null;
        logger.log(msg + ' Exit with code: ' + code);

        setTimeout(() => {
          channel.exit(code);
        }, 500);
      });
    }
  }

  async function sendDBSize() {
    if (!process.connected) return;
   
    try {
      let fileSize = 0;
      const data = {};
      let stats = await fs.stat(opt.dbPath);
      fileSize = stats.size / 1048576;
      stats = await fs.stat(opt.dbPath + '-shm');
      fileSize += stats.size / 1048576;
      stats = await fs.stat(opt.dbPath + '-wal');
      fileSize += stats.size / 1048576;
      data.size = Math.round(fileSize * 100) / 100;

      const needDelete = [];
      for (const name of tableNames) {
        // const result = await client.query('SELECT Count (*) count From ' + name);
        // const count = result ? result[0].count : 0;
        const count = await getTableRecordsCount(name);
        // if (count > 1000) {
        //  await showGroups(name);
        // }
        data[name] = count;
        if (maxlogrecords > 0 && count > maxlogrecords && name != 'mainlog') needDelete.push(name);
      }

      // Отправить фактическое состояние
      if (process.connected) process.send({ type: 'procinfo', data });

      if (!needDelete.length) return;

      for (const name of needDelete) {
        await deleteRecordsMax(name);
      }
    } catch (e) {
      logger.log('sendDBSize ERROR: '+util.inspect(e));
    }
  }

  async function getTableRecordsCount(name) {
    const result = await client.query('SELECT Count (*) count From ' + name);
    return result ? result[0].count : 0;
  }
}

// Частные функции
// Строка для создания таблиц в БД
function getCreateTableStr(tableName) {
  let result;
  switch (tableName) {
    case 'devicelog':
      result = 'did TEXT,prop TEXT,val TEXT,txt TEXT, ts INTEGER NOT NULL,tsid TEXT,cmd TEXT,sender TEXT';
      break;
    case 'pluginlog':
      result = 'unit TEXT, txt TEXT,level INTEGER, ts INTEGER NOT NULL, tsid TEXT, sender TEXT';
      break;

    case 'iseclog':
      result =
        'type TEXT, msg TEXT, subjid TEXT, subjname TEXT, result TEXT, changed TEXT, ip TEXT, class TEXT, app TEXT, version TEXT, objid TEXT, objname TEXT, level INTEGER, ts INTEGER NOT NULL, tsid TEXT,sender TEXT';
      break;

    default:
      result = 'tags TEXT, did TEXT, location TEXT, txt TEXT, level INTEGER, ts INTEGER NOT NULL,tsid TEXT,sender TEXT';
  }
  return 'CREATE TABLE IF NOT EXISTS ' + tableName + ' (' + result + ')';
}

function getColumns(tableName) {
  switch (tableName) {
    case 'devicelog':
      return ['did', 'prop', 'val', 'txt', 'ts', 'tsid', 'cmd', 'sender'];

    case 'pluginlog':
      return ['unit', 'txt', 'level', 'ts', 'tsid', 'sender'];

    case 'iseclog':
      return [
        'type',
        'msg',
        'subjid',
        'subjname',
        'objid',
        'objname',
        'result',
        'changed',
        'ip',
        'app',
        'class',
        'version',
        'level',
        'ts',
        'tsid',
        'sender'
      ];

    default:
      return ['tags', 'did', 'location', 'txt', 'level', 'ts', 'tsid', 'sender'];
  }
}

function getGroupQuery(tableName) {
  switch (tableName) {
    case 'devicelog':
      return 'SELECT did, Count (tsid) count from devicelog group by did';

    case 'pluginlog':
      return 'SELECT unit, Count (tsid) count from pluginlog group by unit';

    default:
      return 'SELECT level, Count (tsid) count from ' + tableName + ' group by level';
  }
}

function sendProcessInfo() {
  const mu = process.memoryUsage();
  const memrss = Math.floor(mu.rss / 1024);
  const memheap = Math.floor(mu.heapTotal / 1024);
  const memhuse = Math.floor(mu.heapUsed / 1024);
  if (process.connected) process.send({ type: 'procinfo', data: { state: 1, memrss, memheap, memhuse } });
}

function sendSettingsRequest() {
  if (process.connected) process.send({ id: 'settings', type: 'settings' });
}
