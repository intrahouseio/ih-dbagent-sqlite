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
const schedule = require('node-schedule');
const { promises: fs } = require('fs');

const logger = require('./logger');
const client = require('./lib/client');
const utils = require('./lib/utils');

const tableNames = ['mainlog', 'pluginlog', 'devicelog', 'authlog'];

let opt;
try {
  opt = JSON.parse(process.argv[2]);
} catch (e) {
  opt = {};
}

const logfile = opt.logfile || path.join(__dirname, 'ih_sqlite3_logagent.log');
const loglevel = opt.loglevel || 0;
logger.start(logfile, loglevel);
logger.log('Start logagent sqlite3. Options: ' + JSON.stringify(opt));

sendProcessInfo();
setInterval(sendProcessInfo, 10000);
setInterval(async () => sendDBSize(), 60000);

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

    /*  
    await client.createTable(getCreateTableStr('mainlog'), 'mainlog');
    await client.createTable(getCreateTableStr('pluginlog'), 'pluginlog');
    await client.createTable(getCreateTableStr('devicelog'), 'devicelog');

    await client.run('CREATE INDEX IF NOT EXISTS mainlog_ts ON mainlog (tsid);');
    await client.run('CREATE INDEX IF NOT EXISTS devicelog_ts ON devicelog (tsid);');
    await client.run('CREATE INDEX IF NOT EXISTS pluginlog_ts ON pluginlog (tsid);');
*/
    for (const name of tableNames) {
      await client.createTable(getCreateTableStr(name), name);
      await client.run('CREATE INDEX IF NOT EXISTS ' + name + '_ts ON ' + name + ' (tsid);');
    }

    channel.on('message', ({ id, type, query, payload }) => {
      if (type == 'write') return write(id, query, payload);
      if (type == 'read') return read(id, query);
      if (type == 'run') return run(id, query);
      if (type == 'settings') return del(payload);
    });

    send({ id: 'settings', type: 'settings' });

    const hoursRule = new schedule.RecurrenceRule();
    hoursRule.rule = '7 0 * * * *';

    schedule.scheduleJob(hoursRule, () => {
      send({ id: 'settings', type: 'settings' });
    });

    process.on('SIGTERM', () => {
      logger.log('Received SIGTERM');
      processExit(0);
    });

    process.on('exit', () => {
      if (client && client.pool) client.pool.close();
    });
  } catch (err) {
    processExit(1, err);
  }

  /**
   *
   * @param {String} id - request uuid
   * @param {Objects} queryObj - {table}
   * @param {Array of Objects} payload - [{ dn, prop, ts, val }]
   */
  async function write(id, queryObj, payload) {
    const table = queryObj && queryObj.table ? queryObj.table : 'mainlog';
    const columns = getColumns(table);
    const values = utils.formValues(payload, columns);
    if (!values || !values.length) return;

    const values1 = values.map(i => `(${i})`).join(', ');
    const sql = 'INSERT INTO ' + table + ' (' + columns.join(',') + ') VALUES ' + values1;

    try {
      const changes = await client.run(sql);
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
      logger.log(`${sql}  Row(s) affected: ${changes}`, 1);
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

    if (client && client.pool) {
      client.pool.close();
      client.pool = null;
      msg += 'Close connection pool.';
    }

    logger.log(msg + ' Exit with code: ' + code);
    setTimeout(() => {
      channel.exit(code);
    }, 500);
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

    default:
      return ['tags', 'did', 'location', 'txt', 'level', 'ts', 'tsid', 'sender'];
  }
}

function sendProcessInfo() {
  const mu = process.memoryUsage();
  const memrss = Math.floor(mu.rss/1024)
  const memheap = Math.floor(mu.heapTotal/1024)
  const memhuse = Math.floor(mu.heapUsed/1024)
  if (process.connected) process.send({type:'procinfo', data:{state:1, memrss,memheap, memhuse }});
}

async function sendDBSize() {
  let stats = await fs.stat(opt.dbPath);
  let fileSize = stats["size"]/1048576;
  stats = await fs.stat(opt.dbPath+"-shm");
  fileSize = fileSize + stats["size"]/1048576;
  stats = await fs.stat(opt.dbPath+"-wal");
  fileSize = fileSize + stats["size"]/1048576;  
  if (process.connected) process.send({type:'procinfo', data:{size: Math.round(fileSize*100)/100}});
}