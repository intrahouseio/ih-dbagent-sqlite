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

const logger = require('./logger');
const client = require('./lib/client');
const utils = require('./lib/utils');

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

    await client.createTable(getCreateTableStr('mainlog'), 'mainlog');
    await client.createTable(getCreateTableStr('pluginlog'), 'pluginlog');
    await client.createTable(getCreateTableStr('devicelog'), 'devicelog');
    await client.run('CREATE INDEX IF NOT EXISTS mainlog_ts ON mainlog (tsid);');
    await client.run('CREATE INDEX IF NOT EXISTS devicelog_ts ON devicelog (tsid);');
    await client.run('CREATE INDEX IF NOT EXISTS pluginlog_ts ON pluginlog (tsid);');

    channel.on('message', ({ id, type, query, payload }) => {
      if (type == 'write') return write(id, query, payload);
      if (type == 'read') return read(id, query);
      // if (type == 'settings') return del(payload);
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
      result = 'did TEXT,prop TEXT,val TEXT,ts INTEGER NOT NULL,tsid TEXT,cmd TEXT,login TEXT';
      break;
    case 'pluginlog':
      result = 'ts INTEGER NOT NULL,tsid TEXT,unit TEXT,txt TEXT,level INTEGER';
      break;
    default:
      result = 'ts INTEGER NOT NULL,tsid TEXT,unit TEXT,txt TEXT,level INTEGER';
  }
  return 'CREATE TABLE IF NOT EXISTS ' + tableName + ' (' + result + ')';
}

function getColumns(tableName) {
  switch (tableName) {
    case 'devicelog':
      return ['did', 'prop', 'val', 'ts', 'tsid', 'cmd', 'login'];

    case 'pluginlog':
      return ['unit', 'txt', 'level', 'ts', 'tsid'];

    default:
      return ['unit','unit', 'txt', 'level', 'ts', 'tsid'];
  }
}
