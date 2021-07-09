/**
 * dbagent.js
 * Точка входа при запуске дочернего процесса
 * Входной параметр - путь к файлу конфигурации или сама конфигурация как строка JSON
 * В данном случае ожидается JSON
 * 
 */

// const util = require('util');
const path = require('path');

const dbagent = require('./lib/index');
const logger = require('./logger');
const { promises: fs } = require('fs');

// Извлечь имя log или писать в /var/log
let opt;
try {
  opt = JSON.parse(process.argv[2]); // dbPath property
  opt.dbLimit = 1024;
} catch (e) {
  opt = {};
}

const logfile = opt.logfile || path.join(__dirname,'ih_sqlite3.log');
const loglevel = opt.loglevel || 0;

logger.start(logfile,loglevel);

logger.log('Start dbagent sqlite3. Options: ' + JSON.stringify(opt));

delete opt.logfile;
delete opt.loglevel;

sendProcessInfo();
setInterval(sendProcessInfo, 10000);
dbagent(process, opt, logger);

function sendProcessInfo() {
  const mu = process.memoryUsage();
  const memrss = Math.floor(mu.rss/1024);
  const memheap = Math.floor(mu.heapTotal/1024);
  const memhuse = Math.floor(mu.heapUsed/1024);

  if (process.connected) process.send({type:'procinfo', data:{memrss,memheap, memhuse }});
}



