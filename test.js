const dbagent = require('./lib/index.js');

const logger = {
 log: console.log
}

dbagent(process, {dbPath:"./hist.db"}, logger);

setTimeout(()=>{}, 1000* 60 * 60)