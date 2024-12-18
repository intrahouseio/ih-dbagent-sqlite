/*const dbagent = require('./lib/index.js');

const logger = {
 log: console.log
}

dbagent(process, {dbPath:"./hist.db"}, logger);

setTimeout(()=>{}, 1000* 60 * 60)*/
sqlite3 = require('sqlite3').verbose();
util = require("util")
let db = new sqlite3.Database("./test.db", err => {
  if (err) console.log("err: " + err);
});


db.serialize(function () {
  // These two queries will run sequentially.
  let sql = "";
  db.run("CREATE TABLE IF NOT EXISTS records (id INTEGER PRIMARY KEY NOT NULL,ts INTEGER NOT NULL,dn TEXT NOT NULL,prop TEXT,val REAL)");
  const start = Date.now();
  console.log(start)
  var stmt = db.prepare("INSERT INTO records (ts, dn, prop, val) VALUES (?,?,?,?)");
  for (var i = 0; i < 10000; i++) {
    stmt.run(start+i, "AI001", "value", i);
  }
  stmt.finalize();
  console.log(Date.now());

  /*stmt = db.prepare("SELECT * FROM records WHERE id=?");
  stmt.each(userId, function (err, row) {
    console.log(row.name, row.email);
  }, function (err, count) {
    stmt.finalize();
  });*/
  /* sql = `INSERT INTO testtable (ts, payload) VALUES (${date}, json('{"col1":"name", "col2":"lastName", "col3":1234}'));`
    
    db.run(sql, (err, records) => {
        if (!err) console.log("Records: " + records);
      });
    */
  //sql = "SELECT testtable.payload FROM testtable "
  /*sql = "SELECT json(testtable.payload) FROM testtable"
   db.all(sql, (err, records) => {
       if (!err) console.log("Records: " + util.inspect(records));
     });
     
   sql = "SELECT json_extract(testtable.payload, '$.col3') FROM testtable;"
   db.all(sql, (err, records) => {    
       if (!err) {
           let data = [];
           records.forEach(function (record) {
               data.push(Object.values(record)[0]);
           });
           console.log("Records: " + util.inspect(data));
       }
     });*/

  /*sql = `UPDATE testtable SET payload = json_set(payload, '$.col3', 9876)`
   db.all(sql, (err, records) => {
       if (!err) console.log("Records: " + util.inspect(records));
     });
*/
  /*sql = "SELECT * FROM testtable, json_each(testtable.payload) WHERE  json_each.value = 1234";
  db.all(sql, (err, records) => {
      (!err) ? console.log("Records: " + util.inspect(records)): console.log(err);
    });*/
});
db.close();