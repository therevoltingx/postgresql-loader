const { Writable } = require('stream')
const { createGzip } = require('zlib')
const tempPath = require('temp-path')
const assert = require('assert')
const moment = require('moment')
const { v4 } = require('uuid')
const pg = require('pg')
const fs = require('fs')

exports = module.exports = options => {
  return new PostgreSQLLoader(options)
}

exports.onError = err => {
  if (!err) return

  console.error(err.stack || err)
}

class PostgreSQLLoader extends Writable {
  constructor (options) {
    super({
      objectMode: true
    })

    this.options = options
    // which stream to write to
    this.i = 0
    // whether data has been written
    this.written = false

    this._validateOptions()

    this.pool = new pg.Pool(this.options.postgres);
  }

  _validateOptions () {
    const { options } = this
    assert(options, 'Options are required!');
    this.table = this.options.table;
  }

  _write (data, NULL, cb) {
    var schemaSql = `select column_name, data_type, character_maximum_length from INFORMATION_SCHEMA.COLUMNS where table_name = '${this.table}'`;
    var _this = this;

    // console.log(schemaSql);
    var res = this.pool.query(schemaSql, [], function(err, res) {
      if (err) {
        console.log('error running schema query');
        return cb(err);
      }

      // console.log(res.rows);
      var columns = res.rows.map(function(row, index) {
        var name = row['column_name'];

        var res = {
          'name': name,
          'value': data[name]
        }

        return res;
      });

      var names = columns.map(function(entry, index) {
        return entry['name'];
      }).join(',');

      var params = columns.map(function(entry, index){
        return `$${index + 1}`;
      });

      var values = columns.map(function(entry, index) {
        return entry['value'];
      });

      var insertSql = `INSERT INTO ${_this.table}(${names}) VALUES(${params})`;
      // onsole.log(insertSql);
      _this.written = true
      _this.pool.query(insertSql, values, function() {
        if (err) {
          console.log('error running insert');
        }

        return cb(err);
      });
    });

    return res;
  }
}

exports.PostgreSQLLoader = PostgreSQLLoader
