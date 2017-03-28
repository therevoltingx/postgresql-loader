
/* eslint-env jest, jasmine */

const moment = require('moment')
const assert = require('assert')
const { v4 } = require('uuid')
const pg = require('pg')
const path = require('path')
const fs = require('fs')

const PostgreSQLLoader = require('..')

jasmine.DEFAULT_TIMEOUT_INTERVAL = 50000

const options = {
  table: 'test_events',

  postgres: {
    user: process.env.PG_USER,
    database: process.env.PGDATABASE,
    password: process.env.PGPASSWORD,
    host: process.env.PGHOST,
    port: process.env.PGPORT || 5432,
    max: 10,
    idleTimeoutMillis: 3000
  }
}

const pool = new pg.Pool(options.postgres);

beforeAll(function(done) {
  var content = fs.readFileSync(path.join(__dirname, 'table.sql'), 'utf8');

  pool.query(content, function(err, result) {
    if (err) {
      return console.error('error running query', err);
    }

    done(err);
  });
});

describe('upload and check', () => {
  const event = {
    id: Math.random().toString(36),
    event: 'impression',
    received_at: '2016-06-23 15:54:16'
  };

  it('it should upload data', (done) => {
    var loader = PostgreSQLLoader(options);
    loader.write(event);
    loader.end();

    loader.on('finish', () => {
      done();
    });
  });

  it('should have downloaded the data', () => {
    return pool.query(`
      SELECT COUNT(*) AS count
      FROM test_events
      WHERE id = $1
    `, [
      event.id
    ]).then(result => {
      assert(~~result.rows[0].count > 0);
    })
  });
});
