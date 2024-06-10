import { Handler, model } from 'dblink-core';
import pg from 'pg';
import pgQueryStream from 'pg-query-stream';
export default class PostgreSql extends Handler {
  connectionPool;
  constructor(config) {
    super(config);
    this.connectionPool = new pg.Pool({
      user: this.config.username,
      password: this.config.password,
      database: this.config.database,
      host: this.config.host,
      port: this.config.port,
      max: this.config.connectionLimit
    });
  }
  async init() {}
  async getConnection() {
    const conn = await this.connectionPool.connect();
    return conn;
  }
  async initTransaction(conn) {
    await conn.query('BEGIN');
  }
  async commit(conn) {
    await conn.query('COMMIT');
  }
  async rollback(conn) {
    await conn.query('ROLLBACK');
  }
  async close(conn) {
    conn.release();
  }
  async run(query, dataArgs, connection) {
    let temp;
    if (connection) {
      temp = await connection.query(query, dataArgs);
    } else {
      const con = await this.connectionPool.connect();
      try {
        temp = await con.query(query, dataArgs);
      } finally {
        con.release();
      }
    }
    const result = new model.ResultSet();
    result.rowCount = temp.rowCount ?? 0;
    result.rows = temp.rows;
    return result;
  }
  async runStatement(queryStmt, connection) {
    let { query, dataArgs } = this.prepareQuery(queryStmt);
    query = this.convertPlaceHolder(query);
    return this.run(query, dataArgs, connection);
  }
  async stream(query, dataArgs, connection) {
    const queryStream = new pgQueryStream(query, dataArgs);
    let stream;
    if (connection) {
      stream = connection.query(queryStream);
    } else {
      const con = await this.connectionPool.connect();
      stream = con.query(queryStream);
      stream.on('end', () => {
        con.release();
      });
    }
    return stream;
  }
  streamStatement(queryStmt, connection) {
    let { query, dataArgs } = this.prepareQuery(queryStmt);
    query = this.convertPlaceHolder(query);
    return this.stream(query, dataArgs, connection);
  }
  convertPlaceHolder(query) {
    let i = 1;
    while (query.includes('?')) {
      query = query.replace('?', `$${i}`);
      i++;
    }
    return query;
  }
  limit(size, index) {
    return ' limit ' + size + (index ? ' OFFSET ' + index : '');
  }
}
//# sourceMappingURL=index.js.map
