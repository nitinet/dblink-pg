import { Handler, model, sql } from 'dblink-core';
import pg from 'pg';
import pgQueryStream from 'pg-query-stream';
import { Readable } from 'stream';

/**
 * PostGreSql Handler
 *
 * @export
 * @class PostgreSql
 * @typedef {PostgreSql}
 * @extends {Handler}
 */
export default class PostgreSql extends Handler {
  /**
   * Connection Pool
   *
   * @type {pg.Pool}
   */
  connectionPool: pg.Pool;

  /**
   * Creates an instance of PostgreSql.
   *
   * @constructor
   * @param {model.IConnectionConfig} config
   */
  constructor(config: model.IConnectionConfig) {
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

  /**
   * Handler initialisation
   *
   * @async
   * @returns {Promise<void>}
   */
  async init(): Promise<void> {
    /* document why this async method 'init' is empty */
  }

  /**
   * Get new Connection
   *
   * @async
   * @returns {Promise<pg.PoolClient>}
   */
  async getConnection(): Promise<pg.PoolClient> {
    let conn = await this.connectionPool.connect();
    return conn;
  }

  /**
   * Initialize Transaction
   *
   * @async
   * @param {pg.Client} conn
   * @returns {Promise<void>}
   */
  async initTransaction(conn: pg.Client): Promise<void> {
    await conn.query('BEGIN');
  }

  /**
   * Commit Transaction
   *
   * @async
   * @param {pg.PoolClient} conn
   * @returns {Promise<void>}
   */
  async commit(conn: pg.PoolClient): Promise<void> {
    await conn.query('COMMIT');
  }

  /**
   * Rollback Transaction
   *
   * @async
   * @param {pg.PoolClient} conn
   * @returns {Promise<void>}
   */
  async rollback(conn: pg.PoolClient): Promise<void> {
    await conn.query('ROLLBACK');
  }

  /**
   * Close Connection
   *
   * @async
   * @param {pg.PoolClient} conn
   * @returns {Promise<void>}
   */
  async close(conn: pg.PoolClient): Promise<void> {
    conn.release();
  }

  /**
   * Run string query
   *
   * @async
   * @param {string} query
   * @param {?any[]} [dataArgs]
   * @param {?pg.Client} [connection]
   * @returns {Promise<model.ResultSet>}
   */
  async run(query: string, dataArgs?: any[], connection?: pg.Client): Promise<model.ResultSet> {
    let temp: pg.QueryResult<any>;
    if (connection) {
      temp = await connection.query(query, dataArgs);
    } else {
      let con = await this.connectionPool.connect();
      try {
        temp = await con.query(query, dataArgs);
      } finally {
        con.release();
      }
    }

    let result = new model.ResultSet();
    result.rowCount = temp.rowCount ?? 0;
    result.rows = temp.rows;
    return result;
  }

  /**
   * Run statements
   *
   * @async
   * @param {(sql.Statement | sql.Statement[])} queryStmt
   * @param {?pg.Client} [connection]
   * @returns {Promise<model.ResultSet>}
   */
  async runStatement(queryStmt: sql.Statement | sql.Statement[], connection?: pg.Client): Promise<model.ResultSet> {
    let { query, dataArgs } = this.prepareQuery(queryStmt);
    query = this.convertPlaceHolder(query);

    return this.run(query, dataArgs, connection);
  }

  /**
   * Run quries and stream output
   *
   * @async
   * @param {string} query
   * @param {?any[]} [dataArgs]
   * @param {?pg.Client} [connection]
   * @returns {Promise<Readable>}
   */
  async stream(query: string, dataArgs?: any[], connection?: pg.Client): Promise<Readable> {
    const queryStream = new pgQueryStream(query, dataArgs);
    let stream: Readable;

    if (connection) {
      stream = connection.query(queryStream);
    } else {
      let con = await this.connectionPool.connect();

      stream = con.query(queryStream);
      stream.on('end', () => {
        con.release();
      });
    }
    return stream;
  }

  /**
   * Run statements and stream output
   *
   * @param {(sql.Statement | sql.Statement[])} queryStmt
   * @param {?pg.Client} [connection]
   * @returns {Promise<Readable>}
   */
  streamStatement(queryStmt: sql.Statement | sql.Statement[], connection?: pg.Client): Promise<Readable> {
    let { query, dataArgs } = this.prepareQuery(queryStmt);
    query = this.convertPlaceHolder(query);

    return this.stream(query, dataArgs, connection);
  }

  /**
   * Convert place holders for query
   *
   * @param {string} query
   * @returns {string}
   */
  convertPlaceHolder(query: string): string {
    let i = 1;
    while (query.includes('?')) {
      query = query.replace('?', `$${i}`);
      i++;
    }
    return query;
  }

  /**
   * Limit Operator
   *
   * @param {string} size
   * @param {?string} [index]
   * @returns {string}
   */
  limit(size: string, index?: string): string {
    return ' limit ' + size + (index ? ' OFFSET ' + index : '');
  }
}
