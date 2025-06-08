import { Handler, model, sql } from 'dblink-core';
import { DataType, IEntityType } from 'dblink-core/src/types';
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
   * @param {pg.PoolConfig} config
   */
  constructor(config: pg.PoolConfig) {
    super(config);

    this.connectionPool = new pg.Pool(config);
  }

  /**
   * Get new Connection
   *
   * @async
   * @returns {Promise<pg.PoolClient>}
   */
  async getConnection(): Promise<pg.PoolClient> {
    const conn = await this.connectionPool.connect();
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
   * @param {?unknown[]} [dataArgs]
   * @param {?pg.Client} [connection]
   * @returns {Promise<model.ResultSet>}
   */
  async run(query: string, dataArgs?: unknown[], connection?: pg.Client): Promise<model.ResultSet> {
    let temp: pg.QueryResult<Record<string, unknown>>;
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
    // eslint-disable-next-line prefer-const
    let { query, dataArgs } = this.prepareQuery(queryStmt);
    query = this.convertPlaceHolder(query);

    return this.run(query, dataArgs, connection);
  }

  /**
   * Run quries and stream output
   *
   * @async
   * @param {string} query
   * @param {?unknown[]} [dataArgs]
   * @param {?pg.Client} [connection]
   * @returns {Promise<Readable>}
   */
  async stream(query: string, dataArgs?: unknown[], connection?: pg.Client): Promise<Readable> {
    const queryStream = new pgQueryStream(query, dataArgs);
    let stream: Readable;

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

  /**
   * Run statements and stream output
   *
   * @param {(sql.Statement | sql.Statement[])} queryStmt
   * @param {?pg.Client} [connection]
   * @returns {Promise<Readable>}
   */
  streamStatement(queryStmt: sql.Statement | sql.Statement[], connection?: pg.Client): Promise<Readable> {
    // eslint-disable-next-line prefer-const
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

  /**
   * creates a returing columns expression for the insert statement
   *
   * @abstract
   * @param {sql.INode[]} returnColumns
   * @returns {string}
   */
  getReturnColumnsStr(returnColumns: sql.INode[]): string {
    const returnColumnsStr = returnColumns.map(a => a.eval(this).query).join(', ');
    return `returning ${returnColumnsStr}`;
  }

  serializeValue(val: unknown, dataType: IEntityType<DataType>): unknown {
    if (dataType == Array) {
      const str = (val as Array<unknown>).map(a => JSON.stringify(a)).join(',');
      return `{${str}}`;
    } else return val;
  }

  deSerializeValue(val: unknown, dataType: IEntityType<DataType>): unknown {
    if (dataType == Array) {
      const str = val as string;
      return str
        .substring(1, str.length - 1)
        .split(',')
        .map(a => JSON.parse(a));
    } else return val;
  }
}
