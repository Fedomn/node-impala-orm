import * as _ from 'lodash';
import * as NodeImpala from 'node-impala-beeswax';
import * as GenericPool from 'generic-pool';
import LogUtil from "./log-util";

export namespace ImpalaConnPool {
  const nodeImpalaConfig = {
    host: 'host',
    port: 'port',
    resultType: 'json-array',
    timeout: 1000 * 5,
  };

  const factory = {
    create: () => {
      return new Promise((resolve, reject) => {
        const impalaClient = NodeImpala.createClient();
        impalaClient.connect(nodeImpalaConfig).then((msg) => {
          // LogUtil.server.info(`ImpalaConnPool Create Success: ${msg}`);
          resolve(impalaClient);
        }).catch((err) => {
          LogUtil.server.warn(`ImpalaConnPool Create Error: ${err}`);
          reject(err);
        });
      });
    },
    destroy: (impalaClient) => {
      return new Promise((resolve, reject) => {
        impalaClient.close(() => {
          // LogUtil.server.info(`ImpalaConnPool Close Success`);
          resolve();
        }).catch((err) => {
          LogUtil.server.warn(`ImpalaConnPool Close Error: ${err}`);
          reject(err);
        });
      }) as any;
    },
  };

  const poolConfig = {
    max: 20,
    min: 0,
    evictionRunIntervalMillis: 1000 * 10, // 10s
    softIdleTimeoutMillis: 1000 * 9, // 9s
    idleTimeoutMillis: 1000 * 9, // 9s
    acquireTimeoutMillis: 1000 * 10, // 10s
  };

  export const connPool = GenericPool.createPool(factory, poolConfig);

  export async function getConn() {
    try {
      const coon = await connPool.acquire();
      // LogUtil.server.info('ImpalaConnPool Acquire Success');
      return coon;
    } catch (err) {
      LogUtil.server.warn(`ImpalaConnPool Acquire Error: ${err}`);
      throw err;
    }
  }

  export async function release(client) {
    try {
      await connPool.release(client);
      // LogUtil.server.info('ImpalaConnPool Release Success');
    } catch (e) {
      LogUtil.server.info('ImpalaConnPool Release Error');
    }
  }

  export async function drain() {
    try {
      await connPool.drain();
      // LogUtil.server.info('ImpalaConnPool Drain Success');
    } catch (err) {
      LogUtil.server.info('ImpalaConnPool Drain Error', err);
    }
    try {
      await connPool.clear();
      // LogUtil.server.info('ImpalaConnPool Clear Success');
    } catch (err) {
      LogUtil.server.info('ImpalaConnPool Clear Error', err);
    }
  }
}

namespace ImpalaUtil {
  const getFieldSchemas = async (sql) => {
    const impalaConn = await ImpalaConnPool.getConn();
    try {
      const metadata = await impalaConn.getResultsMetadata(sql);
      const fieldSchemas = metadata['schema']['fieldSchemas'];
      return _.reduce(fieldSchemas, (reduceRes, each) => {
        reduceRes[each['name']] = each['type'];
        return reduceRes;
      }, {});
    } catch (err) {
      LogUtil.server.warn(`ImpalaUtil getFieldSchemas Error: ${err}`);
      throw err;
    } finally {
      await ImpalaConnPool.release(impalaConn);
    }
  };

  /**
   * convert data type(int bigint double string)
   */
  const convertDataType = (schemas, data) => {
    return _.reduce(data, (reduceRes, eachRow: any) => {
      const cloneRow = _.reduce(eachRow, (cloneRowRes, eachFieldVal: any, eachFieldKey: any) => {
        if (schemas[eachFieldKey] === 'bigint' || schemas[eachFieldKey] === 'int') {
          cloneRowRes[eachFieldKey] = isNaN(parseInt(eachFieldVal)) ? null : parseInt(eachFieldVal);
        } else if (schemas[eachFieldKey] === 'double') {
          cloneRowRes[eachFieldKey] = isNaN(parseFloat(eachFieldVal)) ? null : parseFloat(eachFieldVal);
        } else {
          cloneRowRes[eachFieldKey] = eachFieldVal;
        }
        return cloneRowRes;
      }, {});
      reduceRes.push(cloneRow);
      return reduceRes;
    }, []);
  };

  export async function query(sql, configuration?) {
    // LogUtil.server.info('ImpalaConnPool Available Number: ', ImpalaConnPool.connPool.available);
    // LogUtil.server.info('ImpalaConnPool Borrowed Number: ', ImpalaConnPool.connPool.borrowed);
    // LogUtil.server.info('ImpalaConnPool Pending Number: ', ImpalaConnPool.connPool.pending);
    const impalaConn = await ImpalaConnPool.getConn();
    try {
      LogUtil.server.info('ImpalaUtil Executing: ', sql);
      const schemas = await getFieldSchemas(sql);
      const data = await impalaConn.query(sql, configuration);
      return convertDataType(schemas, data);
    } catch (queryErr) {
      LogUtil.server.warn('ImpalaUtil Exception: ', queryErr.message);
      throw queryErr;
    } finally {
      await ImpalaConnPool.release(impalaConn);
    }
  }

  export namespace SqlFormatter {
    const typeToStr = Object.prototype.toString;
    const ARRAY_TYPE_STR = '[object Array]';
    const OBJECT_TYPE_STR = '[object Object]';

    const isObject = (val) => {
      return typeToStr.call(val) === OBJECT_TYPE_STR;
    };

    const isArray = (val) => {
      return typeToStr.call(val) === ARRAY_TYPE_STR;
    };

    const replaceBindString = (sql, bindVal, key) => {
      if (!isArray(bindVal)) {
        bindVal = [bindVal];
      }
      const replaceVal = `'${bindVal.join(`','`)}'`;
      return _.replace(sql, new RegExp(`\\$${key}`, 'g'), replaceVal);
    };

    const replaceBindNumber = (sql, bindVal, key) => {
      if (!isArray(bindVal)) {
        bindVal = [bindVal];
      }
      const replaceVal = bindVal.join(',');
      return _.replace(sql, new RegExp(`\\$${key}`, 'g'), replaceVal);
    };

    /**
     * 默认bind param类型为string
     * No Time 注释省略。。。
     */
    export const formatBindParameters = (sql, bind) => {
      let cloneSql = _.cloneDeep(sql);
      _.forEach(bind, (value, key) => {
        if (isObject(value)) {
          const bindType = value.type;
          const bindVal = value.value;
          switch (bindType) {
            case 'number':
              cloneSql = replaceBindNumber(cloneSql, bindVal, key);
              break;
            default:
              cloneSql = replaceBindString(cloneSql, bindVal, key);
              break;
          }
        } else {
          cloneSql = replaceBindString(cloneSql, value, key);
        }
      });
      return cloneSql;
    };
  }

}

export default ImpalaUtil;
