import * as _ from "lodash";
import FsUtil from "./fs-util";
import ImpalaUtil from "./impala-util";

namespace ImpalaModel {
  // 转换select sql
  const convertAttributes2Sql = (attributes) => {
    const attrSql = _.reduce(attributes, (reduceRes, each) => {
      let copyEach = _.cloneDeep(each);
      if (!_.isArray(copyEach)) {
        copyEach = [copyEach];
      }
      const column = copyEach[0];
      const alias = copyEach[1] ? copyEach[1] : copyEach[0];
      reduceRes.push(`${column} as ${alias}`);
      return reduceRes;
    }, []).join(', ');
    return `select ${attrSql}`;
  };

  // 转换where sql
  const typeToStr = (val) => {
    return Object.prototype.toString.call(val);
  };
  const ARRAY_TYPE_STR = '[object Array]';
  const OBJECT_TYPE_STR = '[object Object]';
  const arrayToList = (array) => {
    let sql = '';

    for (let i = 0; i < array.length; i++) {
      const val = array[i];

      if (Array.isArray(val)) {
        sql += (i === 0 ? '' : ', ') + '(' + arrayToList(val) + ')';
      } else {
        sql += (i === 0 ? '' : ', ') + escape(val);
      }
    }

    return sql;
  };
  const escape = (val) => {
    if (val === undefined || val === null) {
      return 'NULL';
    }

    switch (typeof val) {
      case 'boolean':
        return (val) ? 'true' : 'false';
      case 'number':
        return val + '';
      case 'object':
        if (Array.isArray(val)) {
          return arrayToList(val);
        } else {
          return `'${val}'`;
        }
      default:
        return `'${val}'`;
    }
  };
  const convertWhere2Sql = (where) => {
    const filterSql = _.reduce(where, (reduceRes, value, key) => {
      if (typeToStr(value) === OBJECT_TYPE_STR) {
        let tmpSql = '';
        const comparator = _.keys(value)[0];
        const queryVal = _.values(value)[0];
        if (comparator === '$between') {
          tmpSql = `${key} between ${escape(queryVal[0])} and ${escape(queryVal[1])}`;
        }
        if (comparator === '$like') {
          tmpSql = `${key} like ${escape(queryVal)}`;
        }
        if (comparator === '$ne') {
          tmpSql = `${key} != ${escape(queryVal)}`;
        }
        if (comparator === '$in') {
          tmpSql = `${key} in (${escape(queryVal)})`;
        }
        if (comparator === '$notIn') {
          tmpSql = `${key} not in (${escape(queryVal)})`;
        }
        if (comparator === '$gt') {
          tmpSql = `${key} > ${escape(queryVal)}`;
        }
        if (comparator === '$lt') {
          tmpSql = `${key} < ${escape(queryVal)}`;
        }
        reduceRes.push(tmpSql);

        // 处理date_flag分区
        if (key === 'report_date' && comparator === '$between') {
          reduceRes.push(`date_flag between ${escape(queryVal[0])} and ${escape(queryVal[1])}`);
        }
      } else {
        reduceRes.push(`${key} = ${escape(value)}`);

        // 处理date_flag分区
        if (key === 'report_date') {
          reduceRes.push(`date_flag = ${escape(value)}`);
        }
      }
      return reduceRes;
    }, []).join(' and ');
    return filterSql ? `where ${filterSql}` : '';
  };

  // 转换group sql
  const convertGroupSql = (group) => {
    if (group) {
      if (typeToStr(group) !== ARRAY_TYPE_STR) {
        group = [group];
      }
      return `group by ${group.join(', ')}`;
    } else {
      return '';
    }
  };

  // 转换order sql
  const convertOrderSql = (order) => {
    if (order) {
      if (typeToStr(order) !== ARRAY_TYPE_STR) {
        order = [order];
      }
      return `order by ${order.join(', ')}`;
    } else {
      return '';
    }
  };

  // 转换limit sql
  const convertLimitSql = (limit, offset) => {
    const resSql = [];
    if (limit) {
      resSql.push(`limit ${limit}`);
    }
    if (offset) {
      resSql.push(`offset ${offset}`);
    }
    return resSql.join(' ');
  };

  // bind model func
  const bindModelCommonFunc = (model) => {
    model['findAll'] = async ({attributes, where, group, order, limit, offset}) => {
      const selectSql = convertAttributes2Sql(attributes);
      const fromSql = `from ${model.tableName}`;
      const whereSql = convertWhere2Sql(where);
      const groupSql = convertGroupSql(group);
      const orderSql = convertOrderSql(order);
      const limitSql = convertLimitSql(limit, offset);
      const lastQuerySql = _.compact([selectSql, fromSql, whereSql, groupSql, orderSql, limitSql]).join('\n');
      return await ImpalaUtil.query(lastQuerySql);
    };
    model['findAndCountAll'] = async ({attributes, where, group, order, limit, offset}) => {
      const countSql = 'select count(1) as count';
      const detailSql = convertAttributes2Sql(attributes);
      const fromSql = `from ${model.tableName}`;
      const whereSql = convertWhere2Sql(where);
      const groupSql = convertGroupSql(group);
      const orderSql = convertOrderSql(order);
      const limitSql = convertLimitSql(limit, offset);
      const countQuerySql = _.compact([countSql, fromSql, whereSql, groupSql]).join('\n');
      const detailQuerySql = _.compact([detailSql, fromSql, whereSql, groupSql, orderSql, limitSql]).join('\n');
      const countData = await ImpalaUtil.query(countQuerySql);
      const total = groupSql
        ? (_.isArray(countData) ? countData.length : 0)
        : (countData[0] ? countData[0]['count'] : 0);
      const detailData = total === 0 ? [] : await ImpalaUtil.query(detailQuerySql);
      return {
        data: detailData,
        total,
      };
    };
  };

  // 构造IMPALA MODEL实现常用的findAll/findAndCountAll
  export class Builder {
    private _models: any = {};

    constructor(inputDirName) {
      const ctx = this;

      const modelFileList = FsUtil.getExportFileList(inputDirName);

      modelFileList.forEach((file) => {
        const model: any = require(file).default();
        ctx._models[model.modelName] = model;
      });

      Object.keys(ctx._models).forEach((modelName) => {
        bindModelCommonFunc(ctx._models[modelName]);
      });
    }

    get models() {
      return this._models;
    }
  }
}

export default ImpalaModel;
