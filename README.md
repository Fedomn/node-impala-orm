使用[ImpalaClient](https://github.com/Fedomn/node-impala-beeswax)，ImpalaClient主要增加了configuration配置，修改每次查询default_query_options配置。

ImpalaOrm参考sequelizejs思路编写，实现findAll和findAndCountAll方法。

主要功能把attributes/where/group/order/limit/offset转换成impala需要的SQL。

其中要注意到impala where条件查询字段类型必须要与数据库字段类型一致。所以，我们要根据类型动态拼接SQL。还是一样通过builder动态构造model。
