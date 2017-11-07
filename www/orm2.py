#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging,asyncio,aiomysql


def log(sql,args=()):
    logging.info('SQL:%s' % sql)

# 连接池编写
async def create_pool(loop,**kw):
    logging.info("create database connection pool ...")
    # 设置全局变量
    global __pool
    __pool = await aiomysql.create_pool(
        host=kw.get("host","localhost"),
        port=kw.get("port",3306),
        user=kw["user"],
        password=kw["password"],
        db=kw["db"],
        # 默认自动提交事务,不同手动提交事务
        charset=kw.get("autocommit",True),
        maxsize=kw.get("maxsize",10),
        minsize=kw.get("minsize",1),
        loop=loop
    )

async def select(sql,args,size=None):
    log(sql,args)
    # 从连接池中获取conn连接
    global __pool
    with (await __pool) as conn:
        # 获取游标,用于查找数据
        with (await conn.cursor(aiomysql.DictCursor)) as cur:
            # SQL占位符为 ？,MySQL占位符为%s
            await cur.execute(sql.replace("?","%s"),args or ())
            # 根据查询的数量调用不同的方法
            if size:
                rs = await cur.fetchmant(size)
            else:
                rs = await cur.fetchall()
            logging.info("rows returned %s" % len(rs))
            # 返回查询结果，元素是tuple的list
            return rs

# insert，update，delete，等可以改变数据库行结构的方法,返回改变数据库行的行数
async def execute(sql,args):
    log(sql)
    global __pool
    with (await __pool) as conn:
        try:
            with (await conn.cursor) as cur:
                await cur.execute(sql.replace("?","%s"),args)
                affected = cur.rowcount
        except BaseException as e:
            raise
        return affected
# 这个函数主要是把查询字段计数 替换成sql识别的?
# 比如说：insert into  `User` (`password`, `email`, `name`, `id`) values (?,?,?,?)  看到了么 后面这四个问号
def create_args_string(num):
    L = []
    for n in range(num):
        L.append("?")
    return ", ".join(L)

# 定义Field类，负责保存(数据库)表的字段名和字段类型
class Field(object):
    # 表的字段包含名字、类型、是否为表的主键和默认值
    def __init__(self,name,column_type,primary_key,default):
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default

    def __str__(self):
        return "<%s,%s:%s>" % (self.__class__.__name__,self.column_type,self.name)

# 定义数据库中五个存储类型
class StringField(Field):
    def __init__(self,name=None,primary_key=False,default=None,ddl="varchar(100)"):
        super().__init__(name,ddl,primary_key,default)

# 布尔类型不可以作为主键
class BooleanField(Field):
    def __init__(self, name=None, default=False):
        super().__init__(name,'Boolean',False, default)
# 不知道这个column type是否可以自己定义 先自己定义看一下
class IntegerField(Field):
    def __init__(self, name=None, primary_key=False, default=0):
        super().__init__(name, 'int', primary_key, default)
class FloatField(Field):
    def __init__(self, name=None, primary_key=False,default=0.0):
        super().__init__(name, 'float', primary_key, default)
class TextField(Field):
    def __init__(self, name=None, default=None):
        super().__init__(name,'text',False, default)

# 定义Model的元类
# 所有的元类都继承自type
# 主要工作室为了一个数据库表映射成一个封装的类做准备
class ModelMetaclass(type):
    # cls代表要__init__的类
    # bases:代表继承父类的集合
    # attrs:类的方法(属性)集合
    def __new__(cls,name,bases,attrs):
        # 排除Model类本身,防止自身被修改
        if name == "Model":
            return type.__new__(cls,name,bases,attrs)
        # 获取table名称,如果存在表名，则返回表明，反之返回name
        tableName = attrs.get("__table__",None) or name
        logging.info("found model: %s (talbe:%s)" %(name,tableName))
        # 获取所有的Pield和主键名：
        mappings = dict()
        # fields保存的是除主键外的属性名
        fields = []
        primaryKey = None
        # 这个k是表示字段名
        for k,v in attrs.items():
            if isinstance(v,Field):
                logging.info(" found mapping:%s ==>%s" %(k,v))
                mappings[k] = v
                if v.primary_key:
                    # 找到主键
                    if primaryKey:
                        # 一个表只能有一个主键，如果在出现主键，则报错
                        raise RuntimeError("Duplicate primary key for filed:%s" % k)
                    # 也就是说主键只能被设置一次
                    primaryKey = k
                else:
                    # 不是主键就保存在fields中
                    fields.append(k)
        # 如果没有主键，报错，没有找到主键
        if not primaryKey:
            raise RuntimeError("Primary key not found")
        # 从类属性中删除Field属性，避免冲突
        for k in mappings.keys():
            attrs.pop(k)
        # 保存除了主键外的属性为''列表形式
        # 将除主键外的其他属性编程'id','name'这种形式，关于反引号``的用法
        escaped_fields = list(map(lambda f:"`%s`" % f,fields))
        # 保存属性和列的映射关系
        attrs['__mappings__'] = mappings
        attrs['__table__'] = tableName
        attrs['__primary_key__'] = primaryKey  # 主键属性名
        attrs['__fields__'] = fields  # 除主键外的属性名
        attrs["__select__"] = "select `%s`,%s from `%s`" % (primaryKey, ", ".join(escaped_fields), tableName)
        attrs["__insert__"] = "insert into `%s` (%s , `%`) values (%s)" % (
        tableName, ", ".join(escaped_fields), primaryKey, create_args_string(len(escaped_fields) + 1))
        attrs["__update__"] = "update `%s` set %s where `%s` = ?" % (
        tableName, ", ".join(map(lambda f: "`%s` = ?" % (mappings.get(f).name or f), fields)), primaryKey)
        attrs["__delete__"] = "delete from `%s` where `%s` = ?" % (tableName, primaryKey)
        return type.__new__(cls, name, bases, attrs)


class Model(dict,metaclass=ModelMetaclass):

    def __init__(self,**kw):
        super(Model,self).__init__(**kw)

    def __getattr__(self,key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Model' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        self[key] = value

    def getValue(self,key):
        return getattr(self,key,None)

    def getValueOrDefault(self,key):
        value = getattr(self,key,None)
        if value is None:
            field = self.__mappings__[key]
            if field.default is not None:
                value = field.default() if callable(field.default) else field.default
                logging.debug("using default value for %s:%s" % (key,str(value)))
                setattr(self,key,value)

    @classmethod
    # 类方法有类变量cls传入，从而可以用cls做一些相关的处理，并且有子类继承时，调用该类的方法时，传入的类变量cls是子类的，而非父类的
    async def find(cls,pk):
        "find object bu primary key."
        rs = await select("%s where `%s` = ?" % (cls.__select__,cls.__primary_key__),[pk],1)
        if len(rs) == 0:
            return None
        # **rs关键字，构成一个cls类的列表，其实就是每条记录对应的类实例
        # 通过传入参数实例化对象
        return cls(**rs[0])

    async def save(self):
        args = list(map(self.getValueOrDefault,self.__fields__))
        args.append(self.getValueOrDefault(self.__primary_key__))
        rows = await execute(self.__insert__,args)
        if rows != 1:
            logging.warn("failed to insert record:affected rows:%s" % rows)

