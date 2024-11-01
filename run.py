# -*- coding: utf-8 -*-
# Author: 薄荷你玩
# Date: 2024/11/01
import pyodbc
import pymysql

# SQL Server 配置
sqlserver_config = {
    'server': 'localhost',  # SqlServer服务器地址
    'database': 'dbname',  # 数据库名称
    'user': 'sa',  # 用户名
    'password': '123'  # 密码
}

# MySQL 配置
mysql_config = {
    'host': 'localhost',  # MySQL服务器地址
    'user': 'root',  # MySQL用户名
    'password': '123'  # MySQL密码
}


def get_sqlserver_connection():
    """连接到 SQL Server"""
    conn_str = (
        "DRIVER={SQL Server};"
        f"SERVER={sqlserver_config['server']};"
        f"DATABASE={sqlserver_config['database']};"
        f"UID={sqlserver_config['user']};"
        f"PWD={sqlserver_config['password']}"
    )
    return pyodbc.connect(conn_str)


def get_mysql_connection(db=None):
    """连接到 MySQL"""
    return pymysql.connect(
        host=mysql_config['host'],
        user=mysql_config['user'],
        password=mysql_config['password'],
        database=db,
        charset='utf8mb4'
    )


def get_sqlserver_tables(cursor):
    """获取 SQL Server 中所有表"""
    cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
    return [row.TABLE_NAME for row in cursor.fetchall()]


def get_table_schema(cursor, table_name):
    """获取 SQL Server 表的列定义"""
    cursor.execute(
        f"SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'")
    return cursor.fetchall()


def map_sqlserver_type_to_mysql(sqlserver_type, length):
    """将 SQL Server 类型映射到 MySQL 类型"""
    type_map = {
        'int': 'INT',
        'bigint': 'BIGINT',
        'smallint': 'SMALLINT',
        'tinyint': 'TINYINT',
        'bit': 'BOOLEAN',
        'nvarchar': f'VARCHAR({length})' if length != -1 else 'TEXT',
        'varchar': f'VARCHAR({length})' if length != -1 else 'TEXT',
        'text': 'TEXT',
        'datetime': 'DATETIME',
        'date': 'DATE',
        'float': 'FLOAT',
        'decimal': 'DECIMAL(10, 2)'
    }
    return type_map.get(sqlserver_type, 'TEXT')  # 默认为 TEXT


def create_mysql_database(cursor, database_name):
    """在 MySQL 中创建新数据库"""
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
    print(f"\n正在创建数据库: [{database_name}]...")
    print(f"Create database: [{database_name}]...")
    cursor.execute(f"USE {database_name}")


def create_mysql_table(cursor, table_name, columns):
    """在 MySQL 中创建表"""
    column_definitions = []
    for column_name, data_type, length in columns:
        mysql_type = map_sqlserver_type_to_mysql(data_type, length)
        column_definitions.append(f"`{column_name}` {mysql_type}")
    column_definitions_str = ", ".join(column_definitions)
    create_table_sql = f"CREATE TABLE IF NOT EXISTS `{table_name}` ({column_definitions_str})"
    cursor.execute(create_table_sql)


def transfer_data(sqlserver_cursor, mysql_cursor, table_name, columns):
    """将数据从 SQL Server 转移到 MySQL"""
    sqlserver_cursor.execute(f"SELECT * FROM {table_name}")
    rows = sqlserver_cursor.fetchall()
    if not rows:
        return

    placeholders = ", ".join(["%s"] * len(columns))
    insert_sql = f"INSERT INTO `{table_name}` VALUES ({placeholders})"
    for row in rows:
        mysql_cursor.execute(insert_sql, tuple(row))


def get_primary_key(sqlserver_cursor, table_name):
    """获取表的主键列"""
    sqlserver_cursor.execute(f"""
        SELECT kcu.COLUMN_NAME
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS kcu
        ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
        WHERE tc.TABLE_NAME = '{table_name}' AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
    """)
    return [row.COLUMN_NAME for row in sqlserver_cursor.fetchall()]


def get_foreign_keys(sqlserver_cursor, table_name):
    """获取表的外键列及引用信息"""
    sqlserver_cursor.execute(f"""
        SELECT 
            kcu.COLUMN_NAME, 
            ccu.TABLE_NAME AS REFERENCED_TABLE_NAME,
            ccu.COLUMN_NAME AS REFERENCED_COLUMN_NAME
        FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS AS rc
        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS kcu 
            ON rc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
        JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE AS ccu 
            ON rc.UNIQUE_CONSTRAINT_NAME = ccu.CONSTRAINT_NAME
        WHERE kcu.TABLE_NAME = '{table_name}'
    """)
    return [(row.COLUMN_NAME, row.REFERENCED_TABLE_NAME, row.REFERENCED_COLUMN_NAME) for row in
            sqlserver_cursor.fetchall()]


def create_mysql_table_with_keys(mysql_cursor, table_name, columns, primary_key, foreign_keys):
    """在 MySQL 中创建表，包含主键和外键"""
    column_definitions = []
    for column_name, data_type, length in columns:
        mysql_type = map_sqlserver_type_to_mysql(data_type, length)
        column_definitions.append(f"`{column_name}` {mysql_type}")

    # 添加主键约束
    if primary_key:
        column_definitions.append(f"PRIMARY KEY (`{primary_key[0]}`)")

    # 添加外键约束
    for fk_column, ref_table, ref_column in foreign_keys:
        column_definitions.append(
            f"FOREIGN KEY (`{fk_column}`) REFERENCES `{ref_table}`(`{ref_column}`)"
        )

    column_definitions_str = ", ".join(column_definitions)
    create_table_sql = f"CREATE TABLE IF NOT EXISTS `{table_name}` ({column_definitions_str})"
    mysql_cursor.execute(create_table_sql)


def migrate_database():
    """迁移 SQL Server 数据库到 MySQL"""
    sqlserver_conn = get_sqlserver_connection()
    sqlserver_cursor = sqlserver_conn.cursor()

    mysql_conn = get_mysql_connection()
    mysql_cursor = mysql_conn.cursor()

    # 获取数据库和表结构
    database_name = sqlserver_config['database'].lower()
    create_mysql_database(mysql_cursor, database_name)
    tables = get_sqlserver_tables(sqlserver_cursor)

    for table_name in tables:
        columns = get_table_schema(sqlserver_cursor, table_name)
        primary_key = get_primary_key(sqlserver_cursor, table_name)
        foreign_keys = get_foreign_keys(sqlserver_cursor, table_name)
        print(f"\n正在创建表: [{table_name}]...")
        print(f"Create table: [{table_name}]...")
        create_mysql_table_with_keys(mysql_cursor, table_name, columns, primary_key, foreign_keys)
        # create_mysql_table(mysql_cursor, table_name, columns)
        print(f"\n正在迁移表: [{table_name}]...")
        print(f"Migrating table: [{table_name}]...")
        transfer_data(sqlserver_cursor, mysql_cursor, table_name, columns)
        mysql_conn.commit()

    # 关闭连接
    sqlserver_cursor.close()
    sqlserver_conn.close()
    mysql_cursor.close()
    mysql_conn.close()
    print("\n=========================\n")
    print("数据库迁移已成功完成。")
    print("Database migration completed successfully.")


if __name__ == "__main__":
    migrate_database()
