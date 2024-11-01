# SQL2MySync
一键将SqlServer数据迁移到MySQL，自动在MySQL创建同名数据库，并保留主键和外键信息。

### 环境依赖
- Python3
- `pip install pyodbc pymysql`

### 开始使用
1. 配置数据库连接:

   将`run.py`中第8-20行，修改为自己的 SqlServer 和 mysql 数据库连接信息。其中 `sqlserver_config["database"]` 为SqlServer中需要迁移的数据库名，迁移过程中会自动在MySQL中创建同名数据库（字母统一转小写）
    ```python
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
    ```

2. 运行代码

    `python run.py`
    ```bash
    正在创建数据库: [xxxx]...
    Create database: [xxxx]...
    
    正在创建表: [zzz]...
    Create table: [zzz]...
    
    正在迁移表: [zzz]...
    Migrating table: [zzz]...
    
    =========================
    
    数据库迁移已成功完成。
    Database migration completed successfully.
    ```
