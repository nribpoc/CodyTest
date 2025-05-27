import psycopg2
from psycopg2 import sql

# 数据库连接参数
db_params = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': '123456',
    'host': 'localhost',
    'port': '5432'
}

def create_connection():
    """创建数据库连接"""
    try:
        # 建立连接
        conn = psycopg2.connect(**db_params)
        conn.autocommit = True
        return conn
    except (Exception, psycopg2.Error) as error:
        print("连接数据库时出错:", error)
        return None

def create_customers_table(conn):
    """创建 customers 表"""
    try:
        # 创建游标
        cursor = conn.cursor()
        
        # 创建表的 SQL 语句
        create_table_query = """
        CREATE TABLE IF NOT EXISTS customers (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            email VARCHAR(100) UNIQUE NOT NULL
        )
        """
        
        # 执行创建表的语句
        cursor.execute(create_table_query)
        
        print("customers 表创建成功")
        
        # 关闭游标
        cursor.close()
    except (Exception, psycopg2.Error) as error:
        print("创建表时出错:", error)

def insert_sample_data(conn):
    """向 customers 表插入示例数据"""
    try:
        cursor = conn.cursor()
        
        # 示例数据
        sample_customers = [
            ('张三', 'zhangsan@email.com'),
            ('李四', 'lisi@email.com'),
            ('王五', 'wangwu@email.com'),
            ('赵六', 'zhaoliu@email.com'),
            ('钱七', 'qianqi@email.com')
        ]
        
        # 插入数据的 SQL 语句
        insert_query = """
        INSERT INTO customers (name, email) 
        VALUES (%s, %s)
        ON CONFLICT (email) DO NOTHING
        """
        
        # 批量插入数据
        cursor.executemany(insert_query, sample_customers)
        
        # 获取插入的行数
        print(f"成功插入 {cursor.rowcount} 条数据到 customers 表")
        
        cursor.close()
    except (Exception, psycopg2.Error) as error:
        print("插入数据时出错:", error)

def display_customers(conn):
    """显示 customers 表中的所有数据"""
    try:
        cursor = conn.cursor()
        
        # 查询所有数据
        select_query = "SELECT id, name, email FROM customers ORDER BY id"
        cursor.execute(select_query)
        
        # 获取所有记录
        records = cursor.fetchall()
        
        print("\n=== customers 表中的数据 ===")
        print("ID\t姓名\t\t邮箱")
        print("-" * 50)
        
        for record in records:
            print(f"{record[0]}\t{record[1]}\t\t{record[2]}")
        
        print(f"\n总共 {len(records)} 条记录")
        
        cursor.close()
    except (Exception, psycopg2.Error) as error:
        print("查询数据时出错:", error)

def main():
    # 创建数据库连接
    conn = create_connection()
    
    if conn is not None:
        # 创建 customers 表
        create_customers_table(conn)
        
        # 插入示例数据
        insert_sample_data(conn)
        
        # 显示表中的数据
        display_customers(conn)
        
        # 关闭数据库连接
        conn.close()
        print("\n数据库连接已关闭")

if __name__ == "__main__":
    main()