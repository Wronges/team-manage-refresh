"""
数据库自动迁移模块
在应用启动时自动检测并执行必要的数据库迁移
"""
import logging
import sqlite3
from pathlib import Path
from datetime import datetime

logger = logging.getLogger(__name__)


def get_db_path():
    """获取数据库文件路径"""
    from app.config import settings
    db_file = settings.database_url.split("///")[-1]
    return Path(db_file)


def column_exists(cursor, table_name, column_name):
    """检查表中是否存在指定列"""
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = [row[1] for row in cursor.fetchall()]
    return column_name in columns


def table_exists(cursor, table_name):
    """检查表是否存在"""
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,)
    )
    return cursor.fetchone() is not None


def run_auto_migration():
    """
    自动运行数据库迁移
    检测缺失的列并自动添加
    """
    db_path = get_db_path()
    
    if not db_path.exists():
        logger.info("数据库文件不存在，跳过迁移")
        return
    
    logger.info("开始检查数据库迁移...")
    
    try:
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()
        
        migrations_applied = []
        
        # 检查并添加质保相关字段
        if not column_exists(cursor, "redemption_codes", "has_warranty"):
            logger.info("添加 redemption_codes.has_warranty 字段")
            cursor.execute("""
                ALTER TABLE redemption_codes 
                ADD COLUMN has_warranty BOOLEAN DEFAULT 0
            """)
            migrations_applied.append("redemption_codes.has_warranty")
        
        if not column_exists(cursor, "redemption_codes", "warranty_expires_at"):
            logger.info("添加 redemption_codes.warranty_expires_at 字段")
            cursor.execute("""
                ALTER TABLE redemption_codes 
                ADD COLUMN warranty_expires_at DATETIME
            """)
            migrations_applied.append("redemption_codes.warranty_expires_at")
        
        if not column_exists(cursor, "redemption_codes", "warranty_days"):
            logger.info("添加 redemption_codes.warranty_days 字段")
            cursor.execute("""
                ALTER TABLE redemption_codes 
                ADD COLUMN warranty_days INTEGER DEFAULT 30
            """)
            migrations_applied.append("redemption_codes.warranty_days")
        
        if not column_exists(cursor, "redemption_records", "is_warranty_redemption"):
            logger.info("添加 redemption_records.is_warranty_redemption 字段")
            cursor.execute("""
                ALTER TABLE redemption_records 
                ADD COLUMN is_warranty_redemption BOOLEAN DEFAULT 0
            """)
            migrations_applied.append("redemption_records.is_warranty_redemption")

        # 检查并添加 Token 刷新相关字段
        if not column_exists(cursor, "teams", "refresh_token_encrypted"):
            logger.info("添加 teams.refresh_token_encrypted 字段")
            cursor.execute("ALTER TABLE teams ADD COLUMN refresh_token_encrypted TEXT")
            migrations_applied.append("teams.refresh_token_encrypted")

        if not column_exists(cursor, "teams", "id_token_encrypted"):
            logger.info("添加 teams.id_token_encrypted 字段")
            cursor.execute("ALTER TABLE teams ADD COLUMN id_token_encrypted TEXT")
            migrations_applied.append("teams.id_token_encrypted")

        if not column_exists(cursor, "teams", "session_token_encrypted"):
            logger.info("添加 teams.session_token_encrypted 字段")
            cursor.execute("ALTER TABLE teams ADD COLUMN session_token_encrypted TEXT")
            migrations_applied.append("teams.session_token_encrypted")

        if not column_exists(cursor, "teams", "client_id"):
            logger.info("添加 teams.client_id 字段")
            cursor.execute("ALTER TABLE teams ADD COLUMN client_id VARCHAR(100)")
            migrations_applied.append("teams.client_id")

        if not column_exists(cursor, "teams", "error_count"):
            logger.info("添加 teams.error_count 字段")
            cursor.execute("ALTER TABLE teams ADD COLUMN error_count INTEGER DEFAULT 0")
            migrations_applied.append("teams.error_count")

        if not column_exists(cursor, "teams", "account_role"):
            logger.info("添加 teams.account_role 字段")
            cursor.execute("ALTER TABLE teams ADD COLUMN account_role VARCHAR(50)")
            migrations_applied.append("teams.account_role")

        if not column_exists(cursor, "teams", "device_code_auth_enabled"):
            logger.info("添加 teams.device_code_auth_enabled 字段")
            cursor.execute("ALTER TABLE teams ADD COLUMN device_code_auth_enabled BOOLEAN DEFAULT 0")
            migrations_applied.append("teams.device_code_auth_enabled")
        

        if not column_exists(cursor, "teams", "pool_type"):
            logger.info("添加 teams.pool_type 字段")
            cursor.execute("ALTER TABLE teams ADD COLUMN pool_type VARCHAR(20) DEFAULT 'normal'")
            migrations_applied.append("teams.pool_type")

        if not column_exists(cursor, "redemption_codes", "pool_type"):
            logger.info("添加 redemption_codes.pool_type 字段")
            cursor.execute("ALTER TABLE redemption_codes ADD COLUMN pool_type VARCHAR(20) DEFAULT 'normal'")
            migrations_applied.append("redemption_codes.pool_type")

        if not column_exists(cursor, "redemption_codes", "reusable_by_seat"):
            logger.info("添加 redemption_codes.reusable_by_seat 字段")
            cursor.execute("ALTER TABLE redemption_codes ADD COLUMN reusable_by_seat BOOLEAN DEFAULT 0")
            migrations_applied.append("redemption_codes.reusable_by_seat")

        if not table_exists(cursor, "team_email_mappings"):
            logger.info("创建 team_email_mappings 表")
            cursor.execute("""
                CREATE TABLE team_email_mappings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    team_id INTEGER NOT NULL,
                    email VARCHAR(255) NOT NULL,
                    status VARCHAR(20) NOT NULL DEFAULT 'invited',
                    source VARCHAR(20) NOT NULL DEFAULT 'sync',
                    last_seen_at DATETIME,
                    missing_sync_count INTEGER NOT NULL DEFAULT 0,
                    created_at DATETIME,
                    updated_at DATETIME,
                    FOREIGN KEY(team_id) REFERENCES teams(id) ON DELETE CASCADE
                )
            """)
            migrations_applied.append("team_email_mappings")

        if table_exists(cursor, "team_email_mappings") and not column_exists(cursor, "team_email_mappings", "missing_sync_count"):
            logger.info("添加 team_email_mappings.missing_sync_count 字段")
            cursor.execute("""
                ALTER TABLE team_email_mappings
                ADD COLUMN missing_sync_count INTEGER NOT NULL DEFAULT 0
            """)
            migrations_applied.append("team_email_mappings.missing_sync_count")

        cursor.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_team_email_unique
            ON team_email_mappings (team_id, email)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_team_email_email
            ON team_email_mappings (email)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_team_email_status
            ON team_email_mappings (team_id, status)
        """)

        if not table_exists(cursor, "shop_products"):
            logger.info("create shop_products")
            cursor.execute("""
                CREATE TABLE shop_products (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name VARCHAR(120) NOT NULL,
                    description TEXT,
                    badge VARCHAR(50),
                    price_cents INTEGER NOT NULL DEFAULT 0,
                    inventory INTEGER NOT NULL DEFAULT 0,
                    sort_order INTEGER NOT NULL DEFAULT 0,
                    is_active BOOLEAN DEFAULT 1,
                    has_warranty BOOLEAN DEFAULT 0,
                    warranty_days INTEGER DEFAULT 30,
                    expires_days INTEGER,
                    created_at DATETIME,
                    updated_at DATETIME
                )
            """)
            migrations_applied.append("shop_products")

        if not table_exists(cursor, "shop_orders"):
            logger.info("create shop_orders")
            cursor.execute("""
                CREATE TABLE shop_orders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    order_no VARCHAR(40) NOT NULL UNIQUE,
                    product_id INTEGER NOT NULL,
                    customer_email VARCHAR(255) NOT NULL,
                    amount_cents INTEGER NOT NULL DEFAULT 0,
                    status VARCHAR(30) NOT NULL DEFAULT 'pending_payment',
                    payment_method VARCHAR(20),
                    payment_reference VARCHAR(120),
                    customer_note TEXT,
                    admin_note TEXT,
                    assigned_code VARCHAR(32),
                    created_at DATETIME,
                    updated_at DATETIME,
                    paid_at DATETIME,
                    completed_at DATETIME,
                    FOREIGN KEY(product_id) REFERENCES shop_products(id),
                    FOREIGN KEY(assigned_code) REFERENCES redemption_codes(code)
                )
            """)
            migrations_applied.append("shop_orders")

        shop_order_columns = {
            "payment_provider": "ALTER TABLE shop_orders ADD COLUMN payment_provider VARCHAR(20)",
            "payment_status": "ALTER TABLE shop_orders ADD COLUMN payment_status VARCHAR(20) NOT NULL DEFAULT 'unpaid'",
            "payment_order_no": "ALTER TABLE shop_orders ADD COLUMN payment_order_no VARCHAR(50)",
            "provider_trade_no": "ALTER TABLE shop_orders ADD COLUMN provider_trade_no VARCHAR(100)",
            "provider_buyer_id": "ALTER TABLE shop_orders ADD COLUMN provider_buyer_id VARCHAR(100)",
            "paid_amount_cents": "ALTER TABLE shop_orders ADD COLUMN paid_amount_cents INTEGER",
            "payment_payload": "ALTER TABLE shop_orders ADD COLUMN payment_payload TEXT",
            "payment_error": "ALTER TABLE shop_orders ADD COLUMN payment_error TEXT",
            "payment_created_at": "ALTER TABLE shop_orders ADD COLUMN payment_created_at DATETIME",
            "payment_notified_at": "ALTER TABLE shop_orders ADD COLUMN payment_notified_at DATETIME",
            "fulfilled_at": "ALTER TABLE shop_orders ADD COLUMN fulfilled_at DATETIME",
            "fulfillment_error": "ALTER TABLE shop_orders ADD COLUMN fulfillment_error TEXT",
            "fulfillment_attempts": "ALTER TABLE shop_orders ADD COLUMN fulfillment_attempts INTEGER NOT NULL DEFAULT 0",
        }
        for column_name, ddl in shop_order_columns.items():
            if table_exists(cursor, "shop_orders") and not column_exists(cursor, "shop_orders", column_name):
                logger.info("add shop_orders.%s", column_name)
                cursor.execute(ddl)
                migrations_applied.append(f"shop_orders.{column_name}")

        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_shop_product_active_sort
            ON shop_products (is_active, sort_order)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_shop_order_status_created
            ON shop_orders (status, created_at)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_shop_order_email
            ON shop_orders (customer_email)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_shop_order_payment_order_no
            ON shop_orders (payment_order_no)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_shop_order_provider_trade_no
            ON shop_orders (provider_trade_no)
        """)

        # 提交更改
        conn.commit()
        
        if migrations_applied:
            logger.info(f"数据库迁移完成，应用了 {len(migrations_applied)} 个迁移: {', '.join(migrations_applied)}")
        else:
            logger.info("数据库已是最新版本，无需迁移")
        
        conn.close()
        
    except Exception as e:
        logger.error(f"数据库迁移失败: {e}")
        raise


if __name__ == "__main__":
    # 允许直接运行此脚本进行迁移
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    run_auto_migration()
    print("迁移完成")
