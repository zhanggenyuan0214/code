"""
数据库初始化脚本
执行与应用启动一致的数据库 bootstrap 流程
"""
import asyncio

from app.bootstrap import bootstrap_application
from app.database import close_db


async def main():
    """主函数"""
    print("开始初始化数据库...")

    try:
        result = await bootstrap_application()
    finally:
        await close_db()

    print("共享初始化流程执行完成")
    if result.created_settings:
        print(f"已补齐默认设置: {', '.join(result.created_settings)}")
    else:
        print("默认设置已存在")

    if result.removed_legacy_settings:
        print(f"已清理兼容遗留设置: {', '.join(result.removed_legacy_settings)}")

    print("数据库初始化完成!")


if __name__ == "__main__":
    asyncio.run(main())
