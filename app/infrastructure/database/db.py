import logging
from datetime import datetime, timezone
from typing import Any

from app.bot.enums.roles import UserRole
from psycopg import AsyncConnection

logger = logging.getLogger()

async def add_user(
        conn: AsyncConnection,
        *,
        user_id: int,
        username: str,
        language: str = "ru",
        role: UserRole = UserRole.USER,
        is_alive: bool = True,
        banned: bool = False,
) -> None:
    async with conn.cursor() as cursor:
        await cursor.execute(
            query="""
                INSERT INTO users(user_id, username, language, role, is_alive, banned)
                VALUES(
                    %(user_id)s,
                    %(username)s,
                    %(language)s,
                    %(role)s,
                    %(is_alive)s,
                    %(banned)s
                ) ON CONFLICT DO NOTHING;
                """,
                params={
                    "user_id": user_id,
                    "username": username,
                    "language": language,
                    "role": role,
                    "is_alive": is_alive,
                    "banned": banned,
                },       
        )
    logger.info(
        "User added. Table='%s', user_id=%d, created_at='%s', "
        "language='%s', role=%s, is_alive=%s, banned=%s",
        "users",
        user_id,
        datetime.now(timezone.utc),
        language,
        role,
        is_alive,
        banned,
    )

async def get_user(
        conn: AsyncConnection,
        *,
        user_id: int,
) -> tuple[Any, ...] | None:
    async with conn.cursor() as cursor:
        data = await cursor.execute(
            query="""
               SELECT
                    id,
                    user_id,
                    username,
                    language,
                    role,
                    is_alive,
                    banned,
                    created_at
                    FROM users WHERE user_id = %s; 
                """,
                params=(user_id,),
        )
        row = await  data.fetchone()
    logger.info("Row is %s", row)
    return row if row else None

async def change_user_alive_status(
        conn: AsyncConnection,
        *,
        is_alive: bool,
        user_id: int,
) -> None:
    async with conn.cursor() as cursor:
        data = await cursor.execute(
            query="""
                 SELECT is_alive 
                 FROM users 
                 WHERE user_id = %s;
                """,
            params=(user_id,),
        )
        row = await data.fetchone()
    if row:
        logger.info("The user with `user_id`=%s has the is_alive status is %s", user_id, row[0])
    else:
        logger.warning("No user with `user_id`=%s found in the database", user_id)
    return row[0] if row else None

async def get_user_banned_status_by_id(
    conn: AsyncConnection,
    *,
    user_id: int,
) -> bool | None:
    async with conn.cursor() as cursor:
        data = await cursor.execute(
            query="""
                SELECT banned 
                FROM users 
                WHERE user_id = %s;
            """,
            params=(user_id,),
        )
        row = await data.fetchone()
    if row:
        logger.info("The user with `user_id`=%s has the banned status is %s", user_id, row[0])
    else:
        logger.warning("No user with `user_id`=%s found in the database", user_id)
    return row[0] if row else None


async def get_user_banned_status_by_username(
    conn: AsyncConnection,
    *,
    username: str,
) -> bool | None:
    async with conn.cursor() as cursor:
        data = await cursor.execute(
            query="""
                SELECT banned 
                FROM users 
                WHERE username = %s;
            """,
            params=(username,),
        )
        row = await data.fetchone()
    if row:
        logger.info("The user with `username`=%s has the banned status is %s", username, row[0])
    else:
        logger.warning("No user with `username`=%s found in the database", username)
    return row[0] if row else None


async def get_user_role(
    conn: AsyncConnection,
    *,
    user_id: int,
) -> UserRole | None:
    async with conn.cursor() as cursor:
        data = await cursor.execute(
            query="""
                SELECT role 
                FROM users 
                WHERE user_id = %s;
            """,
            params=(user_id,),
        )
        row = await data.fetchone()
    if row:
        logger.info("The user with `user_id`=%s has the role is %s", user_id, row[0])
    else:
        logger.warning("No user with `user_id`=%s found in the database", user_id)
    return UserRole(row[0]) if row else None


async def add_user_activity(
    conn: AsyncConnection,
    *,
    user_id: int,
) -> None:
    async with conn.cursor() as cursor:
        await cursor.execute(
            query="""
                INSERT INTO activity (user_id)
                VALUES (%s)
                ON CONFLICT (user_id, activity_date)
                DO UPDATE
                SET actions = activity.actions + 1;
            """,
            params=(user_id,),
        )
    logger.info("User activity updated. table=`activity`, user_id=%d", user_id)


async def get_statistics(conn: AsyncConnection) -> list[Any, ...] | None:
    async with conn.cursor() as cursor:
        data = await cursor.execute(
            query="""
                SELECT user_id, SUM(actions) AS total_actions
                FROM activity
                GROUP BY user_id
                ORDER BY total_actions DESC
                LIMIT 5;
            """,
        )
        rows = await data.fetchall()
    logger.info("Users activity got from table=`activity`")
    return [*rows] if rows else None        