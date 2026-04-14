"""
ChatGPT API 服务模块
封装 ChatGPT 后端 API，处理 Team 管理相关操作
"""
import asyncio
import hashlib
import logging
import random
from typing import Optional, Dict, Any, Tuple
from curl_cffi.requests import AsyncSession

from app.config import settings
from app.services.settings import settings_service
from sqlalchemy.ext.asyncio import AsyncSession as DBAsyncSession
from app.utils.jwt_parser import JWTParser

logger = logging.getLogger(__name__)


class ChatGPTService:

    BASE_URL = "https://chatgpt.com/backend-api"

    # 重试配置
    MAX_RETRIES = 3
    RETRY_DELAYS = [1, 2, 4]  # 分别等待 1s, 2s, 4s

    def __init__(self):
        """Initialize the ChatGPT API service."""
        self.jwt_parser = JWTParser()
        # 按 Email 或 TeamID 隔离 CF 会话
        self._sessions: Dict[str, AsyncSession] = {}

    @staticmethod
    def _build_token_identifier(prefix: str, token_value: str) -> str:
        digest = hashlib.sha256(token_value.encode("utf-8")).hexdigest()[:12]
        return f"{prefix}_{digest}"

    @staticmethod
    def _extract_error_details(response) -> Tuple[str, Optional[str]]:
        error_msg = response.text
        error_code = None

        try:
            error_data = response.json()
        except Exception:
            return error_msg, error_code

        if not isinstance(error_data, dict):
            return error_msg, error_code

        error_info = error_data.get("error")
        detail = error_data.get("detail")

        if isinstance(error_info, dict):
            error_msg = (
                error_info.get("message")
                or error_info.get("detail")
                or (str(detail) if detail is not None else None)
                or response.text
            )
            error_code = error_info.get("code") or error_data.get("code")
        elif isinstance(error_info, str) and error_info:
            error_msg = error_info
            error_code = error_data.get("code")
        elif isinstance(detail, str) and detail:
            error_msg = detail
            error_code = error_data.get("code")
        elif detail is not None:
            error_msg = str(detail)
            error_code = error_data.get("code")
        elif isinstance(error_data.get("message"), str) and error_data.get("message"):
            error_msg = error_data["message"]
            error_code = error_data.get("code")

        if not isinstance(error_msg, str):
            error_msg = str(error_msg)

        return error_msg, error_code

    async def _get_proxy_config(self, db_session: DBAsyncSession) -> Optional[str]:
        """
        获取代理配置
        """
        proxy_config = await settings_service.get_proxy_config(db_session)
        if proxy_config["enabled"] and proxy_config["proxy"]:
            return proxy_config["proxy"]
        return None

    async def _create_session(self, db_session: DBAsyncSession) -> AsyncSession:
        """
        创建新的 HTTP 会话
        """
        proxy = await self._get_proxy_config(db_session)
        # 使用 chrome110 指纹，curl_cffi 绕过 CF 检测
        session = AsyncSession(
            impersonate="chrome110",
            proxies={"http": proxy, "https": proxy} if proxy else None,
            timeout=30,
            verify=settings.tls_verify
        )
        return session

    async def _get_session(self, db_session: DBAsyncSession, identifier: str) -> AsyncSession:
        """
        获取或创建会话
        """
        if identifier not in self._sessions:
            logger.info(f"Creating isolated session for {identifier}")
            self._sessions[identifier] = await self._create_session(db_session)
        return self._sessions[identifier]

    async def _make_request(
        self,
        method: str,
        url: str,
        headers: Dict[str, str],
        json_data: Optional[Dict[str, Any]] = None,
        db_session: Optional[DBAsyncSession] = None,
        identifier: str = "default"
    ) -> Dict[str, Any]:
        """
        发送 HTTP 请求（带 CF 绕过和重试）
        """
        # 从 Header 中提取 Token 标识
        if identifier == "default":
            # 优先用 ID 标识 Team 会话
            acc_id = headers.get("chatgpt-account-id")
            if acc_id:
                identifier = f"acc_{acc_id}"
            # 其次用 Token 邮箱
            elif "Authorization" in headers:
                token = headers["Authorization"].replace("Bearer ", "")
                email = self.jwt_parser.extract_email(token)
                if email:
                    identifier = email

        session = await self._get_session(db_session, identifier)
        
        # 设置基础请求头
        base_headers = {
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://chatgpt.com/",
            "Origin": "https://chatgpt.com",
            "Connection": "keep-alive"
        }
        for k, v in base_headers.items():
            if k not in headers:
                headers[k] = v

        for attempt in range(self.MAX_RETRIES):
            try:
                if attempt > 0:
                    delay = self.RETRY_DELAYS[attempt - 1] + random.uniform(0.5, 1.5)
                    await asyncio.sleep(delay)

                logger.info(f"[{identifier}] 发送 {method} {url} (第 {attempt + 1} 次)")

                if method == "GET":
                    response = await session.get(url, headers=headers)
                elif method == "POST":
                    response = await session.post(url, headers=headers, json=json_data)
                elif method == "DELETE":
                    response = await session.delete(url, headers=headers, json=json_data)
                else:
                    raise ValueError(f"不支持的 HTTP 方法: {method}")

                status_code = response.status_code
                logger.info(f"响应状态码: {status_code}")

                if 200 <= status_code < 300:
                    try:
                        data = response.json()
                    except Exception:
                        data = {}
                    return {"success": True, "status_code": status_code, "data": data, "error": None}

                if 400 <= status_code < 500:
                    error_msg, error_code = self._extract_error_details(response)

                    if status_code == 429:
                        if attempt < self.MAX_RETRIES - 1:
                            continue
                        logger.warning(f"请求限流 {status_code}: {error_msg}")
                        return {"success": False, "status_code": status_code, "error": error_msg, "error_code": error_code}

                    if error_code == "token_invalidated" or "token_invalidated" in str(error_msg).lower():
                        logger.warning(f"检测到 Token 已失效，清除 {identifier} 会话")
                        await self.clear_session(identifier)

                    logger.warning(f"客户端错误 {status_code}: {error_msg}")
                    return {"success": False, "status_code": status_code, "error": error_msg, "error_code": error_code}

                if status_code >= 500:
                    if attempt < self.MAX_RETRIES - 1:
                        continue
                    return {"success": False, "status_code": status_code, "error": f"服务端错误 {status_code}"}

            except Exception as e:
                logger.error(f"请求异常: {e}")
                if attempt < self.MAX_RETRIES - 1:
                    continue
                return {"success": False, "status_code": 0, "error": str(e)}

        return {"success": False, "status_code": 0, "error": "请求失败，已达最大重试次数"}

    async def send_invite(
        self,
        access_token: str,
        account_id: str,
        email: str,
        db_session: DBAsyncSession,
        identifier: str = "default"
    ) -> Dict[str, Any]:
        url = f"{self.BASE_URL}/accounts/{account_id}/invites"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {access_token}",
            "chatgpt-account-id": account_id
        }
        json_data = {"email_addresses": [email], "role": "standard-user", "resend_emails": True}
        return await self._make_request("POST", url, headers, json_data, db_session, identifier)

    async def get_members(
        self,
        access_token: str,
        account_id: str,
        db_session: DBAsyncSession,
        identifier: str = "default"
    ) -> Dict[str, Any]:
        """获取 Team 成员列表"""
        all_members = []
        offset = 0
        limit = 50
        while True:
            url = f"{self.BASE_URL}/accounts/{account_id}/users?limit={limit}&offset={offset}"
            headers = {"Authorization": f"Bearer {access_token}"}
            result = await self._make_request("GET", url, headers, db_session=db_session, identifier=identifier)
            if not result["success"]:
                return {
                    "success": False,
                    "members": [],
                    "total": 0,
                    "error": result["error"],
                    "error_code": result.get("error_code"),
                }
            data = result["data"]
            items = data.get("items", [])
            total = data.get("total", 0)
            all_members.extend(items)
            if len(all_members) >= total:
                break
            offset += limit
        return {"success": True, "members": all_members, "total": len(all_members), "error": None}

    async def get_invites(
        self,
        access_token: str,
        account_id: str,
        db_session: DBAsyncSession,
        identifier: str = "default"
    ) -> Dict[str, Any]:
        url = f"{self.BASE_URL}/accounts/{account_id}/invites"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "chatgpt-account-id": account_id
        }
        result = await self._make_request("GET", url, headers, db_session=db_session, identifier=identifier)
        if not result["success"]:
            return {
                "success": False,
                "items": [],
                "total": 0,
                "error": result["error"],
                "error_code": result.get("error_code"),
            }
        data = result["data"]
        items = data.get("items", [])
        return {"success": True, "items": items, "total": len(items), "error": None}

    async def delete_invite(
        self,
        access_token: str,
        account_id: str,
        email: str,
        db_session: DBAsyncSession,
        identifier: str = "default"
    ) -> Dict[str, Any]:
        url = f"{self.BASE_URL}/accounts/{account_id}/invites"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {access_token}",
            "chatgpt-account-id": account_id
        }
        json_data = {"email_address": email}
        return await self._make_request("DELETE", url, headers, json_data, db_session, identifier)

    async def delete_member(
        self,
        access_token: str,
        account_id: str,
        user_id: str,
        db_session: DBAsyncSession,
        identifier: str = "default"
    ) -> Dict[str, Any]:
        """删除 Team 成员"""
        url = f"{self.BASE_URL}/accounts/{account_id}/users/{user_id}"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "chatgpt-account-id": account_id
        }
        result = await self._make_request("DELETE", url, headers, db_session=db_session, identifier=identifier)
        return result

    async def toggle_beta_feature(
        self,
        access_token: str,
        account_id: str,
        feature: str,
        value: bool,
        db_session: DBAsyncSession,
        identifier: str = "default"
    ) -> Dict[str, Any]:
        """切换 Beta 功能设置"""
        url = f"{self.BASE_URL}/accounts/{account_id}/beta_features"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {access_token}",
            "chatgpt-account-id": account_id,
            "oai-language": "zh-CN",
            "sec-ch-ua-platform": '"Windows"'
        }
        json_data = {"feature": feature, "value": value}
        return await self._make_request("POST", url, headers, json_data, db_session, identifier)

    async def get_account_info(
        self,
        access_token: str,
        db_session: DBAsyncSession,
        identifier: str = "default"
    ) -> Dict[str, Any]:
        url = f"{self.BASE_URL}/accounts/check/v4-2023-04-27"
        headers = {"Authorization": f"Bearer {access_token}"}
        result = await self._make_request("GET", url, headers, db_session=db_session, identifier=identifier)
        if not result["success"]:
            return {
                "success": False,
                "accounts": [],
                "error": result["error"],
                "error_code": result.get("error_code"),
            }
        
        data = result["data"]
        accounts_data = data.get("accounts", {})
        team_accounts = []
        for aid, info in accounts_data.items():
            account = info.get("account", {})
            entitlement = info.get("entitlement", {})
            if account.get("plan_type") == "team":
                team_accounts.append({
                    "account_id": aid,
                    "name": account.get("name", ""),
                    "plan_type": "team",
                    "account_user_role": account.get("account_user_role", ""),
                    "subscription_plan": entitlement.get("subscription_plan", ""),
                    "expires_at": entitlement.get("expires_at", ""),
                    "has_active_subscription": entitlement.get("has_active_subscription", False)
                })
        return {"success": True, "accounts": team_accounts, "error": None}

    async def get_account_settings(
        self,
        access_token: str,
        account_id: str,
        db_session: DBAsyncSession,
        identifier: str = "default"
    ) -> Dict[str, Any]:
        """获取账户设置（包含 beta_settings）"""
        url = f"{self.BASE_URL}/accounts/{account_id}/settings"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "chatgpt-account-id": account_id
        }
        return await self._make_request("GET", url, headers, db_session=db_session, identifier=identifier)

    async def refresh_access_token_with_session_token(
        self,
        session_token: str,
        db_session: DBAsyncSession,
        account_id: Optional[str] = None,
        identifier: str = "default"
    ) -> Dict[str, Any]:
        """使用 session_token 刷新 AT（含指纹）"""
        url = "https://chatgpt.com/api/auth/session"
        if account_id:
            url += f"?exchange_workspace_token=true&workspace_id={account_id}&reason=setCurrentAccount"
            
        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Cookie": f"__Secure-next-auth.session-token={session_token}"
        }
        
        if identifier == "default":
            identifier = self._build_token_identifier("st", session_token)

        session = await self._get_session(db_session, identifier)
        try:
            headers["Referer"] = "https://chatgpt.com/"
            headers["Connection"] = "keep-alive"
            
            response = await session.get(url, headers=headers)
            if response.status_code == 200:
                try:
                    data = response.json()
                except Exception:
                    return {"success": False, "error": "无法解析 JSON 响应"}
                
                at = data.get("accessToken")
                st = data.get("sessionToken")
                if at:
                    return {"success": True, "access_token": at, "session_token": st}
                
                # 返回 200 但没有 token
                error_info = data.get("error")
                error_msg = (
                    error_info.get("message")
                    if isinstance(error_info, dict)
                    else data.get("detail")
                    or error_info
                    or "响应中缺少 accessToken"
                )
                error_code = error_info.get("code") if isinstance(error_info, dict) else data.get("code")
                return {"success": False, "error": str(error_msg), "error_code": error_code}
            else:
                error_msg, error_code = self._extract_error_details(response)
                return {
                    "success": False,
                    "status_code": response.status_code,
                    "error": error_msg,
                    "error_code": error_code,
                }
        except Exception as e:
            return {"success": False, "error": str(e)}

    async def refresh_access_token_with_refresh_token(
        self,
        refresh_token: str,
        client_id: str,
        db_session: DBAsyncSession,
        identifier: str = "default"
    ) -> Dict[str, Any]:
        """使用 refresh_token 刷新 AT"""
        url = "https://auth.openai.com/oauth/token"
        json_data = {
            "client_id": client_id,
            "grant_type": "refresh_token",
            "redirect_uri": "com.openai.sora://auth.openai.com/android/com.openai.sora/callback",
            "refresh_token": refresh_token
        }
        headers = {"Content-Type": "application/json"}
        
        if identifier == "default":
            identifier = self._build_token_identifier("rt", refresh_token)

        result = await self._make_request("POST", url, headers, json_data, db_session, identifier)
        if result["success"]:
            data = result.get("data", {})
            return {
                "success": True,
                "access_token": data.get("access_token"),
                "refresh_token": data.get("refresh_token"),
                "data": data
            }
        return result

    async def clear_session(self, identifier: Optional[str] = None):
        if identifier:
            if identifier in self._sessions:
                try:
                    await self._sessions[identifier].close()
                except:
                    pass
                del self._sessions[identifier]
        else:
            await self.close()

    async def close(self):
        for session in self._sessions.values():
            try:
                await session.close()
            except:
                pass
        self._sessions.clear()


# 全局单例
chatgpt_service = ChatGPTService()



