"""
Token 正则匹配工具
用于从文本中提取 AT Token、邮箱、Account ID 等信息
"""
import csv
import re
from typing import List, Optional, Dict, Tuple
import logging

from app.utils.jwt_parser import JWTParser

logger = logging.getLogger(__name__)


class TokenParser:
    """Token 正则匹配解析器"""

    # JWT Token 正则 (以 eyJ 开头的 Base64 字符串)
    # 简化匹配逻辑，三段式 Base64，Header 以 eyJ 开头
    JWT_PATTERN = r'eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+'

    # 邮箱正则 (更通用的邮箱格式)
    EMAIL_PATTERN = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'

    # Account ID 正则 (UUID 格式)
    ACCOUNT_ID_PATTERN = r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'

    # Refresh Token 正则 (支持 rt- 或 rt_ 前缀,且包含点号)
    REFRESH_TOKEN_PATTERN = r'rt[_-][A-Za-z0-9._-]+'
    
    # Session Token 正则 (通常比较长，包含两个点)
    SESSION_TOKEN_PATTERN = r'eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]*\.[A-Za-z0-9_-]+(?:\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+)?'

    # Client ID 正则 (严格匹配 app_ 开头)
    CLIENT_ID_PATTERN = r'app_[A-Za-z0-9]+'

    def __init__(self):
        self.jwt_parser = JWTParser()

    def _looks_like_access_token(self, token: str) -> bool:
        return bool(self.jwt_parser.extract_email(token))

    def _select_jwt_fields(self, jwt_candidates: List[str]) -> Tuple[Optional[str], Optional[str]]:
        if not jwt_candidates:
            return None, None

        if len(jwt_candidates) == 1:
            candidate = jwt_candidates[0]
            if self._looks_like_access_token(candidate):
                return candidate, None
            return None, candidate

        access_token = next((token for token in jwt_candidates if self._looks_like_access_token(token)), None)
        if access_token:
            session_token = next((token for token in jwt_candidates if token != access_token), None)
            return access_token, session_token

        return jwt_candidates[0], jwt_candidates[1]

    def _parse_comma_separated_line(self, line: str) -> Optional[Dict[str, Optional[str]]]:
        if "," not in line:
            return None

        parts = [part.strip() for part in next(csv.reader([line]))]
        if len(parts) < 2:
            return None

        email = parts[0] if parts and re.fullmatch(self.EMAIL_PATTERN, parts[0]) else None
        token = parts[1] if len(parts) > 1 and re.fullmatch(self.JWT_PATTERN, parts[1]) else None
        refresh_token = (
            parts[2]
            if len(parts) > 2 and re.fullmatch(self.REFRESH_TOKEN_PATTERN, parts[2])
            else None
        )
        session_token = (
            parts[3]
            if len(parts) > 3 and re.fullmatch(self.SESSION_TOKEN_PATTERN, parts[3])
            else None
        )
        client_id = (
            parts[4]
            if len(parts) > 4 and re.fullmatch(self.CLIENT_ID_PATTERN, parts[4])
            else None
        )

        account_id = None
        for part in parts[5:]:
            if re.fullmatch(self.ACCOUNT_ID_PATTERN, part, re.IGNORECASE):
                account_id = part
                break

        if not account_id:
            account_ids = re.findall(self.ACCOUNT_ID_PATTERN, line, re.IGNORECASE)
            account_id = account_ids[0] if account_ids else None

        has_auth_material = bool(token or session_token or refresh_token)
        if not has_auth_material and not email:
            return None

        return {
            "token": token,
            "email": email,
            "account_id": account_id,
            "refresh_token": refresh_token,
            "session_token": session_token,
            "client_id": client_id,
        }

    def parse_team_import_text(self, text: str) -> List[Dict[str, Optional[str]]]:
        """
        解析 Team 导入文本,提取 AT、邮箱、Account ID
        优先解析 [email]----[jwt]----[uuid] 等结构化格式

        Args:
            text: 导入的文本内容

        Returns:
            解析结果列表,每个元素包含 token, email, account_id
        """
        results = []

        # 按行分割文本
        lines = text.strip().split('\n')

        for line in lines:
            line = line.strip()
            if not line:
                continue

            comma_parsed = self._parse_comma_separated_line(line)
            if comma_parsed:
                if (
                    comma_parsed["token"]
                    or comma_parsed["session_token"]
                    or comma_parsed["refresh_token"]
                ):
                    results.append(comma_parsed)
                continue

            token = None
            email = None
            account_id = None
            refresh_token = None
            session_token = None
            client_id = None
            jwt_candidates = []

            # 1. 尝试使用分隔符解析 (支持 ----, | , \t, 以及多个空格)
            parts = [p.strip() for p in re.split(r'----|\||\t|\s{2,}', line) if p.strip()]
            
            if len(parts) >= 2:
                # 根据格式特征自动识别各部分
                for part in parts:
                    if re.fullmatch(self.JWT_PATTERN, part) or re.fullmatch(self.SESSION_TOKEN_PATTERN, part):
                        jwt_candidates.append(part)
                    elif not email and re.fullmatch(self.EMAIL_PATTERN, part):
                        email = part
                    elif not account_id and re.fullmatch(self.ACCOUNT_ID_PATTERN, part, re.IGNORECASE):
                        account_id = part
                    elif not refresh_token and re.fullmatch(self.REFRESH_TOKEN_PATTERN, part):
                        refresh_token = part
                    elif not client_id and re.fullmatch(self.CLIENT_ID_PATTERN, part):
                        client_id = part

                token, session_token = self._select_jwt_fields(jwt_candidates)

            # 2. 如果结构化解析未找到 Token，尝试全局正则提取结果 (兜底逻辑)
            if not token and not session_token:
                tokens = re.findall(self.SESSION_TOKEN_PATTERN, line)
                token, session_token = self._select_jwt_fields(tokens)
                
                # 只有在非结构化情况下才全局提取其他信息
                if not email:
                    emails = re.findall(self.EMAIL_PATTERN, line)
                    email = emails[0] if emails else None
                if not account_id:
                    account_ids = re.findall(self.ACCOUNT_ID_PATTERN, line, re.IGNORECASE)
                    account_id = account_ids[0] if account_ids else None
                if not refresh_token:
                    rts = re.findall(self.REFRESH_TOKEN_PATTERN, line)
                    refresh_token = rts[0] if rts else None
                if not client_id:
                    cids = re.findall(self.CLIENT_ID_PATTERN, line)
                    client_id = cids[0] if cids else None

            if token or session_token or refresh_token:
                results.append({
                    "token": token,
                    "email": email,
                    "account_id": account_id,
                    "refresh_token": refresh_token,
                    "session_token": session_token,
                    "client_id": client_id
                })

        logger.info(f"解析完成,共提取 {len(results)} 条 Team 信息")
        return results
