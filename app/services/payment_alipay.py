import json
import logging
from typing import Any, Dict, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from app.services.settings import settings_service

logger = logging.getLogger(__name__)


class AlipayPaymentService:
    async def load_config(self, db_session: AsyncSession) -> Dict[str, str]:
        return {
            "enabled": str(await settings_service.get_setting(db_session, "payment_alipay_enabled", "false")).lower(),
            "app_id": (await settings_service.get_setting(db_session, "payment_alipay_app_id", "") or "").strip(),
            "gateway": (await settings_service.get_setting(db_session, "payment_alipay_gateway", "https://openapi.alipay.com/gateway.do") or "").strip(),
            "notify_url": (await settings_service.get_setting(db_session, "payment_alipay_notify_url", "") or "").strip(),
            "return_url": (await settings_service.get_setting(db_session, "payment_alipay_return_url", "") or "").strip(),
            "merchant_private_key": (await settings_service.get_setting(db_session, "payment_alipay_private_key", "") or "").strip(),
            "alipay_public_key": (await settings_service.get_setting(db_session, "payment_alipay_public_key", "") or "").strip(),
        }

    async def create_payment(
        self,
        db_session: AsyncSession,
        *,
        order_no: str,
        amount_cents: int,
        subject: str,
    ) -> Dict[str, Any]:
        config = await self.load_config(db_session)
        enabled = config["enabled"] in {"1", "true", "yes", "on"}
        required = ["app_id", "notify_url", "return_url", "merchant_private_key", "alipay_public_key"]
        missing = [key for key in required if not config.get(key)]
        if not enabled or missing:
            return {
                "success": False,
                "error": "支付宝自动支付未配置完成",
                "missing_fields": missing,
            }

        payload = {
            "provider": "alipay",
            "order_no": order_no,
            "amount_yuan": f"{(amount_cents or 0) / 100:.2f}",
            "subject": subject,
            "gateway": config["gateway"],
            "notify_url": config["notify_url"],
            "return_url": config["return_url"],
            "mode": "skeleton",
        }
        return {
            "success": True,
            "payment_url": "",
            "qr_content": "",
            "payload": payload,
        }

    async def verify_callback(self, db_session: AsyncSession, payload: Dict[str, Any]) -> Dict[str, Any]:
        config = await self.load_config(db_session)
        enabled = config["enabled"] in {"1", "true", "yes", "on"}
        if not enabled:
            return {"success": False, "error": "支付宝自动支付未启用"}
        return {
            "success": False,
            "error": "支付宝验签骨架已接入，待配置正式 SDK/验签逻辑",
            "raw_payload": json.dumps(payload, ensure_ascii=False),
        }


alipay_payment_service = AlipayPaymentService()
