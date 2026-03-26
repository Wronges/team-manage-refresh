import json
import logging
from typing import Any, Dict

from sqlalchemy.ext.asyncio import AsyncSession

from app.models import ShopOrder
from app.services.fulfillment import fulfillment_service
from app.services.payment_alipay import alipay_payment_service
from app.services.shop import SHOP_STATUS_PAYING
from app.utils.time_utils import get_now

logger = logging.getLogger(__name__)


class PaymentService:
    async def create_provider_payment(
        self,
        db_session: AsyncSession,
        *,
        order: ShopOrder,
        provider: str,
    ) -> Dict[str, Any]:
        if provider != "alipay":
            return {"success": False, "error": "暂不支持该支付渠道"}

        subject = f"{order.product.name if order.product else '兑换码商品'}-{order.order_no}"
        result = await alipay_payment_service.create_payment(
            db_session,
            order_no=order.order_no,
            amount_cents=order.amount_cents,
            subject=subject[:120],
        )
        if not result.get("success"):
            return result

        order.payment_provider = provider
        order.payment_status = "pending"
        order.status = SHOP_STATUS_PAYING
        order.payment_order_no = order.payment_order_no or f"PAY-{order.order_no}"
        order.payment_created_at = get_now()
        order.payment_payload = json.dumps(result.get("payload") or {}, ensure_ascii=False)
        await db_session.commit()
        return result

    async def handle_alipay_notify(
        self,
        db_session: AsyncSession,
        *,
        order: ShopOrder,
        payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        verify_result = await alipay_payment_service.verify_callback(db_session, payload)
        if not verify_result.get("success"):
            return verify_result

        await fulfillment_service.mark_paid(
            db_session,
            order,
            payment_provider="alipay",
            provider_trade_no=verify_result.get("provider_trade_no"),
            provider_buyer_id=verify_result.get("provider_buyer_id"),
            paid_amount_cents=verify_result.get("paid_amount_cents"),
            payment_payload=json.dumps(payload, ensure_ascii=False),
            payment_notified=True,
        )
        return await fulfillment_service.fulfill_shop_order(
            db_session,
            order,
            trigger="alipay_notify",
        )


payment_service = PaymentService()
