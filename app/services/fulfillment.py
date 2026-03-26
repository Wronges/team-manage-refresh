import logging
from typing import Any, Dict, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from app.models import ShopOrder
from app.services.redemption import redemption_service
from app.utils.time_utils import get_now

logger = logging.getLogger(__name__)


class FulfillmentService:
    async def fulfill_shop_order(
        self,
        db_session: AsyncSession,
        order: ShopOrder,
        *,
        trigger: str,
        admin_note: str = "",
    ) -> Dict[str, Any]:
        if not order:
            return {"success": False, "error": "订单不存在"}

        if order.assigned_code and order.status == "fulfilled":
            return {"success": True, "assigned_code": order.assigned_code, "message": "订单已履约"}

        product = order.product
        if not product:
            return {"success": False, "error": "商品不存在"}
        if product.inventory <= 0:
            order.fulfillment_error = "商品库存不足"
            order.fulfillment_attempts = int(order.fulfillment_attempts or 0) + 1
            await db_session.commit()
            return {"success": False, "error": "商品库存不足"}

        code_result = await redemption_service.generate_code_single(
            db_session=db_session,
            expires_days=product.expires_days,
            has_warranty=product.has_warranty,
            warranty_days=product.warranty_days,
            pool_type="normal",
        )
        if not code_result.get("success"):
            order.fulfillment_error = code_result.get("error") or "生成兑换码失败"
            order.fulfillment_attempts = int(order.fulfillment_attempts or 0) + 1
            await db_session.commit()
            return {"success": False, "error": order.fulfillment_error}

        product.inventory = max(0, int(product.inventory) - 1)
        order.assigned_code = code_result["code"]
        order.status = "fulfilled"
        order.payment_status = "paid"
        now = get_now()
        order.fulfilled_at = now
        order.completed_at = now
        order.paid_at = order.paid_at or now
        order.fulfillment_error = None
        order.fulfillment_attempts = int(order.fulfillment_attempts or 0) + 1
        if admin_note.strip():
            order.admin_note = admin_note.strip()

        logger.info("shop order fulfilled: order=%s trigger=%s code=%s", order.order_no, trigger, order.assigned_code)
        await db_session.commit()
        return {
            "success": True,
            "assigned_code": order.assigned_code,
            "message": "订单已完成履约",
        }

    async def mark_paid(
        self,
        db_session: AsyncSession,
        order: ShopOrder,
        *,
        payment_provider: str,
        provider_trade_no: Optional[str] = None,
        provider_buyer_id: Optional[str] = None,
        paid_amount_cents: Optional[int] = None,
        payment_payload: Optional[str] = None,
        payment_notified: bool = False,
    ) -> Dict[str, Any]:
        if not order:
            return {"success": False, "error": "订单不存在"}

        if order.payment_status == "paid" and order.status in {"paid", "fulfilled"}:
            return {"success": True, "message": "订单已标记支付成功"}

        now = get_now()
        order.payment_provider = payment_provider
        order.payment_status = "paid"
        order.status = "paid"
        order.provider_trade_no = provider_trade_no or order.provider_trade_no
        order.provider_buyer_id = provider_buyer_id or order.provider_buyer_id
        order.paid_amount_cents = paid_amount_cents if paid_amount_cents is not None else order.paid_amount_cents
        order.payment_payload = payment_payload or order.payment_payload
        order.paid_at = order.paid_at or now
        if payment_notified:
            order.payment_notified_at = now
        await db_session.commit()
        return {"success": True}


fulfillment_service = FulfillmentService()
