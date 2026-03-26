import logging
import secrets
from datetime import datetime
from typing import Any, Dict, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.models import ShopOrder, ShopProduct
from app.services.redemption import redemption_service
from app.services.settings import settings_service
from app.utils.time_utils import get_now

logger = logging.getLogger(__name__)


SHOP_STATUS_PENDING_PAYMENT = "pending_payment"
SHOP_STATUS_PENDING_REVIEW = "pending_review"
SHOP_STATUS_COMPLETED = "completed"
SHOP_STATUS_REJECTED = "rejected"
SHOP_STATUS_CANCELLED = "cancelled"


class ShopService:
    @staticmethod
    def _generate_order_no() -> str:
        now = datetime.now().strftime("%Y%m%d%H%M%S")
        suffix = secrets.token_hex(3).upper()
        return f"SHOP{now}{suffix}"

    @staticmethod
    def cents_to_yuan(amount_cents: int) -> str:
        return f"{(amount_cents or 0) / 100:.2f}"

    async def get_shop_settings(self, db_session: AsyncSession) -> Dict[str, Any]:
        enabled_raw = await settings_service.get_setting(db_session, "shop_enabled", "true")
        return {
            "enabled": str(enabled_raw).lower() in {"1", "true", "yes", "on"},
            "alipay_qr_url": (await settings_service.get_setting(db_session, "shop_alipay_qr_url", "") or "").strip(),
            "wechat_qr_url": (await settings_service.get_setting(db_session, "shop_wechat_qr_url", "") or "").strip(),
            "payment_notice": (await settings_service.get_setting(db_session, "shop_payment_notice", "") or "").strip(),
            "contact_notice": (await settings_service.get_setting(db_session, "shop_contact_notice", "") or "").strip(),
        }

    async def list_products(self, db_session: AsyncSession, active_only: bool = False) -> list[ShopProduct]:
        stmt = select(ShopProduct).order_by(ShopProduct.sort_order.asc(), ShopProduct.id.desc())
        if active_only:
            stmt = stmt.where(ShopProduct.is_active == True)  # noqa: E712
        result = await db_session.execute(stmt)
        return list(result.scalars().all())

    async def get_product(self, db_session: AsyncSession, product_id: int) -> Optional[ShopProduct]:
        result = await db_session.execute(select(ShopProduct).where(ShopProduct.id == product_id))
        return result.scalar_one_or_none()

    async def save_product(
        self,
        db_session: AsyncSession,
        *,
        product_id: Optional[int],
        name: str,
        description: str,
        badge: str,
        price_cents: int,
        inventory: int,
        sort_order: int,
        is_active: bool,
        has_warranty: bool,
        warranty_days: int,
        expires_days: Optional[int],
    ) -> Dict[str, Any]:
        try:
            product = None
            if product_id:
                product = await self.get_product(db_session, product_id)
            if product is None:
                product = ShopProduct()
                db_session.add(product)

            product.name = name.strip()
            product.description = description.strip()
            product.badge = badge.strip() or None
            product.price_cents = max(0, int(price_cents))
            product.inventory = max(0, int(inventory))
            product.sort_order = int(sort_order)
            product.is_active = bool(is_active)
            product.has_warranty = bool(has_warranty)
            product.warranty_days = max(1, int(warranty_days))
            product.expires_days = int(expires_days) if expires_days else None

            await db_session.commit()
            return {"success": True, "product_id": product.id}
        except Exception as exc:
            await db_session.rollback()
            logger.exception("save product failed")
            return {"success": False, "error": str(exc)}

    async def toggle_product(self, db_session: AsyncSession, product_id: int) -> Dict[str, Any]:
        product = await self.get_product(db_session, product_id)
        if not product:
            return {"success": False, "error": "商品不存在"}
        product.is_active = not bool(product.is_active)
        await db_session.commit()
        return {"success": True, "is_active": bool(product.is_active)}

    async def delete_product(self, db_session: AsyncSession, product_id: int) -> Dict[str, Any]:
        product = await self.get_product(db_session, product_id)
        if not product:
            return {"success": False, "error": "商品不存在"}
        order_count = await db_session.execute(select(ShopOrder.id).where(ShopOrder.product_id == product_id))
        if order_count.first():
            return {"success": False, "error": "该商品已有订单，不能直接删除"}
        await db_session.delete(product)
        await db_session.commit()
        return {"success": True}

    async def create_order(
        self,
        db_session: AsyncSession,
        *,
        product_id: int,
        customer_email: str,
        customer_note: str = "",
    ) -> Dict[str, Any]:
        product = await self.get_product(db_session, product_id)
        if not product or not product.is_active:
            return {"success": False, "error": "商品不存在或已下架"}
        if product.inventory <= 0:
            return {"success": False, "error": "商品库存不足"}

        order = ShopOrder(
            order_no=self._generate_order_no(),
            product_id=product.id,
            customer_email=customer_email.strip().lower(),
            amount_cents=product.price_cents,
            customer_note=customer_note.strip(),
            status=SHOP_STATUS_PENDING_PAYMENT,
        )
        db_session.add(order)
        await db_session.commit()
        return {"success": True, "order_no": order.order_no}

    async def get_order_by_no(self, db_session: AsyncSession, order_no: str) -> Optional[ShopOrder]:
        result = await db_session.execute(
            select(ShopOrder)
            .options(selectinload(ShopOrder.product))
            .where(ShopOrder.order_no == order_no)
        )
        return result.scalar_one_or_none()

    async def get_order_by_id(self, db_session: AsyncSession, order_id: int) -> Optional[ShopOrder]:
        result = await db_session.execute(
            select(ShopOrder)
            .options(selectinload(ShopOrder.product))
            .where(ShopOrder.id == order_id)
        )
        return result.scalar_one_or_none()

    async def submit_payment(
        self,
        db_session: AsyncSession,
        *,
        order_no: str,
        payment_method: str,
        payment_reference: str,
        customer_note: str = "",
    ) -> Dict[str, Any]:
        order = await self.get_order_by_no(db_session, order_no)
        if not order:
            return {"success": False, "error": "订单不存在"}
        if order.status == SHOP_STATUS_COMPLETED:
            return {"success": False, "error": "订单已完成"}

        order.payment_method = payment_method
        order.payment_reference = payment_reference.strip()
        if customer_note.strip():
            order.customer_note = customer_note.strip()
        order.status = SHOP_STATUS_PENDING_REVIEW
        order.paid_at = get_now()
        await db_session.commit()
        return {"success": True}

    async def list_orders(self, db_session: AsyncSession, status: Optional[str] = None) -> list[ShopOrder]:
        stmt = (
            select(ShopOrder)
            .options(selectinload(ShopOrder.product))
            .order_by(ShopOrder.created_at.desc(), ShopOrder.id.desc())
        )
        if status:
            stmt = stmt.where(ShopOrder.status == status)
        result = await db_session.execute(stmt)
        return list(result.scalars().all())

    async def approve_order(self, db_session: AsyncSession, *, order_id: int, admin_note: str = "") -> Dict[str, Any]:
        order = await self.get_order_by_id(db_session, order_id)
        if not order:
            return {"success": False, "error": "订单不存在"}
        if order.status == SHOP_STATUS_COMPLETED:
            return {"success": False, "error": "订单已完成"}

        product = order.product
        if not product:
            return {"success": False, "error": "商品不存在"}
        if product.inventory <= 0:
            return {"success": False, "error": "商品库存不足"}

        code_result = await redemption_service.generate_code_single(
            db_session=db_session,
            expires_days=product.expires_days,
            has_warranty=product.has_warranty,
            warranty_days=product.warranty_days,
            pool_type="normal",
        )
        if not code_result.get("success"):
            return {"success": False, "error": code_result.get("error") or "生成兑换码失败"}

        product.inventory = max(0, int(product.inventory) - 1)
        order.assigned_code = code_result["code"]
        order.status = SHOP_STATUS_COMPLETED
        order.completed_at = get_now()
        if not order.paid_at:
            order.paid_at = get_now()
        order.admin_note = admin_note.strip()
        await db_session.commit()
        return {"success": True, "assigned_code": order.assigned_code}

    async def reject_order(self, db_session: AsyncSession, *, order_id: int, admin_note: str = "") -> Dict[str, Any]:
        order = await self.get_order_by_id(db_session, order_id)
        if not order:
            return {"success": False, "error": "订单不存在"}
        if order.status == SHOP_STATUS_COMPLETED:
            return {"success": False, "error": "订单已完成，不能驳回"}
        order.status = SHOP_STATUS_REJECTED
        order.admin_note = admin_note.strip()
        await db_session.commit()
        return {"success": True}


shop_service = ShopService()
