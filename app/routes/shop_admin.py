import logging
from typing import Optional

from fastapi import APIRouter, Depends, Query, Request, status
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.dependencies.auth import require_admin
from app.services.settings import settings_service
from app.services.shop import shop_service

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/shop", tags=["admin-shop"])


class ProductSaveRequest(BaseModel):
    product_id: Optional[int] = None
    name: str = Field(..., min_length=1, max_length=120)
    description: str = Field("", max_length=3000)
    badge: str = Field("", max_length=50)
    price_cents: int = Field(..., ge=0)
    inventory: int = Field(..., ge=0)
    sort_order: int = Field(0, ge=0)
    is_active: bool = True
    has_warranty: bool = False
    warranty_days: int = Field(30, ge=1, le=3650)
    expires_days: Optional[int] = Field(None, ge=1, le=3650)


class PaymentSettingsRequest(BaseModel):
    shop_enabled: bool = True
    alipay_qr_url: str = ""
    wechat_qr_url: str = ""
    payment_notice: str = ""
    contact_notice: str = ""


class OrderActionRequest(BaseModel):
    admin_note: str = Field("", max_length=1000)


async def resolve_ui_theme(db: AsyncSession) -> str:
    return settings_service.normalize_ui_theme(
        await settings_service.get_setting(db, "ui_theme", "ocean")
    )


@router.get("/products", response_class=HTMLResponse)
async def products_page(request: Request, db: AsyncSession = Depends(get_db), current_user: dict = Depends(require_admin)):
    from app.main import templates

    products = await shop_service.list_products(db, active_only=False)
    return templates.TemplateResponse(
        request,
        "admin/shop/products.html",
        {
            "user": current_user,
            "active_page": "shop_products",
            "ui_theme": await resolve_ui_theme(db),
            "products": products,
            "cents_to_yuan": shop_service.cents_to_yuan,
        },
    )


@router.get("/orders", response_class=HTMLResponse)
async def orders_page(
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(require_admin),
    status_filter: Optional[str] = Query(None, alias="status"),
):
    from app.main import templates

    orders = await shop_service.list_orders(db, status_filter)
    return templates.TemplateResponse(
        request,
        "admin/shop/orders.html",
        {
            "user": current_user,
            "active_page": "shop_orders",
            "ui_theme": await resolve_ui_theme(db),
            "orders": orders,
            "status_filter": status_filter or "",
            "cents_to_yuan": shop_service.cents_to_yuan,
        },
    )


@router.post("/products/save")
async def save_product(payload: ProductSaveRequest, db: AsyncSession = Depends(get_db), current_user: dict = Depends(require_admin)):
    result = await shop_service.save_product(
        db,
        product_id=payload.product_id,
        name=payload.name,
        description=payload.description,
        badge=payload.badge,
        price_cents=payload.price_cents,
        inventory=payload.inventory,
        sort_order=payload.sort_order,
        is_active=payload.is_active,
        has_warranty=payload.has_warranty,
        warranty_days=payload.warranty_days,
        expires_days=payload.expires_days,
    )
    if not result["success"]:
        return JSONResponse(status_code=400, content=result)
    return JSONResponse(content=result)


@router.post("/products/{product_id}/toggle")
async def toggle_product(product_id: int, db: AsyncSession = Depends(get_db), current_user: dict = Depends(require_admin)):
    result = await shop_service.toggle_product(db, product_id)
    if not result["success"]:
        return JSONResponse(status_code=400, content=result)
    return JSONResponse(content=result)


@router.post("/products/{product_id}/delete")
async def delete_product(product_id: int, db: AsyncSession = Depends(get_db), current_user: dict = Depends(require_admin)):
    result = await shop_service.delete_product(db, product_id)
    if not result["success"]:
        return JSONResponse(status_code=400, content=result)
    return JSONResponse(content=result)


@router.post("/orders/{order_id}/approve")
async def approve_order(order_id: int, payload: OrderActionRequest, db: AsyncSession = Depends(get_db), current_user: dict = Depends(require_admin)):
    result = await shop_service.approve_order(db, order_id=order_id, admin_note=payload.admin_note)
    if not result["success"]:
        return JSONResponse(status_code=400, content=result)
    return JSONResponse(content=result)


@router.post("/orders/{order_id}/reject")
async def reject_order(order_id: int, payload: OrderActionRequest, db: AsyncSession = Depends(get_db), current_user: dict = Depends(require_admin)):
    result = await shop_service.reject_order(db, order_id=order_id, admin_note=payload.admin_note)
    if not result["success"]:
        return JSONResponse(status_code=400, content=result)
    return JSONResponse(content=result)


@router.post("/settings/payment")
async def save_payment_settings(payload: PaymentSettingsRequest, db: AsyncSession = Depends(get_db), current_user: dict = Depends(require_admin)):
    success = await settings_service.update_settings(
        db,
        {
            "shop_enabled": str(payload.shop_enabled).lower(),
            "shop_alipay_qr_url": payload.alipay_qr_url.strip(),
            "shop_wechat_qr_url": payload.wechat_qr_url.strip(),
            "shop_payment_notice": payload.payment_notice.strip(),
            "shop_contact_notice": payload.contact_notice.strip(),
        },
    )
    if not success:
        return JSONResponse(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, content={"success": False, "error": "保存失败"})
    return JSONResponse(content={"success": True, "message": "支付配置已保存"})
