import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, EmailStr, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.services.settings import settings_service
from app.services.shop import (
    SHOP_STATUS_COMPLETED,
    shop_service,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/shop", tags=["shop"])


class CreateOrderRequest(BaseModel):
    product_id: int = Field(..., ge=1)
    customer_email: EmailStr
    customer_note: str = Field("", max_length=500)


class SubmitPaymentRequest(BaseModel):
    payment_method: str = Field(..., pattern="^(alipay|wechat)$")
    payment_reference: str = Field(..., min_length=1, max_length=120)
    customer_note: str = Field("", max_length=500)


@router.get("", response_class=HTMLResponse)
async def shop_page(request: Request, db: AsyncSession = Depends(get_db)):
    from app.main import templates

    shop_settings = await shop_service.get_shop_settings(db)
    products = await shop_service.list_products(db, active_only=True)
    ui_theme = settings_service.normalize_ui_theme(await settings_service.get_setting(db, "ui_theme", "ocean"))
    return templates.TemplateResponse(
        request,
        "user/shop.html",
        {
            "products": products,
            "shop_settings": shop_settings,
            "ui_theme": ui_theme,
            "shop_enabled": shop_settings["enabled"],
            "default_warranty_days": await settings_service.get_default_warranty_days(db),
            "cents_to_yuan": shop_service.cents_to_yuan,
        },
    )


@router.get("/orders/{order_no}", response_class=HTMLResponse)
async def shop_order_page(order_no: str, request: Request, db: AsyncSession = Depends(get_db)):
    from app.main import templates

    order = await shop_service.get_order_by_no(db, order_no)
    if not order:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="订单不存在")

    shop_settings = await shop_service.get_shop_settings(db)
    ui_theme = settings_service.normalize_ui_theme(await settings_service.get_setting(db, "ui_theme", "ocean"))
    return templates.TemplateResponse(
        request,
        "user/shop_order.html",
        {
            "order": order,
            "shop_settings": shop_settings,
            "ui_theme": ui_theme,
            "shop_enabled": shop_settings["enabled"],
            "cents_to_yuan": shop_service.cents_to_yuan,
            "order_completed": order.status == SHOP_STATUS_COMPLETED,
        },
    )


@router.post("/orders")
async def create_order(payload: CreateOrderRequest, db: AsyncSession = Depends(get_db)):
    shop_settings = await shop_service.get_shop_settings(db)
    if not shop_settings["enabled"]:
        return JSONResponse(status_code=400, content={"success": False, "error": "购买功能暂未开放"})

    result = await shop_service.create_order(
        db,
        product_id=payload.product_id,
        customer_email=payload.customer_email,
        customer_note=payload.customer_note,
    )
    if not result["success"]:
        return JSONResponse(status_code=400, content=result)
    return JSONResponse(content=result)


@router.post("/orders/{order_no}/submit-payment")
async def submit_payment(order_no: str, payload: SubmitPaymentRequest, db: AsyncSession = Depends(get_db)):
    result = await shop_service.submit_payment(
        db,
        order_no=order_no,
        payment_method=payload.payment_method,
        payment_reference=payload.payment_reference,
        customer_note=payload.customer_note,
    )
    if not result["success"]:
        return JSONResponse(status_code=400, content=result)
    return JSONResponse(content=result)
