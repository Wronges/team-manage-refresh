from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.services.payment_service import payment_service
from app.services.shop import shop_service

router = APIRouter(prefix="/payment", tags=["payment"])


@router.post("/alipay/create/{order_no}")
async def create_alipay_payment(order_no: str, db: AsyncSession = Depends(get_db)):
    order = await shop_service.get_order_by_no(db, order_no)
    if not order:
        return JSONResponse(status_code=404, content={"success": False, "error": "订单不存在"})
    result = await payment_service.create_provider_payment(
        db,
        order=order,
        provider="alipay",
    )
    if not result.get("success"):
        return JSONResponse(status_code=400, content=result)
    return JSONResponse(content=result)


@router.post("/alipay/notify")
async def alipay_notify(request: Request, db: AsyncSession = Depends(get_db)):
    form = await request.form()
    payload = dict(form)
    order_no = str(payload.get("out_trade_no") or "").strip()
    if not order_no:
        return PlainTextResponse("failure", status_code=400)

    order = await shop_service.get_order_by_no(db, order_no)
    if not order:
        return PlainTextResponse("failure", status_code=404)

    result = await payment_service.handle_alipay_notify(
        db,
        order=order,
        payload=payload,
    )
    if result.get("success"):
        return PlainTextResponse("success")
    return PlainTextResponse("failure", status_code=400)


@router.get("/alipay/return")
async def alipay_return():
    return JSONResponse(
        content={
            "success": True,
            "message": "已返回系统，请以异步回调或订单状态为准确认支付结果",
        }
    )
