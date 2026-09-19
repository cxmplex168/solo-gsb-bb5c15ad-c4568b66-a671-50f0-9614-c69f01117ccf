"""FastAPI 入口：多人文件交接留痕（哈希链）服务。"""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from . import config, database
from .schemas import (
    ChainOut,
    ChainSummary,
    CreateChainRequest,
    HandoverRequest,
    HandoverResult,
    HealthOut,
    OfferCancelRequest,
    OfferConfirmRequest,
    OfferConfirmResult,
    OfferCreateRequest,
    OfferOut,
    OfferStatusOut,
    VerifyResult,
    WitnessDecisionRequest,
    WitnessDecisionResult,
    WitnessStatusOut,
)
from .service import (
    ChainError,
    append_handover,
    cancel_offer,
    confirm_offer,
    create_chain,
    create_offer,
    get_chain,
    get_offer,
    get_witness_status,
    list_chains,
    record_witness_decision,
    verify_chain,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 启动即完成数据库初始化（建表 / 校验目录）
    database.init_db()
    yield


app = FastAPI(
    title="文件交接留痕服务",
    description="基于 SHA-256 哈希链的多人文件交接记录、查询与完整性校验。",
    version="1.2.0",
    lifespan=lifespan,
)


@app.exception_handler(ChainError)
async def chain_error_handler(request: Request, exc: ChainError) -> JSONResponse:
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": exc.code, "detail": exc.message},
    )


@app.get("/health", response_model=HealthOut, tags=["meta"], summary="健康检查")
def health() -> HealthOut:
    return HealthOut(status="ok")


@app.post(
    "/chains",
    response_model=ChainOut,
    status_code=201,
    tags=["chains"],
    summary="创建交接链",
    description="提交文件 SHA-256 摘要与首位持有人，生成序号 0 的创世记录并返回其哈希。",
)
def api_create_chain(req: CreateChainRequest) -> ChainOut:
    return ChainOut(**create_chain(req.file_hash, req.first_holder))


@app.post(
    "/handovers",
    response_model=HandoverResult,
    status_code=201,
    tags=["chains"],
    summary="执行一次移交（原子追加）",
    description=(
        "必须携带当前链尾哈希 tail_hash、接收方 receiver 与一次性随机数 nonce。"
        "交出方由服务端按当前持有人确定。链尾不匹配或随机数重放返回 409，不产生分叉。"
    ),
)
def api_handover(req: HandoverRequest) -> HandoverResult:
    return HandoverResult(
        **append_handover(req.chain_id, req.tail_hash, req.receiver, req.nonce)
    )


@app.post(
    "/offers",
    response_model=OfferOut,
    status_code=201,
    tags=["offers"],
    summary="发起待接收移交邀约（可选 2～5 名见证人、可选撤销码）",
    description=(
        "以最新链尾 tail_hash 锁定邀约，指定 receiver、有效期（秒）与一次性确认码。"
        "服务端只保存确认码的 SHA-256 摘要，返回 offer_id 供接收方确认。"
        "可选提交 2～5 名见证人及通过阈值 witness_threshold：每位见证人获得一个"
        "独立一次性令牌（仅在本响应中明文返回一次，服务端只存其 SHA-256 摘要）。"
        "省略见证人则创建与旧版完全兼容的无见证邀约。"
        "可选提交一次性撤销码 cancellation_code：服务端只保存带 offer_id 域分隔的"
        " SHA-256 摘要，持有人此后可凭码撤销；缺省则邀约不可撤销。"
    ),
)
def api_create_offer(req: OfferCreateRequest) -> OfferOut:
    witnesses = (
        [{"name": w.name, "token": w.token} for w in req.witnesses]
        if req.witnesses
        else None
    )
    return OfferOut(
        **create_offer(
            req.chain_id,
            req.tail_hash,
            req.receiver,
            req.expires_in_seconds,
            req.confirmation_code,
            witnesses=witnesses,
            witness_threshold=req.witness_threshold,
            cancellation_code=req.cancellation_code,
        )
    )


@app.post(
    "/offers/{offer_id}/confirm",
    response_model=OfferConfirmResult,
    status_code=201,
    tags=["offers"],
    summary="确认邀约（原子追加移交事件）",
    description=(
        "接收方凭 offer_id 与一次性确认码确认。服务端在同一 SQLite 立即事务中校验"
        "邀约未过期、未使用且链尾仍匹配；见证模式下还须无见证人拒绝且同意数达到"
        "阈值，然后才追加唯一移交事件并将邀约置为 accepted。过期、重复确认、"
        "见证拒绝、同意数不足或链尾已推进均返回 409，不写事件。"
    ),
)
def api_confirm_offer(offer_id: str, req: OfferConfirmRequest) -> OfferConfirmResult:
    return OfferConfirmResult(**confirm_offer(offer_id, req.confirmation_code))


@app.post(
    "/offers/{offer_id}/cancel",
    response_model=OfferStatusOut,
    tags=["offers"],
    summary="持有人凭撤销码撤销邀约",
    description=(
        "持有人凭 offer_id 与创建时设置的一次性撤销码撤销邀约。服务端只比对"
        "带 offer_id 域分隔的 SHA-256 摘要，错码返回 403 cancel_code_mismatch。"
        "撤销与接收方确认、见证人表态在同一个 SQLite 立即事务中串行互斥："
        "仅 pending 且链尾未推进、未过期、无见证拒绝的邀约可撤销，竞争中"
        "只有其中一个终态能成功；已 accepted、rejected、expired、terminated"
        "或 cancelled 的邀约均返回 409 offer_not_cancellable 且不改链。"
        "未设置撤销码的邀约（含存量旧邀约）不可撤销。响应不含任何摘要。"
    ),
)
def api_cancel_offer(offer_id: str, req: OfferCancelRequest) -> OfferStatusOut:
    return OfferStatusOut(**cancel_offer(offer_id, req.cancellation_code))


@app.post(
    "/offers/{offer_id}/witness-decisions",
    response_model=WitnessDecisionResult,
    status_code=201,
    tags=["offers"],
    summary="见证人凭一次性令牌表态（同意 / 拒绝）",
    description=(
        "见证模式邀约的见证人凭创建时分发的独立一次性令牌表态 approved 或 rejected。"
        "服务端只保存令牌摘要，同一见证人至多表态一次（重复使用令牌返回 409，"
        "令牌错误返回 403）。任一拒绝使邀约终止；邀约过期或链尾推进后表态返回 409。"
        "响应不含令牌摘要。"
    ),
)
def api_witness_decision(
    offer_id: str, req: WitnessDecisionRequest
) -> WitnessDecisionResult:
    return WitnessDecisionResult(
        **record_witness_decision(offer_id, req.token, req.decision)
    )


@app.get(
    "/offers/{offer_id}/witnesses",
    response_model=WitnessStatusOut,
    tags=["offers"],
    summary="查询见证邀约的见证人表态状态",
    description=(
        "返回见证邀约各见证人的表态结果与同意/拒绝/待表态计数、派生邀约状态，"
        "绝不返回令牌及其摘要。无见证邀约返回 404。"
    ),
)
def api_get_witness_status(offer_id: str) -> WitnessStatusOut:
    return WitnessStatusOut(**get_witness_status(offer_id))


@app.get(
    "/offers/{offer_id}",
    response_model=OfferStatusOut,
    tags=["offers"],
    summary="查询单个邀约状态",
    description=(
        "返回邀约状态（pending / accepted / cancelled / expired / rejected / "
        "terminated，后三者为派生状态）与确认/见证结果，不含确认码、撤销码、"
        "令牌及其摘要。"
    ),
)
def api_get_offer(offer_id: str) -> OfferStatusOut:
    return OfferStatusOut(**get_offer(offer_id))


@app.get(
    "/chains",
    response_model=list[ChainSummary],
    tags=["chains"],
    summary="列出全部交接链",
)
def api_list_chains() -> list[ChainSummary]:
    return [ChainSummary(**c) for c in list_chains()]


@app.get(
    "/chains/{chain_id}",
    response_model=ChainOut,
    tags=["chains"],
    summary="查询链条（含全部事件）",
)
def api_get_chain(chain_id: str) -> ChainOut:
    return ChainOut(**get_chain(chain_id))


@app.get(
    "/chains/{chain_id}/verify",
    response_model=VerifyResult,
    tags=["chains"],
    summary="完整性校验",
    description="逐条重算哈希并校验前序指针，结果给出首个断裂序号 first_broken_seq。",
)
def api_verify_chain(chain_id: str) -> VerifyResult:
    return VerifyResult(**verify_chain(chain_id))


def main() -> None:
    """本地直接运行入口（python -m app.main）。"""
    import uvicorn

    database.init_db()
    uvicorn.run("app.main:app", host=config.HOST, port=config.PORT)


if __name__ == "__main__":
    main()
