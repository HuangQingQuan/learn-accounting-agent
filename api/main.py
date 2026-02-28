"""
FastAPI主应用
提供账务审核智能体的REST API服务
"""

from fastapi import FastAPI, HTTPException, UploadFile, File, BackgroundTasks, Depends, Query, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse, FileResponse, StreamingResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional, Union
import pandas as pd
import logging
import asyncio
import io
import tempfile
import os
from datetime import datetime, timedelta
from pathlib import Path
import uuid
import json

# 导入项目模块
from agents.accounting_agent import AccountingAgent
from agents.batch_processor import BatchProcessor, BatchConfig
from agents.utils.report_generator import ReportGenerator, ReportConfig, ReportData, generate_audit_report
from agents.utils.db import get_audit_record_manager, get_task_execution_manager
from skills.impl.data_parse import parse_account_data
from skills.impl.rule_check import rule_check_skill
from skills.impl.anomaly_detect import anomaly_detect_skill
from skills.impl.llm_explain import llm_explain_skill

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 创建FastAPI应用
app = FastAPI(
    title="Learn Accounting Agent API",
    description="智能账务审核系统API",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# 添加中间件
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 生产环境中应该限制具体域名
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(GZipMiddleware, minimum_size=1000)

# 安全认证
security = HTTPBearer(auto_error=False)

# 全局变量
agent_instance = None
batch_processors = {}  # 存储批量处理器实例
task_status = {}  # 存储任务状态


# Pydantic模型定义
class AuditRequest(BaseModel):
    """审核请求模型"""
    task_type: str = Field(..., description="任务类型", example="rule_check")
    config: Optional[Dict[str, Any]] = Field(None, description="任务配置")
    options: Optional[Dict[str, Any]] = Field(None, description="额外选项")


class BatchAuditRequest(BaseModel):
    """批量审核请求模型"""
    task_types: List[str] = Field(..., description="任务类型列表", example=["rule_check", "anomaly_detect"])
    batch_config: Optional[Dict[str, Any]] = Field(None, description="批量处理配置")
    notification_url: Optional[str] = Field(None, description="完成通知URL")


class ReportRequest(BaseModel):
    """报告生成请求模型"""
    report_config: Optional[Dict[str, Any]] = Field(None, description="报告配置")
    formats: List[str] = Field(default=["html", "excel"], description="报告格式列表")
    data_filter: Optional[Dict[str, Any]] = Field(None, description="数据过滤条件")


class ApiResponse(BaseModel):
    """API响应模型"""
    success: bool = Field(..., description="是否成功")
    message: str = Field(..., description="响应消息")
    data: Optional[Any] = Field(None, description="响应数据")
    timestamp: datetime = Field(default_factory=datetime.now, description="响应时间")


class TaskStatus(BaseModel):
    """任务状态模型"""
    task_id: str = Field(..., description="任务ID")
    status: str = Field(..., description="任务状态")
    progress: Optional[float] = Field(None, description="进度百分比")
    start_time: datetime = Field(..., description="开始时间")
    end_time: Optional[datetime] = Field(None, description="结束时间")
    result: Optional[Dict[str, Any]] = Field(None, description="任务结果")
    error: Optional[str] = Field(None, description="错误信息")


# 依赖注入函数
def get_agent() -> AccountingAgent:
    """获取智能体实例"""
    global agent_instance
    if agent_instance is None:
        agent_instance = AccountingAgent()
        # 注册技能
        agent_instance.register_skill("data_parse", parse_account_data)
        agent_instance.register_skill("rule_check", rule_check_skill)
        agent_instance.register_skill("anomaly_detect", anomaly_detect_skill)
        agent_instance.register_skill("llm_explain", llm_explain_skill)
        logger.info("智能体实例已初始化")
    return agent_instance


async def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """验证访问令牌"""
    # 这里可以实现真实的token验证逻辑
    # 目前为了演示，允许所有请求
    return credentials


def create_response(success: bool, message: str, data: Any = None) -> ApiResponse:
    """创建标准API响应"""
    return ApiResponse(
        success=success,
        message=message,
        data=data,
        timestamp=datetime.now()
    )


# 启动事件
@app.on_event("startup")
async def startup_event():
    """应用启动事件"""
    logger.info("Learn Accounting Agent API 启动中...")
    
    # 创建必要的目录
    os.makedirs("uploads", exist_ok=True)
    os.makedirs("reports", exist_ok=True)
    os.makedirs("batch_results", exist_ok=True)
    
    logger.info("API 启动完成")


@app.on_event("shutdown")
async def shutdown_event():
    """应用关闭事件"""
    logger.info("API 正在关闭...")


# API路由定义

@app.get("/", response_model=ApiResponse)
async def root():
    """根路径"""
    return create_response(
        success=True,
        message="Learn Accounting Agent API is running",
        data={
            "version": "1.0.0",
            "docs": "/docs",
            "health": "/health"
        }
    )


@app.get("/health", response_model=ApiResponse)
async def health_check():
    """健康检查"""
    try:
        # 检查数据库连接
        audit_manager = get_audit_record_manager()
        stats = audit_manager.get_audit_statistics()
        
        return create_response(
            success=True,
            message="系统运行正常",
            data={
                "status": "healthy",
                "database": "connected",
                "total_records": stats.get("total_records", 0)
            }
        )
    except Exception as e:
        logger.error(f"健康检查失败: {e}")
        return create_response(
            success=False,
            message=f"系统异常: {str(e)}",
            data={"status": "unhealthy"}
        )


@app.post("/api/v1/upload", response_model=ApiResponse)
async def upload_file(file: UploadFile = File(...)):
    """上传文件"""
    try:
        # 检查文件类型
        allowed_extensions = ['.xlsx', '.xls', '.csv', '.json']
        file_extension = Path(file.filename).suffix.lower()
        
        if file_extension not in allowed_extensions:
            raise HTTPException(
                status_code=400,
                detail=f"不支持的文件类型: {file_extension}"
            )
        
        # 保存文件
        file_id = str(uuid.uuid4())
        file_path = Path("uploads") / f"{file_id}{file_extension}"
        
        with open(file_path, "wb") as buffer:
            content = await file.read()
            buffer.write(content)
        
        logger.info(f"文件上传成功: {file.filename} -> {file_path}")
        
        return create_response(
            success=True,
            message="文件上传成功",
            data={
                "file_id": file_id,
                "original_name": file.filename,
                "file_path": str(file_path),
                "file_size": len(content)
            }
        )
        
    except Exception as e:
        logger.error(f"文件上传失败: {e}")
        raise HTTPException(status_code=500, detail=f"文件上传失败: {str(e)}")


@app.post("/api/v1/audit/single", response_model=ApiResponse)
async def audit_single_file(
    file_id: str = Body(..., description="文件ID"),
    audit_request: AuditRequest = Body(...),
    token: HTTPAuthorizationCredentials = Depends(verify_token)
):
    """单文件审核"""
    try:
        # 查找文件
        file_path = None
        for ext in ['.xlsx', '.xls', '.csv', '.json']:
            potential_path = Path("uploads") / f"{file_id}{ext}"
            if potential_path.exists():
                file_path = potential_path
                break
        
        if not file_path:
            raise HTTPException(status_code=404, detail="文件不存在")
        
        # 获取智能体
        agent = get_agent()
        
        # 解析数据
        logger.info(f"开始解析文件: {file_path}")
        data = parse_account_data(str(file_path))
        
        # 执行审核任务
        logger.info(f"执行审核任务: {audit_request.task_type}")
        result = agent.run(audit_request.task_type, data, **(audit_request.config or {}))
        
        # 保存审核记录
        from agents.utils.db import AuditRecord, save_audit_record
        audit_record = AuditRecord(
            record_id=file_id,
            task_type=audit_request.task_type,
            data_source=str(file_path),
            passed=getattr(result, 'passed', True),
            risk_level=getattr(result, 'risk_level', 'low'),
            rule_results=getattr(result, 'rule_results', []),
            anomaly_results=getattr(result, 'anomaly_results', []),
            explanation=getattr(result, 'explanation', None),
            suggestions=getattr(result, 'suggestions', [])
        )
        
        record_id = save_audit_record(audit_record)
        
        return create_response(
            success=True,
            message="审核完成",
            data={
                "record_id": record_id,
                "task_type": audit_request.task_type,
                "result": result,
                "data_summary": {
                    "total_records": len(data),
                    "columns": list(data.columns)
                }
            }
        )
        
    except Exception as e:
        logger.error(f"单文件审核失败: {e}")
        raise HTTPException(status_code=500, detail=f"审核失败: {str(e)}")


@app.post("/api/v1/audit/batch", response_model=ApiResponse)
async def audit_batch_files(
    background_tasks: BackgroundTasks,
    file_id: str = Body(..., description="文件ID"),
    batch_request: BatchAuditRequest = Body(...),
    token: HTTPAuthorizationCredentials = Depends(verify_token)
):
    """批量文件审核"""
    try:
        # 查找文件
        file_path = None
        for ext in ['.xlsx', '.xls', '.csv', '.json']:
            potential_path = Path("uploads") / f"{file_id}{ext}"
            if potential_path.exists():
                file_path = potential_path
                break
        
        if not file_path:
            raise HTTPException(status_code=404, detail="文件不存在")
        
        # 创建任务ID
        task_id = str(uuid.uuid4())
        
        # 初始化任务状态
        task_status[task_id] = TaskStatus(
            task_id=task_id,
            status="pending",
            start_time=datetime.now()
        )
        
        # 添加后台任务
        background_tasks.add_task(
            process_batch_audit,
            task_id,
            str(file_path),
            batch_request
        )
        
        return create_response(
            success=True,
            message="批量审核任务已启动",
            data={
                "task_id": task_id,
                "status": "pending",
                "estimated_time": "根据数据量而定"
            }
        )
        
    except Exception as e:
        logger.error(f"批量审核启动失败: {e}")
        raise HTTPException(status_code=500, detail=f"批量审核启动失败: {str(e)}")


async def process_batch_audit(task_id: str, file_path: str, batch_request: BatchAuditRequest):
    """后台批量审核处理"""
    try:
        # 更新任务状态
        task_status[task_id].status = "running"
        
        # 获取智能体
        agent = get_agent()
        
        # 解析数据
        logger.info(f"批量审核开始解析文件: {file_path}")
        data = parse_account_data(file_path)
        
        # 创建批量处理器
        batch_config = BatchConfig(**(batch_request.batch_config or {}))
        processor = BatchProcessor(agent, batch_config)
        
        # 添加进度回调
        def progress_callback(progress):
            task_status[task_id].progress = progress.get_progress_percentage()
            
        processor.add_progress_callback(progress_callback)
        
        # 执行批量处理
        logger.info(f"开始批量处理，任务类型: {batch_request.task_types}")
        result = processor.process_batch_sequential(data, batch_request.task_types)
        
        # 更新任务状态
        task_status[task_id].status = "completed"
        task_status[task_id].end_time = datetime.now()
        task_status[task_id].result = result
        
        # 发送通知（如果提供了通知URL）
        if batch_request.notification_url:
            await send_completion_notification(batch_request.notification_url, task_id, result)
            
        logger.info(f"批量审核任务完成: {task_id}")
        
    except Exception as e:
        logger.error(f"批量审核任务失败: {e}")
        task_status[task_id].status = "failed"
        task_status[task_id].end_time = datetime.now()
        task_status[task_id].error = str(e)


async def send_completion_notification(notification_url: str, task_id: str, result: Dict[str, Any]):
    """发送完成通知"""
    try:
        import httpx
        
        notification_data = {
            "task_id": task_id,
            "status": "completed",
            "result_summary": {
                "total_records": result.get("total_records", 0),
                "processed_records": result.get("processed_records", 0),
                "failed_records": result.get("failed_records", 0)
            }
        }
        
        async with httpx.AsyncClient() as client:
            response = await client.post(notification_url, json=notification_data)
            logger.info(f"通知发送结果: {response.status_code}")
            
    except Exception as e:
        logger.error(f"发送通知失败: {e}")


@app.get("/api/v1/tasks/{task_id}/status", response_model=ApiResponse)
async def get_task_status(task_id: str):
    """获取任务状态"""
    if task_id not in task_status:
        raise HTTPException(status_code=404, detail="任务不存在")
    
    status = task_status[task_id]
    return create_response(
        success=True,
        message="任务状态获取成功",
        data=status.dict()
    )


@app.get("/api/v1/tasks", response_model=ApiResponse)
async def list_tasks(
    status_filter: Optional[str] = Query(None, description="状态过滤"),
    limit: int = Query(10, description="返回数量限制")
):
    """获取任务列表"""
    tasks = []
    
    for task_id, status in task_status.items():
        if status_filter is None or status.status == status_filter:
            tasks.append(status.dict())
    
    # 按开始时间倒序排列
    tasks.sort(key=lambda x: x["start_time"], reverse=True)
    
    return create_response(
        success=True,
        message="任务列表获取成功",
        data={
            "tasks": tasks[:limit],
            "total": len(tasks)
        }
    )


@app.post("/api/v1/reports/generate", response_model=ApiResponse)
async def generate_report(
    report_request: ReportRequest = Body(...),
    token: HTTPAuthorizationCredentials = Depends(verify_token)
):
    """生成审核报告"""
    try:
        # 获取审核记录管理器
        audit_manager = get_audit_record_manager()
        
        # 应用过滤条件
        start_date = None
        end_date = None
        if report_request.data_filter:
            if "start_date" in report_request.data_filter:
                start_date = datetime.fromisoformat(report_request.data_filter["start_date"])
            if "end_date" in report_request.data_filter:
                end_date = datetime.fromisoformat(report_request.data_filter["end_date"])
        
        # 获取统计数据
        stats = audit_manager.get_audit_statistics(start_date, end_date)
        
        # 获取详细记录
        records = audit_manager.get_audit_records_by_filter(
            start_date=start_date,
            end_date=end_date,
            limit=1000
        )
        
        # 准备报告数据
        from agents.utils.report_generator import ReportData
        report_data = ReportData(
            summary=stats,
            details=pd.DataFrame([record.__dict__ for record in records])
        )
        
        # 生成报告
        report_config = ReportConfig(**(report_request.report_config or {}))
        generator = ReportGenerator(report_config)
        
        reports = generator.generate_comprehensive_report(report_data, report_request.formats)
        
        return create_response(
            success=True,
            message="报告生成成功",
            data={
                "reports": reports,
                "summary": stats
            }
        )
        
    except Exception as e:
        logger.error(f"报告生成失败: {e}")
        raise HTTPException(status_code=500, detail=f"报告生成失败: {str(e)}")


@app.get("/api/v1/reports/{report_id}/download")
async def download_report(report_id: str):
    """下载报告文件"""
    try:
        # 查找报告文件
        report_path = None
        for ext in ['.html', '.pdf', '.xlsx', '.docx', '.json']:
            potential_path = Path("reports") / f"{report_id}{ext}"
            if potential_path.exists():
                report_path = potential_path
                break
        
        if not report_path:
            raise HTTPException(status_code=404, detail="报告文件不存在")
        
        return FileResponse(
            path=str(report_path),
            filename=report_path.name,
            media_type='application/octet-stream'
        )
        
    except Exception as e:
        logger.error(f"报告下载失败: {e}")
        raise HTTPException(status_code=500, detail=f"报告下载失败: {str(e)}")


@app.get("/api/v1/statistics/overview", response_model=ApiResponse)
async def get_statistics_overview():
    """获取统计概览"""
    try:
        audit_manager = get_audit_record_manager()
        
        # 获取总体统计
        overall_stats = audit_manager.get_audit_statistics()
        
        # 获取最近30天的统计
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)
        recent_stats = audit_manager.get_audit_statistics(start_date, end_date)
        
        # 获取风险分布
        risk_distribution = overall_stats.get("risk_distribution", {})
        
        # 获取任务类型分布
        task_distribution = overall_stats.get("task_distribution", {})
        
        return create_response(
            success=True,
            message="统计概览获取成功",
            data={
                "overall": overall_stats,
                "recent_30_days": recent_stats,
                "risk_distribution": risk_distribution,
                "task_distribution": task_distribution
            }
        )
        
    except Exception as e:
        logger.error(f"统计概览获取失败: {e}")
        raise HTTPException(status_code=500, detail=f"统计概览获取失败: {str(e)}")


@app.get("/api/v1/records", response_model=ApiResponse)
async def get_audit_records(
    page: int = Query(1, ge=1, description="页码"),
    size: int = Query(20, ge=1, le=100, description="每页大小"),
    risk_level: Optional[str] = Query(None, description="风险等级过滤"),
    task_type: Optional[str] = Query(None, description="任务类型过滤"),
    start_date: Optional[str] = Query(None, description="开始日期"),
    end_date: Optional[str] = Query(None, description="结束日期")
):
    """获取审核记录列表"""
    try:
        audit_manager = get_audit_record_manager()
        
        # 解析日期
        start_dt = datetime.fromisoformat(start_date) if start_date else None
        end_dt = datetime.fromisoformat(end_date) if end_date else None
        
        # 获取记录
        records = audit_manager.get_audit_records_by_filter(
            start_date=start_dt,
            end_date=end_dt,
            risk_level=risk_level,
            task_type=task_type,
            limit=size * page  # 获取足够的数据用于分页
        )
        
        # 分页处理
        total_records = len(records)
        start_idx = (page - 1) * size
        end_idx = start_idx + size
        page_records = records[start_idx:end_idx]
        
        return create_response(
            success=True,
            message="审核记录获取成功",
            data={
                "records": [record.__dict__ for record in page_records],
                "pagination": {
                    "page": page,
                    "size": size,
                    "total": total_records,
                    "pages": (total_records + size - 1) // size
                }
            }
        )
        
    except Exception as e:
        logger.error(f"审核记录获取失败: {e}")
        raise HTTPException(status_code=500, detail=f"审核记录获取失败: {str(e)}")


@app.delete("/api/v1/records/{record_id}", response_model=ApiResponse)
async def delete_audit_record(record_id: int):
    """删除审核记录"""
    try:
        audit_manager = get_audit_record_manager()
        success = audit_manager.delete_audit_record(record_id)
        
        if success:
            return create_response(
                success=True,
                message="审核记录删除成功"
            )
        else:
            raise HTTPException(status_code=404, detail="记录不存在")
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"审核记录删除失败: {e}")
        raise HTTPException(status_code=500, detail=f"审核记录删除失败: {str(e)}")


# 错误处理
@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    """HTTP异常处理"""
    return JSONResponse(
        status_code=exc.status_code,
        content=create_response(
            success=False,
            message=exc.detail,
            data={"status_code": exc.status_code}
        ).dict()
    )


@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    """通用异常处理"""
    logger.error(f"未处理的异常: {exc}")
    return JSONResponse(
        status_code=500,
        content=create_response(
            success=False,
            message="内部服务器错误",
            data={"error": str(exc)}
        ).dict()
    )


# 开发环境启动
if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
