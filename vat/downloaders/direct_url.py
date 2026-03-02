"""
HTTP/HTTPS 直链视频下载器

支持：
- 流式下载（避免内存爆炸）
- 从 Content-Disposition 或 URL 推导文件名和扩展名
- 进度回调
- 代理支持
"""
import hashlib
from pathlib import Path
from typing import Dict, Any, Optional, Set
from urllib.parse import urlparse, unquote

from .base import BaseDownloader
from vat.utils.logger import setup_logger

logger = setup_logger("downloader.direct_url")

# 支持的视频扩展名（用于从 URL/Content-Type 推导）
_VIDEO_EXTENSIONS = {'.mp4', '.mkv', '.webm', '.avi', '.mov', '.flv', '.ts', '.m4v'}

# Content-Type 到扩展名的映射
_CONTENT_TYPE_MAP = {
    'video/mp4': '.mp4',
    'video/webm': '.webm',
    'video/x-matroska': '.mkv',
    'video/quicktime': '.mov',
    'video/x-flv': '.flv',
    'video/mp2t': '.ts',
    'video/x-msvideo': '.avi',
}


class DirectURLDownloader(BaseDownloader):
    """HTTP/HTTPS 直链视频下载器
    
    从裸视频直链下载文件到 output_dir/original.{ext}，
    通过 ffprobe 提取元数据，从 URL 推导标题。
    """
    
    def __init__(self, proxy: str = "", timeout: int = 300):
        """
        Args:
            proxy: HTTP 代理地址（可选）
            timeout: 连接超时秒数
        """
        self.proxy = proxy
        self.timeout = timeout
    
    @property
    def guaranteed_fields(self) -> Set[str]:
        # 直链只保证 duration（来自 ffprobe），不保证 title/description/uploader
        return {'duration'}
    
    def download(self, source: str, output_dir: Path, **kwargs) -> Dict[str, Any]:
        """从直链下载视频
        
        Args:
            source: HTTP/HTTPS 视频 URL
            output_dir: 输出目录
            **kwargs:
                title: 手动指定标题（可选）
                progress_callback: 进度回调（可选）
        """
        import requests
        
        assert source.startswith(('http://', 'https://')), (
            f"DirectURLDownloader 仅支持 HTTP/HTTPS URL，收到: {source}"
        )
        
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        progress_callback = kwargs.get('progress_callback')
        if progress_callback:
            progress_callback(f"开始下载直链视频: {source[:100]}...")
        
        # 流式下载
        proxies = {"http": self.proxy, "https": self.proxy} if self.proxy else None
        
        try:
            resp = requests.get(
                source, stream=True, timeout=self.timeout, proxies=proxies,
                headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
            )
            resp.raise_for_status()
        except requests.RequestException as e:
            raise RuntimeError(f"直链下载请求失败: {source} — {e}") from e
        
        # 推导文件扩展名
        ext = self._guess_extension(source, resp)
        output_path = output_dir / f"original{ext}"
        
        # 写入文件
        total_size = int(resp.headers.get('content-length', 0))
        downloaded = 0
        
        try:
            with open(output_path, 'wb') as f:
                for chunk in resp.iter_content(chunk_size=65536):  # 64KB chunks
                    f.write(chunk)
                    downloaded += len(chunk)
                    if progress_callback and total_size:
                        pct = downloaded / total_size * 100
                        progress_callback(f"下载进度: {pct:.1f}% ({downloaded // 1024 // 1024}MB)")
        except Exception as e:
            # 下载中断，清理不完整文件
            if output_path.exists():
                output_path.unlink()
            raise RuntimeError(f"直链下载写入失败: {e}") from e
        
        file_size = output_path.stat().st_size
        assert file_size > 0, f"下载的文件大小为 0: {source}"
        
        logger.info(f"直链下载完成: {output_path} ({file_size // 1024 // 1024}MB)")
        
        # ffprobe 提取 metadata
        probe_data = self.probe_video_metadata(output_path)
        duration = probe_data.get('duration', 0) if probe_data else 0
        
        if not duration:
            logger.warning(f"ffprobe 未能提取时长: {output_path}")
        
        # 标题：手动指定 > URL 文件名
        title = kwargs.get('title', '') or self._title_from_url(source)
        
        if progress_callback:
            progress_callback(f"直链视频下载完成: {output_path.name}")
        
        return {
            'video_path': output_path,
            'title': title,
            'subtitles': {},
            'metadata': {
                'duration': duration,
                'url': source,
                'video_id': '',
                'description': '',
                'uploader': '',
                'upload_date': '',
                'thumbnail': '',
                'channel_id': '',
                'subtitle_source': 'asr',
                'available_subtitles': [],
                'available_auto_subtitles': [],
            }
        }
    
    def validate_source(self, source: str) -> bool:
        """验证源是否为 HTTP/HTTPS URL"""
        return isinstance(source, str) and source.startswith(('http://', 'https://'))
    
    def extract_video_id(self, source: str) -> str:
        """基于 URL 生成视频 ID"""
        return hashlib.md5(source.encode()).hexdigest()[:16]
    
    def _guess_extension(self, url: str, resp) -> str:
        """从 URL 路径或 Content-Type 推导文件扩展名"""
        # 优先从 URL 路径
        path = urlparse(url).path
        ext = Path(path).suffix.lower()
        if ext in _VIDEO_EXTENSIONS:
            return ext
        
        # 从 Content-Type
        content_type = resp.headers.get('content-type', '').split(';')[0].strip()
        if content_type in _CONTENT_TYPE_MAP:
            return _CONTENT_TYPE_MAP[content_type]
        
        # 默认 .mp4
        logger.debug(f"无法从 URL/Content-Type 推导扩展名，默认 .mp4 (url={url}, ct={content_type})")
        return '.mp4'
    
    @staticmethod
    def _title_from_url(url: str) -> str:
        """从 URL 推导标题（取最后一段路径的文件名 stem）"""
        path = urlparse(url).path
        filename = unquote(Path(path).stem)
        # 过滤无意义的值
        if filename and filename not in ('/', '', 'index', 'video', 'download'):
            return filename
        return ''
