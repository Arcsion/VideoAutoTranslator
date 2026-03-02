"""
本地视频文件导入器

不执行下载，仅：
1. 验证文件存在且为支持的视频格式
2. 在 output_dir 创建软链接（统一 output_dir 结构）
3. 通过 ffprobe 提取视频元数据
4. 从文件名推导标题（如果未手动指定）
"""
import hashlib
from pathlib import Path
from typing import Dict, Any, Optional, Set

from .base import BaseDownloader
from vat.utils.logger import setup_logger

logger = setup_logger("downloader.local")

SUPPORTED_VIDEO_EXTENSIONS = {
    '.mp4', '.mkv', '.webm', '.avi', '.mov', '.flv', '.ts', '.m4v',
}


def generate_content_based_id(file_path: Path) -> str:
    """基于文件内容生成稳定的视频 ID
    
    读取文件前 1MB + 文件大小作为哈希输入。
    - 前 1MB：区分不同视频文件
    - 文件大小：防止前 1MB 相同但长度不同的文件碰撞
    
    Args:
        file_path: 视频文件路径（必须存在）
        
    Returns:
        16 字符 hex ID
    """
    hasher = hashlib.md5()
    with open(file_path, 'rb') as f:
        hasher.update(f.read(1024 * 1024))  # 前 1MB
    hasher.update(str(file_path.stat().st_size).encode())
    return hasher.hexdigest()[:16]


class LocalImporter(BaseDownloader):
    """本地视频文件导入器
    
    将本地视频文件"导入"到 VAT pipeline：
    - 在 output_dir 创建指向原始文件的软链接（零磁盘开销）
    - 通过 ffprobe 提取视频元数据（时长、编码等）
    - 从文件名推导标题（如果用户未手动指定）
    
    软链接失败时（如跨文件系统），自动 fallback 为硬链接或复制。
    """
    
    @property
    def guaranteed_fields(self) -> Set[str]:
        # LOCAL 只保证 duration（来自 ffprobe），不保证 title/description/uploader
        return {'duration'}
    
    def download(self, source: str, output_dir: Path, **kwargs) -> Dict[str, Any]:
        """导入本地视频文件
        
        Args:
            source: 本地视频文件绝对路径
            output_dir: 输出目录
            **kwargs:
                title: 手动指定标题（可选）
                progress_callback: 进度回调（可选，本地导入基本不用）
        """
        source_path = Path(source).resolve()
        
        # 验证
        assert source_path.exists(), f"本地视频文件不存在: {source}"
        assert source_path.is_file(), f"路径不是文件: {source}"
        assert source_path.suffix.lower() in SUPPORTED_VIDEO_EXTENSIONS, (
            f"不支持的视频格式: {source_path.suffix}，"
            f"支持: {sorted(SUPPORTED_VIDEO_EXTENSIONS)}"
        )
        
        # 在 output_dir 创建软链接
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        link_name = f"original{source_path.suffix.lower()}"
        link_path = output_dir / link_name
        
        self._create_link(source_path, link_path)
        
        # ffprobe 提取 metadata
        probe_data = self.probe_video_metadata(source_path)
        duration = probe_data.get('duration', 0) if probe_data else 0
        
        if not duration:
            logger.warning(f"ffprobe 未能提取时长: {source_path}")
        
        # 标题：手动指定 > 文件名 stem
        title = kwargs.get('title', '') or source_path.stem
        
        progress_callback = kwargs.get('progress_callback')
        if progress_callback:
            progress_callback(f"本地文件导入完成: {source_path.name}")
        
        return {
            'video_path': link_path,
            'title': title,
            'subtitles': {},
            'metadata': {
                'duration': duration,
                'url': str(source_path),
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
        """验证本地文件路径是否为有效的视频文件"""
        p = Path(source)
        return p.exists() and p.is_file() and p.suffix.lower() in SUPPORTED_VIDEO_EXTENSIONS
    
    def extract_video_id(self, source: str) -> str:
        """基于文件内容生成稳定的视频 ID"""
        return generate_content_based_id(Path(source))
    
    def _create_link(self, source_path: Path, link_path: Path) -> None:
        """在 output_dir 创建指向源文件的链接
        
        优先软链接，失败时尝试硬链接，最后 fallback 为复制。
        """
        # 清理已存在的链接/文件
        if link_path.exists() or link_path.is_symlink():
            link_path.unlink()
        
        # 尝试软链接
        try:
            link_path.symlink_to(source_path)
            logger.debug(f"创建软链接: {link_path} -> {source_path}")
            return
        except OSError as e:
            logger.warning(f"软链接失败（可能跨文件系统）: {e}，尝试硬链接")
        
        # 尝试硬链接
        try:
            link_path.hardlink_to(source_path)
            logger.debug(f"创建硬链接: {link_path} -> {source_path}")
            return
        except OSError as e:
            logger.warning(f"硬链接也失败: {e}，fallback 为复制文件")
        
        # 最后 fallback：复制
        import shutil
        shutil.copy2(source_path, link_path)
        logger.info(f"已复制文件: {source_path} -> {link_path}")
