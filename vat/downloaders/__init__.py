"""
下载器模块

两层接口层级：
- BaseDownloader: 所有源类型的通用接口
- PlatformDownloader: 平台下载器接口（YouTube 等），增加 playlist 等能力
"""
from .base import BaseDownloader, PlatformDownloader
from .youtube import YouTubeDownloader, VideoInfoResult
from .local import LocalImporter, generate_content_based_id
from .direct_url import DirectURLDownloader

__all__ = [
    'BaseDownloader', 'PlatformDownloader',
    'YouTubeDownloader', 'VideoInfoResult',
    'LocalImporter', 'generate_content_based_id',
    'DirectURLDownloader',
]
