#!/usr/bin/env python3
"""
批量同步所有已上传视频的B站标题/描述/标签。
使用 resync_video_info 确保模板渲染正确。
"""
import sys
import time
import logging

sys.path.insert(0, '/home/gzy/py/vat')

from vat.config import load_config
from vat.database import Database
from vat.uploaders.bilibili import BilibiliUploader, resync_video_info
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)-8s | %(message)s')
logger = logging.getLogger(__name__)

def main():
    config = load_config()
    db = Database(config.storage.database_path, output_base_dir=config.storage.output_dir)
    cookie_file = Path(config.uploader.bilibili.cookies_file).expanduser()
    uploader = BilibiliUploader(cookies_file=str(cookie_file))
    
    # 收集所有有 bilibili_aid 的视频
    all_videos = db.list_videos()
    to_sync = []
    for v in all_videos:
        meta = v.metadata or {}
        aid = meta.get('bilibili_aid')
        if aid and meta.get('translated'):
            to_sync.append({'vid': v.id, 'aid': int(aid), 'title': (v.title or '')[:40]})
    
    logger.info(f"需要同步信息的视频: {len(to_sync)} 个")
    
    success = 0
    failed = 0
    skipped = 0
    
    for i, item in enumerate(to_sync):
        prefix = f"[{i+1}/{len(to_sync)}] {item['vid']} | aid={item['aid']}"
        
        def cb(msg):
            logger.info(f"  {msg}")
        
        try:
            result = resync_video_info(db, uploader, config, item['aid'], callback=cb)
            if result['success']:
                success += 1
                logger.info(f"{prefix} ✅ {result['title'][:50]}")
            else:
                failed += 1
                logger.warning(f"{prefix} ❌ {result['message']}")
        except Exception as e:
            failed += 1
            logger.error(f"{prefix} 异常: {e}")
        
        # 限流：每次请求间隔 0.5s
        time.sleep(0.5)
    
    logger.info(f"\n{'='*60}")
    logger.info(f"完成: 成功={success}, 失败={failed}, 总计={len(to_sync)}")


if __name__ == '__main__':
    main()
