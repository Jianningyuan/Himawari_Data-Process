# Himawari 卫星数据处理脚本

import logging
import sys
import os
import re
import bz2
import shutil
import functools  # 为了使用部分函数应用
import concurrent.futures
from datetime import datetime
from collections import defaultdict
from pathlib import Path
import numpy as np
from satpy import Scene
from satpy.composites import DayNightCompositor
from satpy.writers import to_image

# --- Configuration ---
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Constants
DATA_ROOT_DIR = Path("./data")
DECOMPRESSED_DIR = Path("./decompressed_data")
OUTPUT_DIR = Path("./output_images")
SATELLITE_READER = "ahi_hsd"
MAX_DECOMPRESSION_THREADS = os.cpu_count() or 4
FILENAME_PATTERN = re.compile(r"HS_H09_(\d{8})_(\d{4})_B\d{2}_.*\.DAT\.bz2")

class HimawariProcessor:
    '处理 Himawari 卫星数据的类'
    def __init__(self):
        # 确保输出目录存在
        DECOMPRESSED_DIR.mkdir(parents=True, exist_ok=True)
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    def scan_available_data(self, data_root: Path) -> dict[str, list[Path]]:
        """
        扫描数据目录以查找可用的数据时间点及其文件。
        """
        logging.info("扫描 %s 以查找可用的数据时间点...", data_root)
        available_slots = defaultdict(list)
        file_count = 0
        for bz2_file in data_root.rglob("HS_H09_*.DAT.bz2"):
            match = FILENAME_PATTERN.match(bz2_file.name)
            if match:
                date_str, time_str = match.groups()
                slot_key = f"{date_str}_{time_str}"
                available_slots[slot_key].append(bz2_file)
                file_count += 1
            else:
                logging.debug("文件名不匹配模式，已跳过: %s ", bz2_file.name)

        logging.info(
            "扫描完成。找到 %s 个数据文件，分布在 %s 个时间点。",
            file_count,
            len(available_slots),
        )
        return dict(sorted(available_slots.items()))

    def prompt_user_selection(self, available_slots: dict[str, list[Path]]) -> list[str]:
        """
        提示用户选择要处理的时间点。
        """
        if not available_slots:
            logging.warning("没有找到可供选择的数据时间点。")
            return []

        print("\n可用的数据时间点:")
        slot_keys = list(available_slots.keys())
        for i, key in enumerate(slot_keys):
            try:
                dt = datetime.strptime(key, "%Y%m%d_%H%M")
                print(
                    f"  {i + 1}: {dt.strftime('%Y-%m-%d %H:%M')} ({len(available_slots[key])} files)"
                )
            except ValueError:
                print(f"  {i + 1}: {key} ({len(available_slots[key])} files)")  # Fallback

        print(
            "\n请输入您想处理的时间点编号 (例如: 1, 3, 5), 或输入 'all' 处理所有, 或按 Enter 退出:"
        )
        while True:
            user_input = input("> ").strip()
            if not user_input:
                logging.info("用户选择退出。")
                return []
            if user_input.lower() == "all":
                logging.info("用户选择处理所有时间点。")
                return slot_keys

            try:
                selected_indices = [int(x.strip()) - 1 for x in user_input.split(",")]
                selected_keys = []
                valid_selection = True
                for index in selected_indices:
                    if 0 <= index < len(slot_keys):
                        selected_keys.append(slot_keys[index])
                    else:
                        print(
                            f"错误: 编号 {index + 1} 无效。请输入 1 到 {len(slot_keys)} 之间的数字。"
                        )
                        valid_selection = False
                        break
                if valid_selection:
                    logging.info(
                        "用户选择了 %s 个时间点: %s", len(selected_keys), selected_keys
                    )
                    return selected_keys
            except ValueError:
                print("输入无效。请输入逗号分隔的数字 (例如: 1, 3, 5) 或 'all'。")

    def decompress_bz2(self, bz2_file_path: Path, output_dir: Path) -> Path | None:
        """
        解压 .bz2 文件，检查重复和零大小。
        """
        output_filename = bz2_file_path.stem
        output_path = output_dir / output_filename
        try:
            if output_path.exists():
                if output_path.stat().st_size > 0:
                    return output_path
                else:
                    logging.warning("存在但大小为零，重新解压: %s", output_path)
                    output_path.unlink()

            with bz2.open(bz2_file_path, "rb") as f_in:
                with open(output_path, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)
            return output_path
        except OSError as e:
            logging.error("解压文件时出错 %s: %s", bz2_file_path.name, e)
            if output_path.exists():
                output_path.unlink()
            return None
        except Exception as e:
            logging.error("处理文件时发生意外错误 %s: %s", bz2_file_path.name, e)
            if output_path.exists():
                output_path.unlink()
            return None

    def decompress_files_multithreaded(self, bz2_files: list[Path], output_dir: Path, max_workers: int) -> dict[Path, Path | None]:
        """
        多线程解压缩 .bz2 文件列表。
        """
        if not bz2_files:
            return {}

        logging.info("开始使用最多 %s 个线程解压 %s个文件...", max_workers, len(bz2_files))
        results = {}
        decompress_func = functools.partial(self.decompress_bz2, output_dir=output_dir)

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_path = {
                executor.submit(decompress_func, bz2_file): bz2_file
                for bz2_file in bz2_files
            }
            decompressed_count = 0
            for future in concurrent.futures.as_completed(future_to_path):
                original_path = future_to_path[future]
                try:
                    result_path = future.result()
                    results[original_path] = result_path
                    if result_path:
                        decompressed_count += 1
                        if decompressed_count % 50 == 0:
                            logging.info(
                                '已解压 %s 文件...',
                                decompressed_count/len(bz2_files),
                            )
                except Exception as exc:
                    logging.error(
                        "文件 %s 解压缩失败: %s",
                        original_path.name,
                        exc,
                    )
                    results[original_path] = None

        successful_decomp = sum(1 for p in results.values() if p is not None)
        logging.info('多线程解压完成。成功解压 %s 个文件。',
                    successful_decomp/len(bz2_files) * 100.0 if len(bz2_files) > 0 else 0.0
                )
        return results

    def invert_image(self, np_array):
        """
        Inverts the image by subtracting each pixel value from the maximum value of the image.
        """
        min_val = np.nanmin(np_array)
        max_val = np.nanmax(np_array)
        return max_val - (np_array - min_val)

    def process_true_data(self, decompressed_files: list[Path], output_dir: Path):
        """
        使用 Satpy 处理解压缩后的数据文件，生成合成图像。
        """
        if not decompressed_files:
            logging.warning("没有可供处理的解压缩文件。")
            return

        try:
            scn = Scene(decompressed_files, reader=SATELLITE_READER)
            scan_time = scn.start_time
            if not scan_time:
                try:
                    fname = decompressed_files[0].name
                    parts = fname.split("_")
                    date_str, time_str = parts[2], parts[3]
                    scan_time = datetime.strptime(f"{date_str}{time_str}", "%Y%m%d%H%M")
                    logging.warning("无法从元数据获取时间，从文件名解析: %s", scan_time)
                except Exception as e:
                    logging.error(
                        "无法确定扫描时间，跳过处理: %s. 错误: %s", 
                        decompressed_files,
                        e
                    )
                    return

            date_str = scan_time.strftime("%Y%m%d")
            time_str = scan_time.strftime("%H%M")
            current_output_dir = output_dir / date_str
            current_output_dir.mkdir(parents=True, exist_ok=True)

            logging.info(
                "--- 开始处理时间点 %s ---", 
                scan_time.strftime("%Y-%m-%d %H:%M")
            )

            true_color_base_bands = {"B01", "B02", "B03"}
            available_datasets = set(scn.available_dataset_names())
            resampled_scn = None
            if true_color_base_bands.issubset(available_datasets):
                scn.load(['true_color', 'B13'])
                resample_cache = Path("./resample_cache")
                resample_cache.mkdir(exist_ok=True)
                resampled_scn = scn.resample(scn.finest_area(),
                                             resampler="native",
                                             cache_dir=str(resample_cache)
                                             )
                del scn
            else:
                logging.warning("缺少必要的波段，无法生成真彩色图像。")

            logging.info("  重采样成功，'%s' 已生成。", 'true_color')

            invert_data = self.invert_image(resampled_scn['B13'].values)
            resampled_scn['B13'].values = invert_data

            compositor = DayNightCompositor('DN', day_night="day_night")
            composite = compositor([resampled_scn['true_color'], resampled_scn["B13"]])
            img = to_image(composite)
            output_filename = (
                        current_output_dir / f"{date_str}_{time_str}_TrueColor.png"
                    )
            img.save(str(output_filename), fill_value=0.0)
            logging.info("生成的图像已保存到: %s", output_filename)
        except Exception as e:
            logging.error("处理数据时发生错误: %s", e)
            return

    def run(self):
        ''' 主运行函数，执行整个处理流程。'''
        available_slots = self.scan_available_data(DATA_ROOT_DIR)
        if not available_slots:
            logging.error("没有找到可供处理的数据。")
            return

        selected_slots = self.prompt_user_selection(available_slots)
        if not selected_slots:
            logging.info("用户未选择任何时间点，退出。")
            return

        for slot_key in selected_slots:
            bz2_files = available_slots[slot_key]
            logging.info("处理时间点: %s", slot_key)

            decompressed_files = self.decompress_files_multithreaded(
                bz2_files, DECOMPRESSED_DIR, MAX_DECOMPRESSION_THREADS
            )

            successful_files = [f for f in decompressed_files.values() if f]

            self.process_true_data(successful_files, OUTPUT_DIR)

if __name__ == "__main__":
    try:
        logging.info("开始执行脚本...")
        processor = HimawariProcessor()
        processor.run()
        logging.info("脚本执行完成。")
    except KeyboardInterrupt:
        logging.info("脚本被用户中断。")
    except Exception as e:
        logging.error("脚本执行过程中发生错误: %s", e)
        sys.exit(1)
