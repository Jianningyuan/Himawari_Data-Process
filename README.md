# Himawari_Data Process

本项目用于处理和分析 Himawari 卫星数据，支持数据预处理、可视化和批量处理等功能。

## 目录结构

```
Himawari_Data/
└── Process/
    ├── objtive_main.py
    ├── data/
    └── download.py
```

## 安装依赖

请确保已安装 Python 3.12 及以上版本。推荐使用虚拟环境。

安装依赖库：

- numpy
- netCDF4
- tqdm
- satpy
- paramiko

安装示例：

```bash
pip install numpy netCDF4 tqdm satpy paramiko
```

或者

```bash
uv add install numpy netCDF4 tqdm satpy paramiko
```

## 使用方法

1. 克隆本仓库或下载代码到本地。
2. 进入 `Process` 目录，安装依赖。
3. 运行主程序：

```bash
python objctive_main.py
```

4. 根据实际需求修改配置或参数。

## 配置说明

- 配置参数可在 `config.py` 或主程序中修改。
- 在程序目录新建`config.py`
- 写入如下内容，修改为自己申请的ptree的数据
```python
FTP_HOST = "replace_with_host"
FTP_USER = "replace_with_user"
FTP_PASS = "replace_with_pass"
```

## 主要功能

- Himawari 卫星数据的读取与预处理
- 数据可视化
- 批量数据处理
- 支持自定义扩展

## 贡献

欢迎提交 issue 或 pull request 以改进本项目。
