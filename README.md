# FamilyClaw 官方天气插件

## 这是什么

这是 FamilyClaw 的官方天气插件源码仓库。

它负责从天气服务提供商拉取天气数据，并把结果同步成 FamilyClaw 可消费的天气设备和实体。

当前默认天气源是 `MET Norway`，同时预留了 `OpenWeather` 和 `WeatherAPI` 的 API Key 配置入口。

## 仓库结构

```text
.
├─ manifest.json
├─ requirements.txt
└─ official_weather/
   ├─ __init__.py
   ├─ integration.py
   ├─ service.py
   ├─ providers.py
   ├─ repository.py
   ├─ models.py
   ├─ schemas.py
   ├─ entity_normalizer.py
   ├─ locales/
   └─ migrations/
```

## 市场提交时建议使用的路径

- 仓库分支：`main`
- `manifest.json` 路径：`official_weather/manifest.json`
- README 路径：`README.md`
- 插件包根目录：`official_weather`
- `requirements.txt` 路径：`requirements.txt`

## 说明

- 这是运行在 FamilyClaw 主程序里的插件，不是一个独立运行的 Python 应用。
- 仓库里的 `official_weather/` 目录是插件包根目录。
- 插件运行时依赖部分 FamilyClaw 宿主提供的内部模块。

## 维护者

- jingyi0605 | https://github.com/jingyi0605
