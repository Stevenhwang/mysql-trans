## Require

```bash
golang 1.16+
```

## Installation

```bash
go build
```

## Settings

```bash
config.json
```

## Usage

1. 使用 mysqldump 全量导出源数据库，需要使用 --flush-logs 参数产生新的 binlog 日志
2. 将以上导出的全量源数据导入目标数据库(导入完成才可以开启 mysql-trans，因为后续同步以此次数据为基础)
3. 查看源数据库目前使用的 binlog 日志文件(show master status)
4. 修改配置，启动 mysql-trans, 它会从配置的起点位置同步到目标数据库与源数据库进度相同
5. mysql-trans 运行时会输出同步位置到日志，如果中途同步失败，可以修改配置从失败处开始同步
6. 可以使用 nohup 放在后台运行
