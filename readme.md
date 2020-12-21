# 读取(下载)文件

支持断点续传

## 配置文件

mm.json

```json
{
	"minio": {
		"endPoint": "127.0.0.1",
		"port": 9000,
		"accessKey": "mmstudio",
		"secretKey": "Mmstudio123",
		"useSSL": false,
		"region": "cn-north-1",
		"partSize": 5242880
	}
}
```

## docker

```yml
version: '3.7'

services:
  minio:
    image: minio/minio
    container_name: minio
    command: server /data
    volumes:
      - /home/taoqf/data/minio:/data
    ports:
      - 9000:9000
    environment:
      MINIO_ACCESS_KEY: mmstudio
      MINIO_SECRET_KEY: Mmstudio123
```%
