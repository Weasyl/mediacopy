```
MEDIA_ROOT=/path/to/weasyl/static/media
SQLALCHEMY_URL=postgresql+psycopg2cffi://weasyl@weasyl-database/weasyl
BUCKET_NAME=c.weasyl.dev
```

```shell
DOCKER_BUILDKIT=1 docker build -t weasyl-mediacopy .
```

- Mount credentials at `/weasyl/.aws/credentials`.
