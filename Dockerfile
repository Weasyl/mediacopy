# syntax=docker/dockerfile:experimental
FROM alpine:3.11
RUN apk add --update \
    musl-dev gcc make \
    libffi-dev \
    postgresql-dev \
    python3-dev
RUN adduser -S weasyl -h /weasyl -u 100
WORKDIR /weasyl
USER weasyl
ENV HOME /weasyl
RUN python3 -m venv .venv
COPY requirements.lock ./
RUN --mount=type=cache,id=pip,target=/weasyl/.cache/pip,sharing=private,uid=100 .venv/bin/pip install -r requirements.lock
COPY mediacopy.py ./
CMD [".venv/bin/python", "mediacopy.py"]
