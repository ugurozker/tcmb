# syntax=docker/dockerfile:1.4
FROM --platform=$BUILDPLATFORM python:3.11 as builder

WORKDIR /app

COPY requirements.txt /app
RUN pip3 install --upgrade pip
RUN --mount=type=cache,target=/root/.cache/pip \
    pip3 install -r requirements.txt

COPY . /app

RUN mkdir /.cache && chmod -R 777 /.cache

ENTRYPOINT ["python3"]
CMD ["tcmb.py"]


FROM builder as dev-envs

RUN <<EOF
apk update
apk add git
EOF

ARG PYDEVD_USE_CYTHON=NO

RUN <<EOF
addgroup -S docker
adduser -S --shell /bin/bash --ingroup docker vscode
EOF

COPY --from=gloursdocker/docker / /
