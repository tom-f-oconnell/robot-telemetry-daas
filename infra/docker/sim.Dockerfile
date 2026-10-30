# ROS-less robot fleet simulator (Kafka producer + MCAP recorder) and the
# contracts CLI (used by the registry-init container).
# Build context: repository root.
#   docker build -f infra/docker/sim.Dockerfile -t ghcr.io/<owner>/rtdaas-sim:dev .
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates libgl1 libglib2.0-0 \
 && rm -rf /var/lib/apt/lists/* \
 && groupadd --gid 10001 rtdaas \
 && useradd --uid 10001 --gid rtdaas --create-home --shell /usr/sbin/nologin rtdaas

WORKDIR /opt/rtdaas
COPY pyproject.toml README.md ./
COPY rtdaas ./rtdaas
COPY contracts ./contracts
COPY quality ./quality
# video extra so `rtdaas-jigsaws synth` (used by scripts/demo.sh) can render videos in this image;
# land extra for `rtdaas-land` (the bag-lander service)
RUN pip install "/opt/rtdaas[kafka,video,land]" \
 && mkdir -p /data && chown -R rtdaas:rtdaas /data /opt/rtdaas

VOLUME ["/data"]
USER rtdaas

ENTRYPOINT ["rtdaas-sim"]
CMD ["run", "--robots", "3", "--duration", "600", "--rate", "50", "--kafka", "--mcap-out", "/data/bags"]
