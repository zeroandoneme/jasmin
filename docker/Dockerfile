FROM python:3.11-slim-bullseye

MAINTAINER Jookies LTD <jasmin@jookies.net>

# Create a non-root user
RUN groupadd -r jasmin && useradd -r -g jasmin jasmin

# Install system dependencies
RUN apt-get update && apt-get install -y \
    libffi-dev \
    libssl-dev \
    libjemalloc2

RUN apt-get clean && rm -rf /var/lib/apt/lists/*

# Run python with jemalloc
ENV LD_PRELOAD /usr/lib/x86_64-linux-gnu/libjemalloc.so.2

# Set up working directories
ENV CONFIG_PATH /etc/jasmin
ENV RESOURCE_PATH /etc/jasmin/resource
ENV STORE_PATH /etc/jasmin/store
ENV LOG_PATH /var/log/jasmin
RUN mkdir -p ${RESOURCE_PATH} ${STORE_PATH} ${LOG_PATH}
RUN chown jasmin:jasmin ${CONFIG_PATH} ${RESOURCE_PATH} ${STORE_PATH} ${LOG_PATH}

WORKDIR /build

# ---- START CACHING DEPENDENCIES ----
# Copy only requirements.txt first to leverage Docker cache
COPY requirements.txt /tmp/requirements.txt

# Install pip build tools, including wheel
RUN pip install --upgrade pip setuptools wheel

# Install dependencies first (cached if requirements.txt doesn't change)
RUN pip install --no-cache-dir -r /tmp/requirements.txt
# ---- END CACHING DEPENDENCIES ----

# Now copy the rest of the code
COPY . .

# Install the application
RUN pip install .

ENV UNICODEMAP_JP unicode-ascii

COPY misc/config/*.cfg ${CONFIG_PATH}/
COPY misc/config/resource ${RESOURCE_PATH}

WORKDIR /etc/jasmin

# Default Redis and RabbitMQ connections
ENV AMQP_BROKER_HOST 'rabbitmq'
ENV AMQP_BROKER_PORT 5672
ENV REDIS_CLIENT_HOST 'redis'
ENV REDIS_CLIENT_PORT 6379

# Change binding settings for jcli, redis, and amqp
RUN sed -i '/\[jcli\]/a bind=0.0.0.0' ${CONFIG_PATH}/jasmin.cfg
RUN sed -i "/\[redis-client\]/a port=$REDIS_CLIENT_PORT" ${CONFIG_PATH}/jasmin.cfg
RUN sed -i "/\[amqp-broker\]/a port=$AMQP_BROKER_PORT" ${CONFIG_PATH}/jasmin.cfg
RUN sed -i "/\[redis-client\]/a host=$REDIS_CLIENT_HOST" ${CONFIG_PATH}/jasmin.cfg
RUN sed -i "/\[amqp-broker\]/a host=$AMQP_BROKER_HOST" ${CONFIG_PATH}/jasmin.cfg

EXPOSE 2775 8990 1401
VOLUME ["/var/log/jasmin", "/etc/jasmin", "/etc/jasmin/store"]

COPY docker/docker-entrypoint.sh /
ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["jasmind.py", "--enable-interceptor-client", "--enable-dlr-thrower", "--enable-dlr-lookup", "-u", "jcliadmin", "-p", "jclipwd"]
# Notes:
# - jasmind is started with native dlr-thrower and dlr-lookup threads instead of standalone processes
# - restapi (0.9rc16+) is not started in this docker configuration
