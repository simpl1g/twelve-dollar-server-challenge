# Builds a Ruby submission exactly as the challenge does: install.sh as root, then build.sh.
# Build context = the submission directory. Layers are ordered so that variants that only change
# .rb files reuse the Ruby/SQLite/libsodium toolchain layer and the gems layer.
FROM ubuntu:24.04
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates sqlite3 jq openssl procps && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY install.sh sqlite_train.c* /app/
RUN bash /app/install.sh
COPY Gemfile Gemfile.lock build.sh /app/
RUN bash /app/build.sh
COPY . /app/
