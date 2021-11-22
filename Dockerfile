FROM rust:alpine3.14


WORKDIR /usr/src/data-streamer
ADD .dockerignore .dockerignore
COPY . .
RUN apk add --no-cache musl-dev
RUN cargo install --path . 
