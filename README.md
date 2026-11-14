# phira-mp

`phira-mp` is a multiplayer server and client library for Phira developed with Rust. This repository is a hardened personal fork of [TeamFlos/phira-mp](https://github.com/TeamFlos/phira-mp) with security, concurrency, lifecycle, and stability fixes while maintaining wire-protocol compatibility.

[简体中文](README.zh-CN.md) | English Version

## Environment

- Rust 1.88 or later (the workspace uses edition 2024 and `let` chains)

## Server Installation

### For Linux

#### Dependent
You will need `git`, `curl`, and Rust 1.88+ (`rustup`: https://www.rust-lang.org/tools/install).

For Ubuntu or Debian users, install `curl`, `git`, and the C build toolchain with:

```shell
sudo apt install curl git build-essential
```
For Fedora or CentOS users, use the following command:
```shell
sudo dnf install curl git gcc
```
For Arch Linux users, install `curl`, `git`, and `base-devel` (or install Rust directly via `sudo pacman -S --needed rustup git base-devel && rustup default stable`):
```shell
sudo pacman -S --needed curl git base-devel
```
After `curl` is installed, install Rust with the following command (skip if you already installed it via `rustup` above):
```shell
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```
Then clone this repository and build the server:
```shell
git clone https://github.com/HaixueNiyi/phira-mp
cd phira-mp
cargo build --release --locked -p phira-mp-server
```
#### Running the Server
You can run the application with the following command:
```shell
RUST_LOG=info target/release/phira-mp-server
```

You can also specify the listen address and port (defaults to `[::]:12346` with fallback to `0.0.0.0:12346`):
```shell
RUST_LOG=info target/release/phira-mp-server --bind 0.0.0.0 --port 8080
```

Log files are written to `./log/` (hourly rotation). Use `--log-level <off|error|warn|info|debug|trace>` (default `debug`) to choose what is written to the files, and `--log-max-files <N>` (default `168`, i.e. one week; `0` keeps everything) to limit how many rotated files are kept.

### For docker

1. Create Dockerfile
```
FROM ubuntu:22.04

RUN apt-get update && apt-get -y upgrade && apt-get install -y ca-certificates curl git build-essential

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"
WORKDIR /root/
RUN git clone https://github.com/HaixueNiyi/phira-mp
WORKDIR /root/phira-mp
RUN cargo build --release --locked -p phira-mp-server

ENTRYPOINT ["/root/phira-mp/target/release/phira-mp-server", "--port", "<preferred-port>"]
```

2. Build the image
`docker build --tag phira-mp .`

3. Run the container
`docker run -it --name phira-mp -p <preferred-port>:<preferred-port> --restart=unless-stopped phira-mp`

#### Monitoring
You can check the running process and the port it's listening on with:
```shell
ps aux | grep phira-mp-server
ss -tuln | grep 12346
```

