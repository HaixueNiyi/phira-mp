# phira-mp

`phira-mp` 是一个用 Rust 开发的 Phira 多人游戏服务端与客户端库。本仓库基于 [TeamFlos/phira-mp](https://github.com/TeamFlos/phira-mp) 进行个人维护与安全/稳定性加固，保持与原版二进制通信协议兼容。

简体中文 | [English Version](README.md)
## 环境

- Rust 1.88 或更高版本（工作区使用 edition 2024 和 `let` 链式条件）

## 服务端安装
### 对于 `Linux` 用户
#### 依赖
需要 `git`、`curl` 与 Rust 1.88 或更高版本（可通过 `rustup` 安装：https://www.rust-lang.org/tools/install ）。

对于 Ubuntu 或 Debian 用户，可使用以下命令安装 `curl` 与 `git`：

```shell
sudo apt install curl git
```
对于 Fedora 或 CentOS 用户，请使用以下命令：
```shell
sudo yum install curl git
```
对于 Arch Linux 用户，可使用以下命令安装 `curl` 与 `git`（也可以直接运行 `sudo pacman -S --needed rustup git && rustup default stable`）：
```shell
sudo pacman -S --needed curl git
```
安装 `curl` 后，使用以下命令安装 Rust（若上面已通过 `rustup` 安装则可跳过）：
```shell
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```
然后克隆本仓库并构建服务端：
```shell
git clone https://github.com/HaixueNiyi/phira-mp
cd phira-mp
cargo build --release --locked -p phira-mp-server
```
#### 运行服务端
您可以使用以下命令运行该应用程序：
```shell
RUST_LOG=info target/release/phira-mp-server
```

也可以通过参数指定监听地址和端口（默认绑定 `[::]:12346`，失败时回退到 `0.0.0.0:12346`）：
```shell
RUST_LOG=info target/release/phira-mp-server --bind 0.0.0.0 --port 8080
```

日志文件写入 `./log/`（按小时滚动）。可用 `--log-level <off|error|warn|info|debug|trace>`（默认 `debug`）控制写入文件的日志级别，用 `--log-max-files <N>`（默认 `168`，即一周；`0` 表示不清理）限制保留的日志文件数量。

### For docker

1. 创建 Dockerfile
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

2. 构建镜像
`docker build --tag phira-mp .`

3. 运行容器
`docker run -it --name phira-mp -p <preferred-port>:<preferred-port> --restart=unless-stopped phira-mp`

#### 监控
您可以检查正在运行的进程及其正在侦听的端口：
```shell
ps aux | grep phira-mp-server
ss -tuln | grep 12346
```
