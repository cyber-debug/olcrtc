# VLESS over olcrtc

`cmd/olcrtc-vless` - экспериментальный bridge, который использует `olcrtc`
как transport, а VLESS framing как proxy protocol поверх него.

Схема TCP:

```text
SOCKS5 client
  -> olcrtc-vless client
  -> smux stream
  -> VLESS TCP request
  -> olcrtc transport
  -> olcrtc-vless server
  -> target TCP
```

UDP relay уже есть в `pkg/olcrtc/vless.ServeUDP`, но CLI client пока
поддерживает только SOCKS5 TCP CONNECT. SOCKS5 UDP ASSOCIATE должен быть
добавлен отдельным шагом, чтобы не смешивать transport API, smux TCP bridge и
UDP flow dispatcher в один большой change.

## Server

```yaml
mode: server
olcrtc:
  auth: jitsi
  roomId: "https://meet.small-dm.ru/ROOM"
  name: "vless-server"
  dns: "8.8.8.8:53"
vless:
  userId: "11111111-1111-1111-1111-111111111111"
  udpIdleTimeout: "30s"
  udpReadTimeout: "1s"
  maxUdpAssociations: 256
```

```bash
go run ./cmd/olcrtc-vless server.yaml
```

## Client

```yaml
mode: client
olcrtc:
  auth: jitsi
  roomId: "https://meet.small-dm.ru/ROOM"
  name: "vless-client"
  dns: "8.8.8.8:53"
vless:
  userId: "11111111-1111-1111-1111-111111111111"
socks:
  listen: "127.0.0.1:1080"
```

```bash
go run ./cmd/olcrtc-vless client.yaml
curl --socks5-hostname 127.0.0.1:1080 https://example.com/
```

## Ограничения

- Это не Xray plugin и не drop-in transport для Happ/Xray.
- Клиентский CLI сейчас поддерживает SOCKS5 TCP CONNECT.
- Один olcrtc reliable stream используется как carrier для `smux`; каждый SOCKS
  TCP CONNECT становится отдельным smux stream с VLESS request внутри.
- Для полноценного VPN нужен TUN или SOCKS5 UDP ASSOCIATE client dispatcher.
