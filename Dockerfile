# The agent image. GoReleaser places the Linux binary of each platform under $TARGETPLATFORM.
# The docker CLI image holds the Compose plugin, which restores and drills use.
FROM docker:28.5.1-cli
ARG TARGETPLATFORM
RUN apk add --no-cache btrfs-progs lvm2 util-linux
COPY $TARGETPLATFORM/synod /usr/local/bin/synod
ENTRYPOINT ["/usr/local/bin/synod"]
CMD ["agent", "run", "--config", "/var/lib/synod/agent/agent.json"]
