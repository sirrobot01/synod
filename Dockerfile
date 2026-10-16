# The agent and controller image. GoReleaser places the Linux binary of each platform under $TARGETPLATFORM.
# The docker CLI image holds the Compose plugin, which restores and drills use.
FROM docker:28.5.1-cli
ARG TARGETPLATFORM
RUN apk add --no-cache btrfs-progs lvm2 util-linux zfs
COPY $TARGETPLATFORM/synod /usr/local/bin/synod
# Discovery skips containers with this label, so Synod never backs up its own agent or controller.
LABEL io.synod.role="agent"
ENTRYPOINT ["/usr/local/bin/synod"]
CMD ["agent", "run", "--config", "/var/lib/synod/agent/agent.json"]
