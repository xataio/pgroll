#
# This Dockerfile is used by GoReleaser in the `release` job.
# See:
# https://goreleaser.com/customization/docker
#
FROM scratch
COPY pgroll /usr/bin/pgroll
ENTRYPOINT [ "/usr/bin/pgroll" ]
