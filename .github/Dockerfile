#
# This Dockerfile builds the image in which cross-platform builds are run.
#
# The reason for using a custom image is that the official
# `goreleaser/goreleaser-cross` image at the time of writing used Goreleaser
# v2.4.4, which had a bug breaking the `builds.overrides` section of the
# `.goreleaser.yaml` file (see
# https://github.com/goreleaser/goreleaser/pull/5298). Once a version of
# `goreleaser/goreleaser-cross` is available using Goreleaser >= 2.4.8 we
# should be able to switch back to the official images.
#
# This image was built and pushed to `surjection/goreleaser-cross:v1.23-v2.4.8`
#

FROM goreleaser/goreleaser-cross:v1.23-v2.4.4

WORKDIR /tmp

RUN wget https://github.com/goreleaser/goreleaser/releases/download/v2.4.8/goreleaser_Linux_x86_64.tar.gz \
  && tar -xvzf goreleaser_Linux_x86_64.tar.gz \
  && mv goreleaser /usr/bin/goreleaser

WORKDIR /
