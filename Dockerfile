FROM scratch
COPY pgroll /usr/bin/pgroll
ENTRYPOINT [ "/usr/bin/pgroll" ]