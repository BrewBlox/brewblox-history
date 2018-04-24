FROM brewblox/brewblox-service:latest

RUN mkdir -p /pkg
COPY ./pkg/* /pkg/

EXPOSE 5000

RUN pip3 install /pkg/* || true \
    && pip3 install brewblox-history \
    && pip3 show brewblox-history

ENTRYPOINT ["python3", "-m", "brewblox_history"]
