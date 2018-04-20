FROM brewblox/brewblox-service:rpi-latest

RUN mkdir -p /pkg
COPY ./pkg/* /pkg/

EXPOSE 5000

RUN pip3 install git+https://github.com/plugaai/aioinflux@89b106389f20f3ad333498fb6986436c07058bfd
RUN pip3 install /pkg/* || true \
    && pip3 install brewblox-history \
    && pip3 show brewblox-history

ENTRYPOINT ["python3", "-m", "brewblox_history"]
