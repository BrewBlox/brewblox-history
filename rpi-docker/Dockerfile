FROM brewblox/brewblox-service:rpi-latest

RUN mkdir -p /pkg
COPY ./pkg/* /pkg/

EXPOSE 5000

RUN pip3 install -U brewblox-history -f /pkg/

ENTRYPOINT ["python3", "-m", "brewblox_history"]
