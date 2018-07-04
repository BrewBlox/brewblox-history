FROM brewblox/brewblox-service:rpi-latest

WORKDIR /app

COPY ./dist /app/dist

EXPOSE 5000

RUN pip3 install /app/dist/* \
    && pip3 show brewblox-history

ENTRYPOINT ["python3", "-m", "brewblox_history"]