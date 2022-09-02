FROM oryd/oathkeeper:v0.39.3 as oathkeeper-image

FROM python:3.10.6-alpine3.16 AS venv-image
WORKDIR /usr/src/app

ENV PIP_VERSION="22.2.2"
ENV POETRY_VERSION="1.1.15"
RUN apk add --no-cache \
    file \
    make \
    build-base \
    curl \
    gcc \
    git \
    musl-dev \
    libffi-dev \
    python3-dev \
    postgresql-dev \
    openssl-dev \
    cargo \
  && pip install --upgrade pip==$PIP_VERSION \
  && curl -sSL https://install.python-poetry.org | python3 - \
  && ln -s "$HOME/.local/bin/poetry" "/usr/local/bin" \
  && python -m venv /opt/venv

ENV PATH="/opt/venv/bin:$PATH"
COPY pyproject.toml poetry.lock ./
RUN poetry config virtualenvs.create false \
  && poetry install --no-dev --no-interaction


# This is the final app image. Starting from a clean alpine image, it
# copies over the previously created virtual environment.
FROM python:3.10.6-alpine3.16 AS app-image
ARG FLASK_APP=swpt_debtors

ENV FLASK_APP=$FLASK_APP
ENV APP_ROOT_DIR=/usr/src/app
ENV APP_ASSOCIATED_LOGGERS=swpt_pythonlib.flask_signalbus.signalbus_cli
ENV PYTHONPATH="$APP_ROOT_DIR"
ENV PATH="/opt/venv/bin:$PATH"
ENV WEBSERVER_PORT=80
ENV RESOURCE_SERVER=http://127.0.0.1:4499
ENV GUNICORN_LOGLEVEL=warning

RUN apk add --no-cache \
    libffi \
    postgresql-libs \
    supervisor \
    gettext \
    && addgroup -S "$FLASK_APP" \
    && adduser -S -D -h "$APP_ROOT_DIR" "$FLASK_APP" "$FLASK_APP"

COPY --from=oathkeeper-image /usr/bin/oathkeeper /usr/bin/oathkeeper
COPY --from=venv-image /opt/venv /opt/venv

WORKDIR /usr/src/app

COPY docker/entrypoint.sh \
     docker/gunicorn.conf.py \
     docker/supervisord-webserver.conf \
     docker/supervisord-all.conf \
     docker/trigger_supervisor_process.py \
     wsgi.py \
     pytest.ini \
     ./
COPY docker/oathkeeper/ oathkeeper/
COPY migrations/ migrations/
COPY $FLASK_APP/ $FLASK_APP/
RUN python -m compileall -x '^\./(migrations|tests)/' . \
    && rm -f .env \
    && chown -R "$FLASK_APP:$FLASK_APP" .
RUN flask openapi write openapi.json

USER $FLASK_APP
ENTRYPOINT ["/usr/src/app/entrypoint.sh"]
CMD ["all"]


# This is the swagger-ui image. Starting from the final app image, it
# copies the auto-generated OpenAPI spec file. The entrypoint
# substitutes the placeholders in the spec file with values from
# environment variables.
FROM swaggerapi/swagger-ui:v3.42.0 AS swagger-ui-image

ENV SWAGGER_JSON=/openapi.json

COPY --from=app-image /usr/src/app/openapi.json /openapi.template
COPY docker/swagger-ui/entrypoint.sh /

ENTRYPOINT ["/entrypoint.sh"]
CMD ["sh", "/usr/share/nginx/run.sh"]
