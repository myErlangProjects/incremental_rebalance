FROM erlang:24.1.2.0-alpine AS Builder
RUN wget https://s3.amazonaws.com/rebar3/rebar3 && chmod +x rebar3 && ./rebar3 local install
RUN apk update && apk upgrade && apk add --no-cache bash git openssh
WORKDIR /incremental_rebalance
COPY rebar.config .
COPY src src
COPY config config
RUN rebar3 release

FROM erlang:24.1.2.0-alpine
RUN mkdir -p /opt/incremental_rebalance/system/
RUN mkdir -p /opt/incremental_rebalance/logs/
RUN mkdir -p /opt/incremental_rebalance/logs/backlogs/
RUN mkdir -p /opt/incremental_rebalance/pipe/
RUN mkdir -p /opt/incremental_rebalance/db/
RUN mkdir -p /opt/incremental_rebalance/db/backup/
WORKDIR /opt/incremental_rebalance/system/
COPY --from=Builder /incremental_rebalance/_build/default/rel/incremental_rebalance/ .
COPY ./entrypoint.sh /opt/incremental_rebalance/
RUN dos2unix /opt/incremental_rebalance/entrypoint.sh
RUN chmod +x /opt/incremental_rebalance/entrypoint.sh
ENTRYPOINT ["/opt/incremental_rebalance/entrypoint.sh"]
