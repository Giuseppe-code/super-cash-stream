FROM alpine:3.20 AS build
RUN apk add --no-cache build-base
WORKDIR /src
COPY generatore.c .
RUN gcc -O2 -s -o genC1 generatore.c

FROM alpine:3.20
WORKDIR /app
COPY --from=build /src/genC1 /usr/local/bin/genC1
ENTRYPOINT ["/usr/local/bin/genC1"]
