// generator.c  — client TCP per Logstash (codec line)
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <sys/socket.h>

#define HOST "logstash"  
#define PORT "8080"       

typedef struct {
    char  tempo[64];
    char  negozio[8];
    int   order_id;
    char  product_id[16];
    int   quantity;
    float price;
} Messaggio;

int main(void) {
    // stdout line-buffered per vedere subito i printf nei log Docker
    setvbuf(stdout, NULL, _IOLBF, 0);

    struct addrinfo hints, *res = NULL;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family   = AF_INET;       // IPv4
    hints.ai_socktype = SOCK_STREAM;   // TCP

    int rc = getaddrinfo(HOST, PORT, &hints, &res);
    if (rc != 0) {
        fprintf(stderr, "getaddrinfo(%s:%s): %s\n", HOST, PORT, gai_strerror(rc));
        return 1;
    }

    int sock = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if (sock < 0) { perror("socket"); freeaddrinfo(res); return 1; }

    // Retry connect finché Logstash non è pronto
    int retries = 0;
    while (connect(sock, res->ai_addr, res->ai_addrlen) < 0) {
        if (retries++ > 20) { perror("connect"); close(sock); freeaddrinfo(res); return 1; }
        fprintf(stderr, "Waiting for Logstash...\n");
        sleep(2);
    }
    freeaddrinfo(res);
    fprintf(stderr, "Connected to Logstash ✅\n");

    Messaggio mes = {0};
    strcpy(mes.negozio, "S293");
    strcpy(mes.product_id, "DP34");
    mes.quantity = 2;
    mes.price    = 4.99f;

    char line[256];
    for (;;) {
        time_t now = time(NULL);
        struct tm *tm_info = gmtime(&now);
        strftime(mes.tempo, sizeof(mes.tempo), "%Y-%m-%dT%H:%M:%SZ", tm_info);

        mes.order_id++;

        int n = snprintf(line, sizeof(line),
                         "%s %s %d %s %d %.2f\n",
                         mes.tempo, mes.negozio, mes.order_id,
                         mes.product_id, mes.quantity, mes.price);
        if (n < 0) { perror("snprintf"); break; }

        ssize_t sent = send(sock, line, (size_t)n, 0);
        if (sent < 0) { perror("send"); break; }

        sleep(1);
    }

    close(sock);
    return 0;
}
