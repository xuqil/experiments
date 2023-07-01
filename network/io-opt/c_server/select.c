#include <sys/types.h>
#include <sys/socket.h>
#include <stdio.h>
#include <netinet/in.h>
#include <sys/time.h>
#include <sys/ioctl.h>
#include <unistd.h>
#include <stdlib.h>

int main()
{
    int sockfd, client, addrlen;
    int server_len, client_len;
    struct sockaddr_in addr;
    struct sockaddr_in client_address;
    int result, max;

    int fds[5];
    fd_set readfds;
    sockfd = socket(AF_INET, SOCK_STREAM, 0); // 建立服务器端socket
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(50007);
    server_len = sizeof(addr);
    bind(sockfd, (struct sockaddr *)&addr, server_len);
    listen(sockfd, 5); // 监听队列最多容纳5个

    for (int i = 0; i < 5; i++) // 模拟5个客户端连接
    {
        memset(&client, 0, sizeof(client));
        addrlen = sizeof(client);
        fds[i] = accept(sockfd, (struct sockaddr *)&client, &addrlen);
        if (fds[i] > max)
        {
            max = fds[i]; // 找到一个最大的文件描述符
        }
    }

    while (1)
    {
        FD_ZERO(&readfds);
        for (int i = 0; i < 5; i++)
        {
            /* &readfds 是一个 bitmap，如果5个文件描述符分别是0,1,2,4,5,7，那么这个 bitmap 为01101101 */
            FD_SET(fds[i], &readfds);
        }

        puts("round again");
        /*select 是一个系统调用，它会阻塞直到有数据发送到 socket，select 会把&readfds 相应的位置重置，但不会返回哪个 socket 有数据*/
        select(max + 1, &readfds, NULL, NULL, NULL);

        /*用户态只要遍历 &readfds，看哪一位被置位了，不需要每次调用系统调用来判断了，效率有很大提升，遍历到被置位的文件描述符就进行读取*/
        for (int i = 0; i < 5; i++)
        {
            if (FD_ISSET(fds[i], &readfds))
            {
                memset(buffer, 0, MAXBUF);
                read(fds[i], buffer, MAXBUF);
                puts(buffer);
            }
        }
    }

    return 0;
}
