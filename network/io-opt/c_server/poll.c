#include <stdio.h>
#include <unistd.h>
#include <sys/poll.h>

int main()
{
    int sockfd, client, addrlen;
    int server_len, client_len;
    struct sockaddr_in addr;
    struct sockaddr_in client_address;
    int result, max;

    int fds[5];

    // 模拟5个客户端
    for (int i = 0; i < 5; i++)
    {
        memset(&client, 0, sizeof(client));
        addrlen = sizeof(client);
        pollfds[i].fd = accept(sockfd, (struct sockaddr *)&client, &addrlen);
        pollfds[i].events = POLLIN; // 这个5个 socket 只关注只读事件
    }
    sleep(1);

    while (1)
    {
        puts("round again");
        /*poll 中传入 pollfds 数组，交给内核判断是否有事件发生，如果哪个发生事件则 revents 置1*/
        poll(pollfds, 5, 50000);

        /*遍历数组，找到哪个 pollfds 有事件发生*/
        for (int i = 0; i < 5; i++)
        {
            if (pollfds[i].revents & POLLIN)
            {
                pollfds[i].revents = 0; // 找到后 revents 置0
                memset(buffer, 0, MAXBUF);
                read(pollfds[i].fd, buffer, MAXBUF); // 读取数据
                puts(buffer);
            }
        }
    }
}