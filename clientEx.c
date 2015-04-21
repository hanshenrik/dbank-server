#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <arpa/inet.h>

#define PORT_NUMBER 5108
 
int main(int argc , char *argv[])
{
  int sock;
  struct sockaddr_in server;
  char message[1000] , server_reply[2000];
   
  // Create socket
  sock = socket(AF_INET, SOCK_STREAM, 0);
  if (sock == -1) {
    printf("C: Could not create socket");
  }
  puts("C: Socket created");
   
  server.sin_addr.s_addr = inet_addr("161.73.147.225");
  server.sin_family = AF_INET;
  server.sin_port = htons( PORT_NUMBER );
 
  // Connect to server
  if (connect(sock , (struct sockaddr *)&server , sizeof(server)) < 0) {
    perror("C: connect failed. Error");
    return 1;
  }
   
  puts("C: Connected\n");
   
  // Communicate with server
  while(1) {
    // Receive a reply from the server
    if ( recv(sock , server_reply , 2000 , 0) < 0) {
      puts("C: recv failed");
      break;
    }
    puts(server_reply);
    
    scanf("%s" , message);
    
    // Send data
    if ( send(sock, message, strlen(message), 0) < 0) {
      puts("C: Send failed");
      return 1;
    }
  }
   
  close(sock);
  return 0;
}
