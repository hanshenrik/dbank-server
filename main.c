/* General stuff */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Sockets */
#include <sys/socket.h>
#include <arpa/inet.h>

/* Database - sqlite3 */
#include <sqlite3.h>

/* MPI */
#include <mpi.h>

/* RPC */
// #include "rpc/account/account.h"

#define GET_BALANCE_OPERATION 1
#define DEPOSIT_OPERATION 2
#define WITHDRAW_OPERATION 3
#define TRANSFER_OPERATION 4

/* MPI related */
#define MESSAGE_SIZE 100
#define MASTER  0

/* sqlite3 related */
#define QUERY_SIZE 500

/* Socket related */
#define SOCKET_MESSAGE_SIZE 2000
#define MAX_CONN_REQ_IN_QUEUE 3
#define PORT_NUMBER 5108
#define NUMBER_OF_CLIENT_PARAMETERS 5

double balance = 13.37;
const char *separator = ";";

// look into https://github.com/bumptech/stud for encryption!
// http://simplestcodings.blogspot.co.uk/2010/08/secure-server-client-using-openssl-in-c.html
// http://stackoverflow.com/questions/11580944/client-to-server-authentication-in-c-using-sockets

void *connection_handler(void *);

int authenticate_user(char * username, char * password);
int get_branch(int account_id, char * username);
double get_balance(int account_id);
double deposit(int account_id, double amount);
double withdraw(int account_id, double amount);
char* transfer(int account_id, int to_account_id, double amount);

int main(int argc, char **argv) {
  // MPI variables
  int my_rank, NP;
  int source, dest;
  int tag;
  int length;
  char name[MPI_MAX_PROCESSOR_NAME + 1];
  char message[MESSAGE_SIZE];
  MPI_Status status;
  
  MPI_Init(&argc, &argv); // argc and argv passed by address to all MPI processes
  MPI_Comm_rank(MPI_COMM_WORLD, &my_rank); // returns taskID = rank of calling process
  MPI_Comm_size(MPI_COMM_WORLD, &NP); // returns total no of MPI processes available

  if (my_rank == MASTER) { // Head Office (master)
    // Print startup info
    printf("HO: started\n");
    MPI_Get_processor_name(name, &length);
    printf("HO: name = %s, my_rank = %d\n", name, my_rank);

    // Socket stuff
    int socket_descriptor, client_sock, c, *new_sock;
    struct sockaddr_in server, client;
    void *thread_status = 0;
    
    // Create socket
    socket_descriptor = socket(AF_INET, SOCK_STREAM, 0);
    if (socket_descriptor == -1) {
      printf("HO: Could not create socket");
    }
    puts("HO: Socket created");
    
    // Prepare sockaddr_in structure
    server.sin_family = AF_INET;
    server.sin_addr.s_addr = INADDR_ANY;
    server.sin_port = htons( PORT_NUMBER );
    
    // Bind
    if ( bind(socket_descriptor, (struct sockaddr *)&server, sizeof(server)) < 0 ) {
      perror("HO: Socket bind failed. Error");
      return 1;
    }
    puts("HO: Socket bind done");
    
    // Listen for new requests
    listen(socket_descriptor, MAX_CONN_REQ_IN_QUEUE);
    puts("HO: Waiting for incoming connections...");
    
    // Accept incoming connection
    c = sizeof(struct sockaddr_in);
    while ( (client_sock = accept(socket_descriptor, (struct sockaddr *)&client, (socklen_t*)&c)) ) {
      puts("HO: Connection accepted!");
      
      pthread_t sniffer_thread;
      new_sock = malloc(1);
      *new_sock = client_sock;
      
      // Start a new thread with connection_handler
      if ( pthread_create(&sniffer_thread, NULL, connection_handler, (void*) new_sock) < 0) {
        perror("HO: Could not create thread");
        return 1;
      }
      
      // Join the thread, so that MASTER doesn't terminate before the thread
      pthread_join(sniffer_thread, &thread_status);

      if (thread_status == 42) {
        printf("HO: Killing signal sent, shutdown system!\n");
        // What to do here?
        // MPI_Abort?
        // MPI_Finalize?
        // break should get us down to MPI_Finalize and return 0, but doesn't work..
        break;
      }
      puts("HO: Socket handler thread exited");
    }
     
    if (client_sock < 0) {
      perror("HO: Connection accept failed");
      return 1;
    }
    
    // for (source = 1; source < NP; source++) {
    //   MPI_Recv(message, MESSAGE_SIZE, MPI_CHAR, MPI_ANY_SOURCE, tag, MPI_COMM_WORLD, &status);
    //   printf("HO received: %s\n", message);
    // }
  }

  else { // Branch Office (slave)
    // Branch variables
    pid_t pid = 0;
    int account_status = 0, wpid;

    // sqlite3 variables
    sqlite3 *db;
    int rc;
    char query[QUERY_SIZE];

    MPI_Get_processor_name(name, &length);
    printf("BO %d: starting on name = %s\n", my_rank, name);

    // Get the number of accounts to start from DB 
    rc = sqlite3_open_v2("accounts.db", &db, SQLITE_OPEN_READONLY, NULL);

    if (rc) {
      fprintf(stderr, "BO %d: Can't open database: %s\n", my_rank, sqlite3_errmsg(db));
      sqlite3_close(db);
      return(1);
    }

    // Find all account ids for this branch
    sqlite3_stmt *statement;
    sprintf(query, "SELECT id, balance FROM accounts WHERE branch = %d", my_rank);
    sqlite3_prepare_v2(db, query, strlen(query) + 1, &statement, NULL);
    
    // For each account, start a new process
    while (1) {
      int row = sqlite3_step(statement);
      if (row == SQLITE_ROW) {
        int account_id;
        account_id = sqlite3_column_int(statement, 0);
        balance = sqlite3_column_double(statement, 1);
        pid = fork();

        if (pid < 0 ) {
          printf("BO %d: pid < 0, fork failed!\n", my_rank);
          continue;
        }

        if (pid == 0) { // Inside bank account process
          printf("BO %d: account_id = %d, balance = %f\n", my_rank, account_id, balance);
          // RPC
          // printf("BO %d_%d: before execl", my_rank, account_id);
          // execl("rpc/accounts/account_server", "account_server", (char*)NULL);
          // printf("BO %d_%d: after execl", my_rank, account_id);
          //
          return 0;
        } else {
          printf("BO %d: created pid = %d\n", my_rank, pid);
        }
      } else if (row == SQLITE_DONE) {
        printf("BO %d: Done creating account processes.\n", my_rank);
        break;
      } else {
        fprintf(stderr, "BO : sqlite row failed..\n");
        return 1;
      }
    }
    sqlite3_finalize(statement);
    sqlite3_close(db);

    // Let Head Office know Branch up and is running
    // sprintf(message, "Branch %d successfully started on processor %s", my_rank, name);
    // MPI_Send(message, strlen(message) + 1, MPI_CHAR, MASTER, tag, MPI_COMM_WORLD);

    // Wait for messages from Head Office for eternity
    while (1) {
      printf("BO %d waiting for MPI_Recv again\n", my_rank);
      MPI_Recv(message, MESSAGE_SIZE, MPI_CHAR, MASTER, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
      printf("BO %d MPI_Recv from master with tag %d: %s\n", my_rank, status.MPI_TAG, message);
      
      char *token, *params[3];
      int account_id, to_account_id;
      double amount, result;
      int j = 0;
      
      // Separate message parameters
      token = strtok(message, separator);
      while (token != NULL) {
        params[j++] = token;
        token = strtok(NULL, separator);
      }

      account_id = atoi(params[0]);
      amount = atof(params[1]);
      to_account_id = atoi(params[2]);

      // Query process for account...
      // RPC not working, use DB instead
      // Operation sent as TAG, get it from status
      switch (status.MPI_TAG) {
        case GET_BALANCE_OPERATION:
          result = get_balance(account_id);
          printf("%f\n", result);
          if (result < 0) {
            sprintf(message, "Couldn't get balance...\n");
          } else {
            sprintf(message, "Balance is £%.2f\n", result);
          }
          break;
        case DEPOSIT_OPERATION:
          result = deposit(account_id, amount);
          if (result < 0) {
            sprintf(message, "Couldn't make deposit...\n");
          } else {
            sprintf(message, "New balance is £%.2f\n", result);
          }
          break;
        case WITHDRAW_OPERATION:
          result = withdraw(account_id, amount);
          if (result < 0) {
            sprintf(message, "Couldn't make withdraw...\n");
          } else {
            sprintf(message, "New balance is £%.2f\n", result);
          }
          break;
        case TRANSFER_OPERATION:
          sprintf(message, transfer(account_id, to_account_id, amount));
          break;
      }

      MPI_Send(message, MESSAGE_SIZE, MPI_CHAR, MASTER, status.MPI_TAG, MPI_COMM_WORLD);
      printf("BO %d sent: %s\n", my_rank, message);
    }

    printf("BO %d: shutting down\n", my_rank);

    // RPC
    // wait for all children to exit
    // while ( (wpid = wait(&account_status) ) > 0) {}
  }

  MPI_Finalize();
  return 0;
}
