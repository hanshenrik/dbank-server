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

/* Sockets related */
#define SOCKET_MESSAGE_SIZE 2000
#define MAX_CONN_REQ_IN_QUEUE 3
#define PORT_NUMBER 5108

double balance = 13.37;

// look into https://github.com/bumptech/stud for encryption!
// http://simplestcodings.blogspot.co.uk/2010/08/secure-server-client-using-openssl-in-c.html
// http://stackoverflow.com/questions/11580944/client-to-server-authentication-in-c-using-sockets

void *connection_handler(void *);

int authenticate_user(char * username, char * password);
int get_branch(int account_id, char * username);
double get_balance(int account_id);

int main(int argc, char **argv) {
  // MPI variables
  int my_rank, NP;
  int source, dest;
  int tag = 50;
  int length;
  char name[MPI_MAX_PROCESSOR_NAME + 1];
  char message[MESSAGE_SIZE];
  MPI_Status status;
  
  MPI_Init(&argc, &argv); // argc and argv passed by address to all MPI processes
  MPI_Comm_rank(MPI_COMM_WORLD, &my_rank); // returns taskID = rank of calling process
  MPI_Comm_size(MPI_COMM_WORLD, &NP); // returns total no of MPI processes available

  // shared bank variables
  int i;
  
  if (my_rank == MASTER) { // Head Office (master)
    // Print startup info
    printf("HO: started\n");
    MPI_Get_processor_name(name, &length);
    printf("HO: name = %s, my_rank = %d\n", name, my_rank);

    // Socket stuff
    int socket_descriptor, client_sock, c, *new_sock;
    struct sockaddr_in server, client;
    
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
    if ( bind(socket_descriptor,(struct sockaddr *)&server , sizeof(server)) < 0) {
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
      
      if ( pthread_create(&sniffer_thread, NULL,  connection_handler, (void*) new_sock) < 0) {
        perror("HO: Could not create thread");
        return 1;
      }
       
      // Now join the thread, so that we dont terminate before the thread
      pthread_join(sniffer_thread, NULL);
      puts("HO: Socket handler thread exited");
    }
     
    if (client_sock < 0) {
      perror("HO: Connection accept failed");
      return 1;
    }
    
    for (source = 1; source < NP; source++) {
      MPI_Recv(message, MESSAGE_SIZE, MPI_CHAR, MPI_ANY_SOURCE, tag, MPI_COMM_WORLD, &status);
      printf("HO received: %s\n", message);
    }
  }

  else { // Branch Office (slave)
    // Branch variables
    pid_t pid = 0;
    int number_of_accounts = 3;
    int account_status = 0, wpid, i;

    // sqlite3 variables
    sqlite3 *db;
    int rc;
    char query[QUERY_SIZE];

    MPI_Get_processor_name(name, &length);
    printf("BO%d: starting on name = %s\n", my_rank, name);

    // Get the number of accounts to start from DB 
    rc = sqlite3_open_v2("accounts.db", &db, SQLITE_OPEN_READONLY, NULL);

    if (rc) {
      fprintf(stderr, "BO%d: Can't open database: %s\n", my_rank, sqlite3_errmsg(db));
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
          printf("BO%d: pid < 0, fork failed!\n", my_rank);
          continue;
        }

        if (pid == 0) { // Inside bank account process
          printf("BO%d: account_id = %d, balance = %f\n", my_rank, account_id, balance);
          // RPC
          // printf("BO%d_%d: before execl", my_rank, account_id);
          // execl("rpc/accounts/account_server", "account_server", (char*)NULL);
          // printf("BO%d_%d: after execl", my_rank, account_id);
          //
          return 0;
        } else {
          printf("BO%d: created pid = %d\n", my_rank, pid);
        }
      } else if (row == SQLITE_DONE) {
        printf("BO%d: Done creating account processes.\n", my_rank);
        break;
      } else {
        fprintf(stderr, "BO: sqlite row failed..\n");
        return 1;
      }
    }
    sqlite3_finalize(statement);
    sqlite3_close(db);

    // Let Head Office know Branch up and is running
    sprintf(message, "Branch %d successfully started on processor %s", my_rank, name);
    MPI_Send(message, strlen(message) + 1, MPI_CHAR, MASTER, tag, MPI_COMM_WORLD);

    // Wait for messages from Head Office for eternity
    for (i = 0; i < 2; i++) {
      printf("BO%d wating for MPI_Recv for %dth time\n", my_rank, i);
      MPI_Recv(message, MESSAGE_SIZE, MPI_CHAR, MASTER, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
      printf("BO%d recv from master with tag %d: %s\n", my_rank, status.MPI_TAG, message);
      // TODO: query process for account status.MPI_TAG

      // TODO: act based on operation. For now, only do get_balance
      // Workaround: get balance from db
      balance = get_balance(status.MPI_TAG);

      MPI_Send(&balance, sizeof(double), MPI_DOUBLE, MASTER, status.MPI_TAG, MPI_COMM_WORLD);
      printf("BO%d sent: %f\n", my_rank, balance);
    }

    // RPC
    // wait for all children to exit
    // while ( (wpid = wait(&account_status) ) > 0) {}
  }

  MPI_Finalize();
  return 0;
}


/* Handles client connections in separate threads on Head Office node */
void *connection_handler(void *socket_descriptor) {
  printf("CH: Connection is being handled.\n");
  // Socket variables
  int read_size;
  char server_message[SOCKET_MESSAGE_SIZE], client_message[SOCKET_MESSAGE_SIZE];
  const char *separator = ";";

  // MPI variables
  char message[MESSAGE_SIZE];
  MPI_Status status;

  // Get socket descriptor
  int sock = *(int*) socket_descriptor;
  
  // Answer client
  sprintf(server_message, "CH: Connection successfull! Please give me: username;password;account_id;operation;amount;to_account\n");
  write(sock, server_message, strlen(server_message));
  
  // Wait for messages from client
  while ( (read_size = recv(sock, client_message, SOCKET_MESSAGE_SIZE, 0)) > 0 ) {
    int i;
    char *token, *parameters[6];
    char *username, *password;
    int account_id, operation, to_account_id;
    double amount;
    int isAuth;

    i = 0;
    token = strtok(client_message, separator);
    // Separate request parameters
    while (token != NULL) {
      parameters[i++] = token;
      token = strtok(NULL, separator);
    }
    
    if (i < 5) {
      printf("i < 5"); // TODO: check we have enough parameters!
    }

    // Put parameters in variables
    username      = parameters[0];
    password      = parameters[1]; // should only receive salt and hash.
    account_id    = atoi(parameters[2]);
    operation     = atoi(parameters[3]);
    amount        = atof(parameters[4]);
    to_account_id = atoi(parameters[5]);

    // TODO: validate parameters better to avoid crash

    printf("CH: %s, %s, %d, %d, %f, %d\n", username, password, account_id, operation, amount, to_account_id);

    // Authenticate user against users.db

    isAuth = authenticate_user(username, password);
    
    if (isAuth) { // Valid username + password
      printf("CH: user authenticated!\n");

      // Find out which branch to contact
      int branch;
      branch = get_branch(account_id, username);

      // Send message to branch, wait for receive (blocking)
      printf("CH: invoking operation %d on account %d on branch %d\n", operation, account_id, branch);
      sprintf(message, "%s;%d;%d;%f;%d", username, account_id, operation, amount, to_account_id);
      MPI_Send(message, strlen(message) + 1, MPI_CHAR, branch, account_id, MPI_COMM_WORLD);
      MPI_Recv(&balance, sizeof(double), MPI_DOUBLE, branch, account_id, MPI_COMM_WORLD, &status);
      
      // Send new balance to client
      // sprintf(server_message, "Balance is £%f\n", *result);
      sprintf(server_message, "Balance is £%f\n", balance);
      printf("CH: sending message: %s\n", server_message);
      write(sock, server_message, strlen(server_message));
      printf("CH: after write");

      // // RPC
      // CLIENT *clnt;
      // void   *pVoid;
      // double *result;
      // char *server;
     
      // server = "node04.scl.cct.brookes.ac.uk"; // TODO get this with MPI?
      // clnt = clnt_create(server, ACCOUNTPROG, ACCOUNTVERS, "tcp");
      // if (clnt == (CLIENT *)NULL) {
      //   // Couldn't establish connection with server. Print error message and die.
      //   clnt_pcreateerror(server);
      //   exit(1);
      // }
      // // Call the remote procedure based on operation
      // if (operation == 1) {
      //   result = getbalance_1(pVoid, clnt);
      // } else if (operation == 2) {
      //   result = deposit_1(amount, clnt);
      // } else if (operation == 3) {
      //   result = withdraw_1(amount, clnt);
      // }
      // if (result == (double *)NULL) {
      //   // An error occurred while calling the server. Print error message and die.
      //   clnt_perror(clnt, server);
      //   exit(1);
      // }
      // // Okay, we successfully called the remote procedure.
      // if (*result == -1.0) {
      //   // Insufficient funds
      //   fprintf(stderr, "Insuficcient funds.\n");
      //   exit(1);
      // }
      // /* Success */
      // printf("Done on: %s\n", server);
      // clnt_destroy( clnt );
      // // exit(0);
      // // end of RPC
    } else if (isAuth == 0) { // Invalid username + password
      printf("CH: Invalid password!\n");
      sprintf(server_message, "Invalid password for user!\n");
      printf("CH: sending message: %s\n", server_message);
      write(sock, server_message, strlen(server_message));
    } else if (isAuth == -1) { // User doen't exist in DB
      printf("CH: Invalid user!\n");
      sprintf(server_message, "Invalid username '%s'!\n", username);
      printf("CH: sending message: %s\n", server_message);
      write(sock, server_message, strlen(server_message));
    }
  }
   
  if (read_size == 0) {
    puts("CH: Client disconnected.");
    fflush(stdout);
  } else if (read_size == -1) {
    perror("CH: recv failed");
  }

  // Free socket pointer
  free(socket_descriptor);
  return 0;
}

double get_balance(int account_id) {
  printf("get_balance()\n");
  double bal;
  sqlite3 *db;
  int rc;
  char query[QUERY_SIZE];
  rc = sqlite3_open_v2("accounts.db", &db, SQLITE_OPEN_READONLY, NULL);
  
  if (rc) {
    fprintf(stderr, "BO: Can't open database: %s\n", sqlite3_errmsg(db));
    sqlite3_close(db);
    return(1);
  }

  sqlite3_stmt *statement;
  sprintf(query, "SELECT balance FROM accounts WHERE id = %d", account_id);
  sqlite3_prepare_v2(db, query, strlen(query) + 1, &statement, NULL);
  int row = sqlite3_step(statement);
  if (row == SQLITE_ROW) {
    bal = sqlite3_column_double(statement, 0);
  } else {
    fprintf(stderr, "BO: Couldn't find account_id\n");
    return 1;
  }
  sqlite3_finalize(statement);
  sqlite3_close(db);
  return bal;
}

int authenticate_user(char * username, char * password) {
  printf("authenticate_user()\n");
  sqlite3 *db;
  sqlite3_backup *pBackup;
  char *zErrMsg = 0;
  int rc;
  char query[QUERY_SIZE];
  sqlite3_stmt *statement;
  
  int isAuth = 0;

  rc = sqlite3_open_v2("users.db", &db, SQLITE_OPEN_READONLY, NULL);

  if (rc) {
    fprintf(stderr, "CH: Can't open database: %s\n", sqlite3_errmsg(db));
    sqlite3_close(db);
    return(1);
  }
  
  sprintf(query, "SELECT password FROM users WHERE username = '%s'", username);
  sqlite3_prepare_v2(db, query, strlen(query) + 1, &statement, NULL);
  int row = sqlite3_step(statement);
  if (row == SQLITE_ROW) {
    const unsigned char *dbPassword;
    dbPassword = sqlite3_column_text(statement, 0);
    if ( strcmp(password, dbPassword) == 0 ) {
      printf("CB: password matches username!\n");
      isAuth = 1;
    } else {
      printf("CB: password didn't match username!\n");
      isAuth = 0;
    }
  } else {
    fprintf(stderr, "CB: couldn't find username!\n");
    return -1;
  }
  sqlite3_finalize(statement);
  sqlite3_close(db);
  return isAuth;
}

int get_branch(int account_id, char * username) {
  printf("get_branch()\n");
  sqlite3 *db;
  int rc;
  char query[QUERY_SIZE];
  sqlite3_stmt *statement;
  
  int branch;
  
  rc = sqlite3_open_v2("accounts.db", &db, SQLITE_OPEN_READONLY, NULL);

  sprintf(query, "SELECT branch FROM accounts WHERE id = %d AND username = '%s'", account_id, username);
  sqlite3_prepare_v2(db, query, strlen(query) + 1, &statement, NULL);
  // TODO: Bind values to host parameters using the sqlite3_bind_*() interfaces.
  int row = sqlite3_step(statement);
  if (row == SQLITE_ROW) {
    branch = sqlite3_column_int(statement, 0);
    printf("HO: %s's account %d runs on branch %d\n", username, account_id, branch);
  } else {
    fprintf(stderr, "HO: username + account_id doesn't match\n");
    branch = -1;
  }
  sqlite3_finalize(statement);
  sqlite3_close(db);
  return branch;
}
