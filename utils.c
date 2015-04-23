#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <sqlite3.h>
#include <mpi.h>

#define GET_BALANCE_OPERATION 1
#define DEPOSIT_OPERATION 2
#define WITHDRAW_OPERATION 3
#define TRANSFER_OPERATION 4
#define MESSAGE_SIZE 100
#define MASTER  0
#define QUERY_SIZE 500
#define SOCKET_MESSAGE_SIZE 2000
#define MAX_CONN_REQ_IN_QUEUE 3
#define PORT_NUMBER 5108
#define NUMBER_OF_CLIENT_PARAMETERS 5

static const char USERNAME_KILL[] = "kill";
static const char PASSWORD_KILL[] = "kill";

/* Handles client connections in separate threads on Head Office node */
void *connection_handler(void *socket_descriptor) {
  printf("CH: Connection is being handled.\n");
  // Socket variables
  int read_size;
  char server_message[SOCKET_MESSAGE_SIZE], client_message[SOCKET_MESSAGE_SIZE];

  // MPI variables
  char message[MESSAGE_SIZE];
  MPI_Status status;

  const char *separator = ";";
  
  // Get socket descriptor
  int sock = *(int*) socket_descriptor;
  
  // Answer client
  sprintf(server_message, "CH: Connection successfull. Please give me: username;password;account_id;operation;amount;to_account\n");
  write(sock, server_message, strlen(server_message));
  
  // Wait for messages from client
  while ( (read_size = recv(sock, client_message, SOCKET_MESSAGE_SIZE, 0)) > 0 ) {
    int i;
    char *token, *parameters[NUMBER_OF_CLIENT_PARAMETERS];
    char *username, *password;
    int account_id, operation, to_account_id;
    double amount;
    int isAuth;

    // Separate request parameters
    i = 0;
    token = strtok(client_message, separator);
    while (token != NULL) {
      parameters[i++] = token;
      token = strtok(NULL, separator);
    }
    
    // Make sure we have all parameters we need
    if (i < NUMBER_OF_CLIENT_PARAMETERS + 1) {
      printf("CH: i < 6. Not enough parameters!\n");
      sprintf(server_message, "Not enough parameters.\n");
      printf("CH: sending message: %s\n", server_message);
      write(sock, server_message, strlen(server_message));
      continue;
    }

    // Put parameters in variables
    username      = parameters[0];
    password      = parameters[1]; // should only receive salt and hash.
    account_id    = atoi(parameters[2]);
    operation     = atoi(parameters[3]);
    amount        = atof(parameters[4]);
    to_account_id = atoi(parameters[5]);

    // TODO: stronger validating of parameters to avoid possible crashes

    printf("CH: %s, %s, operation = %d, account_id = %d, amount = %f, to_account_id = %d\n", username, password, account_id, operation, amount, to_account_id);

    // Check if SHUTDOWN signal sent
    if (strcmp(username, USERNAME_KILL) == 0 && strcmp(password, PASSWORD_KILL) == 0) {
      printf("CH: kill user specified, shutting down!\n");
      return (void *) 42;
    }
    // Authenticate user against users.db
    isAuth = authenticate_user(username, password);
    
    if (isAuth) { // Valid username + password
      printf("CH: user authenticated!\n");

      // Find out which branch to contact
      int branch;
      branch = get_branch(account_id, username);
      if (branch == -1) {
        printf("CH: username + account_id doesn't match!\n");
        sprintf(message, "You don't have an account with id '%d'.\n", account_id);
        printf("CH: sending message: %s\n", message);
        write(sock, message, strlen(message));
        continue;
      }

      // Send message to branch, wait for receive (blocking)
      printf("CH: invoking operation %d on account %d on branch %d\n", operation, account_id, branch);
      sprintf(message, "%d;%f;%d\n", account_id, amount, to_account_id);
      MPI_Send(message, MESSAGE_SIZE, MPI_CHAR, branch, operation, MPI_COMM_WORLD);
      MPI_Recv(message, MESSAGE_SIZE, MPI_CHAR, branch, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
      
      // Send new balance to client
      printf("CH: sending message: %s\n", message);
      write(sock, message, strlen(message));

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
      // sprintf(message, "Balance is £%.2f\n", *result);
      // // end of RPC

    } else if (isAuth == 0) { // Invalid password
      printf("CH: Invalid password!\n");
      sprintf(message, "Invalid password.\n");
      printf("CH: sending message: %s\n", message);
      write(sock, message, strlen(message));
      continue;
    } else if (isAuth == -1) { // User doen't exist in DB
      printf("CH: Invalid user!\n");
      sprintf(message, "Invalid username '%s'.\n", username);
      printf("CH: sending message: %s\n", message);
      write(sock, message, strlen(message));
      continue;
    } else if (isAuth == -2) { // DB error
      printf("CH: DB error!\n");
      sprintf(message, "Error. Please try again later.\n");
      printf("CH: sending message: %s\n", message);
      write(sock, message, strlen(message));
      continue;
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

int authenticate_user(char * username, char * password) {
  printf("authenticate_user()\n");
  sqlite3 *db;
  int rc;
  char query[QUERY_SIZE];
  sqlite3_stmt *statement;
  
  int isAuth = 0;

  rc = sqlite3_open_v2("users.db", &db, SQLITE_OPEN_READONLY, NULL);

  if (rc) {
    fprintf(stderr, "authenticate_user(): Can't open database: %s\n", sqlite3_errmsg(db));
    sqlite3_close(db);
    isAuth = -2;
  }
  
  sprintf(query, "SELECT password FROM users WHERE username = '%s'", username);
  sqlite3_prepare_v2(db, query, strlen(query) + 1, &statement, NULL);
  int row = sqlite3_step(statement);
  if (row == SQLITE_ROW) {
    const unsigned char *dbPassword;
    dbPassword = sqlite3_column_text(statement, 0);
    if ( strcmp(password, dbPassword) == 0 ) {
      printf("authenticate_user(): password matches username!\n");
      isAuth = 1;
    } else {
      printf("authenticate_user(): password didn't match username!\n");
      isAuth = 0;
    }
  } else {
    fprintf(stderr, "CB: couldn't find username!\n");
    isAuth = -1;
  }
  sqlite3_finalize(statement);
  sqlite3_close(db);
  return isAuth;
}

int get_branch(int account_id, char * username) {
  printf("get_branch() account_id = %d\n", account_id);
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
    printf("get_branch(): %s's account %d runs on branch %d\n", username, account_id, branch);
  } else {
    fprintf(stderr, "get_branch(): username + account_id doesn't match\n");
    branch = -1;
  }
  sqlite3_finalize(statement);
  sqlite3_close(db);
  return branch;
}
