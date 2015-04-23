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
    
    // for (source = 1; source < NP; source++) {
    //   MPI_Recv(message, MESSAGE_SIZE, MPI_CHAR, MPI_ANY_SOURCE, tag, MPI_COMM_WORLD, &status);
    //   printf("HO received: %s\n", message);
    // }
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

      // TODO: query process for account

      // RPC not working, use DB instead
      switch (status.MPI_TAG) {
        case GET_BALANCE_OPERATION:
          result = get_balance(account_id);
          printf("%f\n", result);
          if (result < 0) {
            sprintf(message, "Couldn't get balance...\n");
          } else {
            sprintf(message, "Balance is £%f\n", result);
          }
          break;
        case DEPOSIT_OPERATION:
          result = deposit(account_id, amount);
          if (result < 0) {
            sprintf(message, "Couldn't make deposit...\n");
          } else {
            sprintf(message, "New balance is £%f\n", result);
          }
          break;
        case WITHDRAW_OPERATION:
          result = withdraw(account_id, amount);
          if (result < 0) {
            sprintf(message, "Couldn't make withdraw...");
          } else {
            sprintf(message, "New balance is £%f\n", result);
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


/* Handles client connections in separate threads on Head Office node */
void *connection_handler(void *socket_descriptor) {
  printf("CH: Connection is being handled.\n");
  // Socket variables
  int read_size;
  char server_message[SOCKET_MESSAGE_SIZE], client_message[SOCKET_MESSAGE_SIZE];

  // MPI variables
  char message[MESSAGE_SIZE];
  MPI_Status status;
  double result;

  // Get socket descriptor
  int sock = *(int*) socket_descriptor;
  
  // Answer client
  sprintf(server_message, "CH: Connection successfull! Please give me: username;password;account_id;operation;amount;to_account\n");
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
    
    if (i < NUMBER_OF_CLIENT_PARAMETERS + 1) {
      printf("CH: i < 6. Not enough parameters!\n");
      sprintf(server_message, "Not enough parameters!\n");
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

    // TODO: validate parameters better to avoid crash

    printf("CH: %s, %s, %d, %d, %f, %d\n", username, password, account_id, operation, amount, to_account_id);

    // Authenticate user against users.db

    isAuth = authenticate_user(username, password);
    
    if (isAuth) { // Valid username + password
      printf("CH: user authenticated!\n");

      // Find out which branch to contact
      int branch;
      branch = get_branch(account_id, username);
      if (branch == -1) {
        printf("CH: username + account_id doesn't match!\n");
        sprintf(message, "You don't own the account '%d'.\n", account_id);
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
      // sprintf(message, "Balance is £%f\n", *result);
      // // end of RPC

    } else if (isAuth == 0) { // Invalid username + password
      printf("CH: Invalid password!\n");
      sprintf(message, "Invalid password!\n");
      printf("CH: sending message: %s\n", message);
      write(sock, message, strlen(message));
      continue;
    } else if (isAuth == -1) { // User doen't exist in DB
      printf("CH: Invalid user!\n");
      sprintf(message, "Invalid username '%s'!\n", username);
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
    return(1);
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
    return -1;
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

double get_balance(int account_id) {
  printf("get_balance() account_id = %d\n", account_id);
  double bal;
  sqlite3 *db;
  int rc;
  char query[QUERY_SIZE];
  rc = sqlite3_open_v2("accounts.db", &db, SQLITE_OPEN_READONLY, NULL);
  
  if (rc) {
    fprintf(stderr, "get_balance(): Can't open database: %s\n", sqlite3_errmsg(db));
    sqlite3_close(db);
    return -1;
  }

  sqlite3_stmt *statement;
  sprintf(query, "SELECT balance FROM accounts WHERE id = %d", account_id);
  sqlite3_prepare_v2(db, query, strlen(query) + 1, &statement, NULL);
  int row = sqlite3_step(statement);
  if (row == SQLITE_ROW) {
    bal = sqlite3_column_double(statement, 0);
  } else {
    fprintf(stderr, "get_balance(): Couldn't find account_id %d\n", account_id);
    return -1;
  }
  sqlite3_finalize(statement);
  sqlite3_close(db);
  printf("%f\n", bal);
  return bal;
}

double deposit(int account_id, double amount) {
  printf("deposit()\n");
  double bal;
  sqlite3 *db;
  sqlite3_backup *pBackup;
  char *zErrMsg = 0;
  int rc;
  char query[QUERY_SIZE];

  // Get current balance
  bal = get_balance(account_id);
  if (amount < 0) {
    printf("Amount must be positive\n");
    return -1;
  } else {
    bal += amount;
  }

  rc = sqlite3_open_v2("accounts.db", &db, SQLITE_OPEN_READWRITE, NULL);
  
  if (rc) {
    fprintf(stderr, "deposit(): Can't open database: %s\n", sqlite3_errmsg(db));
    sqlite3_close(db);
    return -1;
  }

  sqlite3_stmt *statement;
  sprintf(query, "UPDATE accounts SET balance = %f WHERE id = %d", bal, account_id);
  sqlite3_prepare_v2(db, query, strlen(query) + 1, &statement, NULL);
  int result = sqlite3_step(statement);
  if (result == SQLITE_DONE) {
    printf("deposit(): its done\n");
  } else {
    fprintf(stderr, "deposit(): Couldn't find account_id\n");
    return -1;
  }
  sqlite3_finalize(statement);

  // Save balance change
  pBackup = sqlite3_backup_init(db, "main", db, "main");
  if (pBackup) {
    (void) sqlite3_backup_step(pBackup, -1);
    (void) sqlite3_backup_finish(pBackup);
  }
  rc = sqlite3_errcode(db);

  sqlite3_close(db);
  return bal;
}

double withdraw(int account_id, double amount) {
  printf("withdraw()\n");
  double bal;
  sqlite3 *db;
  sqlite3_backup *pBackup;
  char *zErrMsg = 0;
  int rc;
  char query[QUERY_SIZE];

  // Make sure current balance is bigger than wanted withdraw amount
  bal = get_balance(account_id);
  if (amount > bal) {
    printf("withdraw(): Insufficient funds!\n");
    return -1;
  } else if (amount < 0) {
    printf("withdraw(): Can' withdraw negtive amount!\n");
    return -1;
  } else {
    bal -= amount;
  }

  rc = sqlite3_open_v2("accounts.db", &db, SQLITE_OPEN_READWRITE, NULL);
  
  if (rc) {
    fprintf(stderr, "withdraw(): Can't open database: %s\n", sqlite3_errmsg(db));
    sqlite3_close(db);
    return -1;
  }

  sqlite3_stmt *statement;
  sprintf(query, "UPDATE accounts SET balance = %f WHERE id = %d", bal, account_id);
  sqlite3_prepare_v2(db, query, strlen(query) + 1, &statement, NULL);
  int result = sqlite3_step(statement);
  printf("withdraw(): result = %d\n", result);
  if (result == SQLITE_DONE) {
    printf("withdraw(): its done\n");
  } else {
    fprintf(stderr, "withdraw(): Couldn't find account_id\n");
    return -1;
  }
  sqlite3_finalize(statement);

  // Save balance change
  pBackup = sqlite3_backup_init(db, "main", db, "main");
  if (pBackup) {
    (void) sqlite3_backup_step(pBackup, -1);
    (void) sqlite3_backup_finish(pBackup);
  }
  rc = sqlite3_errcode(db);
  
  sqlite3_close(db);
  return bal;
}

char* transfer(int account_id, int to_account_id, double amount) {
  printf("transfer()\n");
  double bal;
  sqlite3 *db;
  sqlite3_backup *pBackup;
  int rc;
  char query[QUERY_SIZE];
  char * message;
  message = malloc(MESSAGE_SIZE);

  // Make sure current balance is bigger than wanted transfer amount
  bal = get_balance(account_id);
  if (amount > bal) {
    sprintf(message, "Insufficient funds!\n");
    printf("transfer(): %s\n", message);
    return message;
  } else if (amount < 0) {
    sprintf(message, "Can't withdraw negtive amount!\n");
    printf("transfer(): %s", message);
    return message;
  } else {
    bal -= amount;
  }

  rc = sqlite3_open_v2("accounts.db", &db, SQLITE_OPEN_READWRITE, NULL);
  
  if (rc) {
    sprintf(message, "transfer(): Can't open database: %s\n", sqlite3_errmsg(db));
    fprintf(stderr, message);
    sqlite3_close(db);
    return message;
  }

  sqlite3_stmt *statement;
  sprintf(query, "UPDATE accounts SET balance = %f WHERE id = %d", bal, account_id);
  sqlite3_prepare_v2(db, query, strlen(query) + 1, &statement, NULL);
  int result = sqlite3_step(statement);
  printf("transfer(): result = %d\n", result);
  if (result == SQLITE_DONE) {
    printf("transfer(): its done\n");
  } else {
    sprintf(message, "transfer(): Couldn't find account_id\n");
    fprintf(stderr, message);
    sqlite3_finalize(statement);
    sqlite3_close(db);
    return message;
  }
  sqlite3_finalize(statement);

  bal = get_balance(to_account_id);
  bal += amount;
  sprintf(query, "UPDATE accounts SET balance = %f WHERE id = %d", bal, to_account_id);
  sqlite3_prepare_v2(db, query, strlen(query) + 1, &statement, NULL);
  result = sqlite3_step(statement);
  printf("transfer(): result = %d\n", result);
  if (result == SQLITE_DONE) {
    printf("transfer(): its done\n");
  } else {
    sprintf(message, "transfer(): Couldn't find account_id\n");
    fprintf(stderr, message);
    sqlite3_finalize(statement);
    sqlite3_close(db);
    return message;
  }
  sqlite3_finalize(statement);

  printf("transfer() before assigning message");
  sprintf(message, "£%f transferred from %d to %d\n", amount, account_id, to_account_id);
  printf("transfer() after assigning message");
  // Save balance change
  pBackup = sqlite3_backup_init(db, "main", db, "main");
  if (pBackup) {
    (void) sqlite3_backup_step(pBackup, -1);
    (void) sqlite3_backup_finish(pBackup);
  }
  rc = sqlite3_errcode(db);
  
  sqlite3_close(db);
  return message;
}
