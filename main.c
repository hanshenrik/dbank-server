#include <stdio.h>      // General stuff
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h> // Sockets
#include <arpa/inet.h>
#include <sqlite3.h>    // Database - sqlite3
#include <mpi.h>        // MPI

#define MESSAGE_SIZE 100          // MPI related
#define MASTER  0
#define QUERY_SIZE 500            // sqlite3 related
// TODO: define all db cols here
#define CLIENT_MESSAGE_SIZE 2000  // Sockets related
#define MAX_CONN_REQ_IN_QUEUE 3
#define PORT_NUMBER 5108

double balance = 13.37;

// stuff to use
// MPI_Is_thread_main
// MPI_Initialized
// MPI_Wait
// MPI_Waitall
// MPI_Waitany


// look into https://github.com/bumptech/stud for encryption!
// http://simplestcodings.blogspot.co.uk/2010/08/secure-server-client-using-openssl-in-c.html

void *connection_handler(void *);

double get_balance(int account_number) {
  // rc = sqlite3_open("accounts.db", &db); // change to v2 for read-only flags etc.

  // if (rc) {
  //   fprintf(stderr, "HO: Can't open database: %s\n", sqlite3_errmsg(db));
  //   sqlite3_close(db);
  //   return(1);
  // }
  
  // sqlite3_stmt *statement;
  // sprintf(query, "SELECT balance FROM accounts WHERE id = '%s'", account_number);
  // sqlite3_prepare_v2(db, query, strlen(query) + 1, &statement, NULL);
  // // TODO: Bind values to host parameters using the sqlite3_bind_*() interfaces.

  // int row = sqlite3_step(statement);
  // printf("row = %d\n", row);
  // if (row == SQLITE_ROW) {
  //   double balance;
  //   balance = sqlite3_column_int(statement, 0);
  // } else if (row == SQLITE_DONE) {
  //   break;
  // } else {
  //   fprintf(stderr, "Failed..\n");
  //   return 1;
  // }
  // sqlite3_finalize(statement);
  // sqlite3_close(db);

  //MPI_Send(&balance, sizeof(balance) + 1, MPI_DOUBLE, MASTER, 50, MPI_COMM_WORLD);
  return balance;
}

int main(int argc, char **argv)
{ // MPI variables
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
  for (i = 0; i < argc; i++) {
    // printf("main: argc = %d, argv[%d] = %s\n", argc, i, argv[i]);
  }
  
  if (my_rank == MASTER) // master
  { // Head Office variables
    // Print startup info
    printf("HO: started\n");
    MPI_Get_processor_name(name, &length);
    printf("HO: name = %s, my_rank = %d\n", name, my_rank);
    printf("HO: balance = %f\n", balance);

    /* Socket stuff */
    int socket_descriptor, client_sock, c, *new_sock;
    struct sockaddr_in server, client;
     
    // Create socket
    socket_descriptor = socket(AF_INET, SOCK_STREAM, 0);
    if (socket_descriptor == -1) {
      printf("S: Could not create socket");
    }
    puts("S: Socket created");
     
    // Prepare sockaddr_in structure
    server.sin_family = AF_INET;
    server.sin_addr.s_addr = INADDR_ANY;
    server.sin_port = htons( PORT_NUMBER );
     
    // Bind
    if ( bind(socket_descriptor,(struct sockaddr *)&server , sizeof(server)) < 0) {
      perror("S: Bind failed. Error");
      return 1;
    }
    puts("S: Bind done");
     
    // Listen for new requests
    listen(socket_descriptor, MAX_CONN_REQ_IN_QUEUE);
    puts("S: Waiting for incoming connections...");
     
     
    c = sizeof(struct sockaddr_in);
    // Accept incoming connection
    while ( (client_sock = accept(socket_descriptor, (struct sockaddr *)&client, (socklen_t*)&c)) ) {
      puts("S: Connection accepted!");
      
      pthread_t sniffer_thread;
      new_sock = malloc(1);
      *new_sock = client_sock;
      
      if ( pthread_create(&sniffer_thread, NULL,  connection_handler, (void*) new_sock) < 0) {
        perror("S: Could not create thread");
        return 1;
      }
       
      // Now join the thread, so that we dont terminate before the thread
      pthread_join(sniffer_thread, NULL);
      puts("S: Handler assigned");
    }
     
    if (client_sock < 0) {
      perror("S: Accept failed");
      return 1;
    }
    
    // for (source = 1; source < NP; source++)
    // { MPI_Recv(message, MESSAGE_SIZE, MPI_CHAR, MPI_ANY_SOURCE, tag, MPI_COMM_WORLD, &status);
    //   printf("HO: %s\n", message);
    // }
  }

  else // slave
  { // Branch variables
    pid_t pid = 0;
    int number_of_accounts = 3;
    int account_status = 0, wpid, i;

    sqlite3 *db;
    sqlite3_backup *pBackup;
    char *zErrMsg = 0;
    int rc;
    char query[QUERY_SIZE];

    MPI_Get_processor_name(name, &length);
    printf("BO: name = %s, my_rank = %d\n", name, my_rank);

    // get number of accounts to start
    rc = sqlite3_open("accounts.db", &db); // change to v2 for read-only flags etc.

    if (rc) {
      fprintf(stderr, "HO: Can't open database: %s\n", sqlite3_errmsg(db));
      sqlite3_close(db);
      return(1);
    }

    // find all account ids for this branch
    sqlite3_stmt *statement;
    sprintf(query, "SELECT id, balance FROM accounts WHERE branch = %d", my_rank);
    sqlite3_prepare_v2(db, query, strlen(query) + 1, &statement, NULL);
    // TODO: Bind values to host parameters using the sqlite3_bind_*() interfaces.
    while (1) {
      int row = sqlite3_step(statement);
      if (row == SQLITE_ROW) {
        int id;
        id = sqlite3_column_int(statement, 0);
        balance = sqlite3_column_double(statement, 1);
        pid = fork();
        if (pid < 0 ) {
          printf("BO: pid < 0, fork failed!\n");
          continue;
        }
        if (pid == 0) {
          printf("BO%d: pid == 0, id = %d, balance = %f\n", my_rank, id, balance);
          return 0;
        } else {
          // printf("BO: pid != 0\n");
        }
      } else if (row == SQLITE_DONE) {
        printf("DONE\n");
        break;
      } else {
        fprintf(stderr, "BO: Row Failed..\n");
        return 1;
      }
    }
    sqlite3_finalize(statement);
    sqlite3_close(db);

    // start child processes (bank accounts)
    // for (i = 0; i < number_of_accounts; i++)
    // { pid = fork();
    //   if (pid < 0 ) {
    //     printf("BO: pid < 0, fork failed!\n");
    //     continue;
    //   }
    //   if (pid == 0) {
    //     printf("BO%d: pid == 0, i = %d\n", my_rank, i);
    //     // while (1) {
    //     //   sleep(1);
    //     //   printf("%d_%d\n", my_rank, i);
    //     // }
    //     return 0;
    //   } else {
    //     // printf("BO: pid != 0\n");
    //   }
    // }
    
    if (my_rank == 2) {
      // receive anything
      // MPI_Recv(message, MESSAGE_SIZE, MPI_CHAR, MASTER, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
      printf("BO2 recv from master: %s\n", message);
      get_balance(2);
    }

    // wait for all children to exit
    // while ( (wpid = wait(&account_status) ) > 0) {}

    // wait for my_tank seconds to distinguish slaves in output
    // sleep(my_rank);

    // create message
    sprintf(message, "From process %d on processor %s", my_rank, name);
    
    dest = 0;
    // send message to master
    // MPI_Send(message, strlen(message) + 1, MPI_CHAR, dest, tag, MPI_COMM_WORLD);
  }

  MPI_Finalize();
  return 0;
}


int deposit(int account_number, double amount)
{
  return 0;
}

int withdraw(int account_number, double amount)
{
  // TODO: check balance
  return 0;
}

int transfer(int from_account_number, int to_account_number, double amount)
{
  // TODO: check balance
  return 0;
}


/* Handles client connections */
void *connection_handler(void *socket_descriptor) {
  int read_size;
  char *server_message, client_message[CLIENT_MESSAGE_SIZE];
  const char *separator = ";";
  
  // DB variables
  sqlite3 *db;
  sqlite3_backup *pBackup;
  char *zErrMsg = 0;
  int rc;
  char query[QUERY_SIZE];

  // MPI variables
  char message[MESSAGE_SIZE];

  // Get socket descriptor
  int sock = *(int*) socket_descriptor;
  
  // Answer client
  server_message = "S: Connection successfull! Give me: username;password;account_id;operation;amount";
  write(sock, server_message, strlen(server_message));
  
  // Wait for messages from client
  while ( (read_size = recv(sock, client_message, CLIENT_MESSAGE_SIZE, 0)) > 0 ) {
    int i;
    char *token, *parameters[5];
    char *username, *password;
    int account_id, operation;
    double amount;

    i = 0;
    token = strtok(client_message, separator);
    // Separate request parameters
    while (token != NULL) {
      parameters[i++] = token;
      token = strtok(NULL, separator);
    }
    
    // Put parameters in variables
    username    = parameters[0];
    password    = parameters[1]; // should only receive salt and hash.
    account_id  = atoi(parameters[2]);
    operation   = atoi(parameters[3]); // 0 = get_balance, 1 = deposit, 2 = withdraw, 3 = transfer
    amount      = atof(parameters[4]);

    printf("S: %s, %s, %d, %d, %f\n", username, password, account_id, operation, amount);

    // authenticate user against users.db
    rc = sqlite3_open("users.db", &db); // change to v2 for read-only flags etc.

    if (rc) {
      fprintf(stderr, "HO: Can't open database: %s\n", sqlite3_errmsg(db));
      sqlite3_close(db);
      return(1);
    }
    
    sqlite3_stmt *statement;
    int isAuth = 0;
    sprintf(query, "SELECT username, password FROM users WHERE username = '%s'", username); // switch password to salt
    sqlite3_prepare_v2(db, query, strlen(query) + 1, &statement, NULL);
    // TODO: Bind values to host parameters using the sqlite3_bind_*() interfaces.
    int row = sqlite3_step(statement);
    if (row == SQLITE_ROW) {
      const unsigned char *dbPassword;
      dbPassword = sqlite3_column_text(statement, 1);
      if ( strcmp(password, dbPassword) == 0 ) {
        printf("CB: match!\n");
        isAuth = 1;
      } else {
        printf("CB: password didn't match!\n");
      }
    } else {
      fprintf(stderr, "Failed..\n");
      return 1;
    }
    sqlite3_finalize(statement);
    
    if (isAuth) { // user was valid, continue
      printf("HO: user authenticated!\n");

      // Finished with users.db, close it and open accounts.db
      sqlite3_close(db);
      rc = sqlite3_open("accounts.db", &db); // change to v2 for read-only flags etc.

      // find out which branch to contact
      sqlite3_stmt *statement;
      sprintf(query, "SELECT branch FROM accounts WHERE id = %d AND username = '%s'", account_id, username);
      printf("query: %s\n", query);
      sqlite3_prepare_v2(db, query, strlen(query) + 1, &statement, NULL);
      // TODO: Bind values to host parameters using the sqlite3_bind_*() interfaces.
      int row = sqlite3_step(statement);
      int branch;
      if (row == SQLITE_ROW) {
        // const unsigned char *text;
        // text = sqlite3_column_text(statement, 0);
        branch = sqlite3_column_int(statement, 0);
        printf("%d: %d\n", row, branch);
      } else {
        fprintf(stderr, "Failed..\n");
        return 1;
      }
      sqlite3_finalize(statement);

      double b;
      sprintf(message, "%s | %d | %d | %f", username, account_id, operation, amount);
      printf("HO: sending to branch %d with tag %d\n", branch, 0);
      // MPI_Send(message, strlen(message) + 1, MPI_CHAR, branch, 0, MPI_COMM_WORLD);
      // MPI_Recv(&b, sizeof(double), MPI_DOUBLE, branch, 0, MPI_COMM_WORLD, &status);
      printf("HO: b = %f\n", b);
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
