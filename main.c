#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sqlite3.h>
#include <mpi.h>

#define MSG_SIZE 100
#define QUERY_SIZE 500
#define MASTER  0

// stuff to use
// MPI_Is_thread_main
// MPI_Initialized
// MPI_Wait
// MPI_Waitall
// MPI_Waitany


static int callback(void *pswd, int argc, char **argv, char **azColName){
  int i;
  char *inputPassword = (char*) pswd;
  char *dbPassword = argv[1];
  printf("CB: argc = %d\n", argc);
  printf("CB: password = %s\n", inputPassword);
  printf("CB: dbPassword = %s\n", dbPassword);
  if (strcmp(inputPassword, dbPassword) == 0) {
    printf("CB: match!\n");
    return 0;
  } else {
    printf("CB: no match!\n");
  }
  return 1;
}

int main(int argc, char **argv)
{ // MPI variables
  int my_rank, NP;
  int source, dest;
  int tag = 50;
  int length;
  char name[MPI_MAX_PROCESSOR_NAME + 1];
  char message[MSG_SIZE];
  MPI_Status status;
  MPI_Init(&argc, &argv); // argc and argv passed by address to all MPI processes
  MPI_Comm_rank(MPI_COMM_WORLD, &my_rank); // returns taskID = rank of calling process
  MPI_Comm_size(MPI_COMM_WORLD, &NP); // returns total no of MPI processes available

  // shared bank variables
  sqlite3 *db;
  sqlite3_backup *pBackup;
  char *zErrMsg = 0;
  int rc;
  char query[QUERY_SIZE];

  int i;
  for (i = 0; i < argc; i++) {
    // printf("main: argc = %d, argv[%d] = %s\n", argc, i, argv[i]);
  }
  
  if (my_rank == MASTER) // master
  { // Head Office variables

    // DEV should be retrieved from socket request
    char username[30] = "kreps";
    char password[30] = "krepsx"; // should only receive salt and hash.
    int account_id = 1;
    int operation = 0; // 0 = getBalance, 1 = deposit, 2 = withdraw, 3 = transfer

    printf("HO: started\n");
    MPI_Get_processor_name(name, &length);
    printf("HO: name = %s\n", name);
    printf("HO: my_rank = %d\n", my_rank);

    // authenticate user against users.db
    rc = sqlite3_open("users.db", &db);

    if (rc) {
      fprintf(stderr, "HO: Can't open database: %s\n", sqlite3_errmsg(db));
      sqlite3_close(db);
      return(1);
    }
    
    sprintf(query, "SELECT username, password FROM users WHERE username = '%s'", username); // switch password to salt
    rc = sqlite3_exec(db, query, callback, (void*) password, &zErrMsg);
    printf("HO: rc = %d\n", rc);
    
    if(rc != SQLITE_OK) {
      fprintf(stderr, "SQL error: %s\n", zErrMsg);
      sqlite3_free(zErrMsg);
    }
    sqlite3_close(db);

    else { // user was valid, continue
      printf("HO: user authenticated!\n");

      // find out which branch to contact

    }
    
    // for (source = 1; source < NP; source++)
    // { MPI_Recv(message, MSG_SIZE, MPI_CHAR, MPI_ANY_SOURCE, tag, MPI_COMM_WORLD, &status);
    //   printf("HO: %s\n", message);
    // }
  }

  else // slave
  { // Branch variables
    pid_t pid = 0;
    int number_of_accounts = 3;
    int account_status = 0, wpid, i;

    // printf("BO: started\n");

    MPI_Get_processor_name(name, &length);
    printf("BO: name = %s, length = %d\n", name, length);

    // start child processes (bank accounts)
    for (i = 0; i < number_of_accounts; i++)
    { pid = fork();
      if (pid < 0 ) {
        printf("BO: pid < 0, fork failed!\n");
        continue;
      }
      if (pid == 0) {
        printf("BO%d: pid == 0, i = %d\n", my_rank, i);
        // while (1) {
        //   sleep(1);
        //   printf("%d_%d\n", my_rank, i);
        // }
        return 0;
      } else {
        // printf("BO: pid != 0\n");
      }
    }

    // wait for all children to exit
    // while ( (wpid = wait(&account_status) ) > 0) {}

    // wait for my_tank seconds to distinguish slaves in output
    sleep(my_rank);

    // create message
    sprintf(message, "From process %d on processor %s", my_rank, name);
    
    dest = 0;
    // send message to master
    MPI_Send(message, strlen(message) + 1, MPI_CHAR, dest, tag, MPI_COMM_WORLD);
  }

  MPI_Finalize();
  return 0;
}

double getBalance(int account_number) {
  return 0.0;
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
