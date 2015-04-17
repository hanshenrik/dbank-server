#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sqlite3.h>
#include <mpi.h>

#define MSGsize 100
#define MASTER  0

// stuff to use
// MPI_Is_thread_main
// MPI_Initialized
// MPI_Wait
// MPI_Waitall
// MPI_Waitany


static int callback(void *notUsed, int argc, char **argv, char **azColName){
  int i;
  printf("------------------------------\n");
  for (i = 0; i < argc; i++) {
    printf("%s = %s\n", azColName[i], argv[i] ? argv[i] : "NULL");
  }
  printf("------------------------------\n");
  return 0;
}

int main(int argc, char **argv)
{ // MPI variables
  int my_rank, NP;
  int source, dest;
  int tag = 50;
  int length;
  char name[MPI_MAX_PROCESSOR_NAME + 1];
  char message[MSGsize];
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

    printf("HO: started\n");
    // printf("HO: MPI_MAX_PROCESSOR_NAME = %d\n", MPI_MAX_PROCESSOR_NAME);
    
    for (source = 1; source < NP; source++)
    { MPI_Recv(message, MSGsize, MPI_CHAR, MPI_ANY_SOURCE, tag, MPI_COMM_WORLD, &status);
      printf("HO: %s\n", message);
    }
  }

  else // slave
  { // Branch variables
    pid_t pid = 0;
    int number_of_accounts = 3;
    int account_status = 0, wpid, i;

    // printf("BO: started\n");

    MPI_Get_processor_name(name, &length);
    printf("BO: name = %s, length = %d\n", name, length);

    // db testing
    sqlite3 *db;
    sqlite3_backup *pBackup;
    char *zErrMsg = 0;
    int rc;
    
    rc = sqlite3_open("testdb.db", &db);

    if (rc) {
      fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db));
      sqlite3_close(db);
      return(1);
    }
    
    // wait for different amount of time before executing query
    sleep(my_rank);
    rc = sqlite3_exec(db, "INSERT INTO table1 VALUES('proc', 13);", callback, 0, &zErrMsg);
    if(rc != SQLITE_OK) {
      fprintf(stderr, "SQL error: %s\n", zErrMsg);
      sqlite3_free(zErrMsg);
    }

    // save it
    pBackup = sqlite3_backup_init(db, "main", db, "main");
    if (pBackup) {
      (void) sqlite3_backup_step(pBackup, -1);
      (void) sqlite3_backup_finish(pBackup);
    }
    rc = sqlite3_errcode(db);
    
    // close it
    sqlite3_close(db);
    

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
