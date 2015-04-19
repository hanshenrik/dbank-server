#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sqlite3.h>

#define QUERY_SIZE 500

static int callback(void *notUsed, int argc, char **argv, char **azColName){
  int i;
  printf("------------------------------\n");
  for (i = 0; i < argc; i++) {
    printf("%s = %s\n", azColName[i], argv[i] ? argv[i] : "NULL");
  }
  printf("------------------------------\n");
  return 0;
}

int main(int argc, char **argv) {
  sqlite3 *db;
  sqlite3_backup *pBackup;
  char *zErrMsg = 0;
  int rc;
  char query[QUERY_SIZE];
  
  // users.db
  rc = sqlite3_open("users.db", &db);

  if (rc) {
    fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db));
    sqlite3_close(db);
    return(1);
  }
  
  sprintf(query, "CREATE TABLE users(username VARCHAR(30) UNIQUE, password VARCHAR(30), branch INT); INSERT INTO users VALUES('kreps', 'krepsx', 1); INSERT INTO users VALUES('reke', 'rekex', 2); INSERT INTO users VALUES('laks', 'laksx', 5);");
  rc = sqlite3_exec(db, query, callback, 0, &zErrMsg);
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


  // accounts.db
  rc = sqlite3_open("accounts.db", &db);

  if (rc) {
    fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db));
    sqlite3_close(db);
    return(1);
  }
  
  sprintf(query, "CREATE TABLE accounts(id INT, branch INT, username VARCHAR(30), balance DOUBLE); INSERT INTO accounts VALUES(1, 1, 'kreps', 100); INSERT INTO accounts VALUES(2, 2, 'kreps', 50); INSERT INTO accounts VALUES(3, 1, 'reke', 50);");
  rc = sqlite3_exec(db, query, callback, 0, &zErrMsg);
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

  return(0);
}
