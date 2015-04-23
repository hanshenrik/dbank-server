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
  int rc;
  char query[QUERY_SIZE];

  // Get current balance
  bal = get_balance(account_id);
  
  // Make sure amount is positive
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
  int rc;
  char query[QUERY_SIZE];

  // Make sure current balance is bigger than wanted withdraw amount and amount is positive
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

  // Make sure current balance is bigger than wanted transfer amount and amount is positive
  bal = get_balance(account_id);
  if (amount > bal) {
    sprintf(message, "Insufficient funds.\n");
    printf("transfer(): %s\n", message);
    return message;
  } else if (amount < 0) {
    sprintf(message, "Can't withdraw negtive amount.\n");
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
    sprintf(message, "Couldn't find account_id '%d'\n", account_id);
    fprintf(stderr, message);
    sqlite3_finalize(statement);
    sqlite3_close(db);
    return message;
  }
  sqlite3_finalize(statement);

  bal = get_balance(to_account_id);
  if (bal < 0) {
    sprintf(message, "Couldn't find to_account_id '%d'\n", to_account_id);
    fprintf(stderr, message);
    sqlite3_close(db);
    return message;
  }
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

  sprintf(message, "£%.2f transferred from %d to %d\n", amount, account_id, to_account_id);
  
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
