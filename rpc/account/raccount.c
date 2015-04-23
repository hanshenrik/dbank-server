/*
 * raccount.c
 */
#include <stdio.h>
#include <stdlib.h>
#include "account.h"      /* generated by rpcgen */
 
main(argc, argv)
  int argc;
  char *argv[];
{
  CLIENT *clnt;
  void   *pVoid;
  double *result;
  char *server;
  int operation;
  double * amount;
  amount = malloc(sizeof(double));
 
  if (argc != 4) {
    fprintf(stderr, "usage: %s host operation amount\n", argv[0]);
    exit(1);
  }
 
  server = argv[1];
  operation = atoi(argv[2]);
  *amount = atof(argv[3]);
  /*
   * Create client "handle" used for calling MESSAGEPROG on the server designated on the command line.
   */
  clnt = clnt_create(server, ACCOUNTPROG, ACCOUNTVERS, "tcp");
  if (clnt == (CLIENT *)NULL) {
    /*
     * Couldn't establish connection with server. Print error message and die.
     */
    clnt_pcreateerror(server);
    exit(1);
  }
    /*
   * Call the remote procedure based on operation
   */
  if (operation == 1) {
    result = getbalance_1(pVoid, clnt);
  } else if (operation == 2) {
    result = deposit_1(amount, clnt);
  } else if (operation == 3) {
    result = withdraw_1(amount, clnt);
  }
  if (result == (double *)NULL) {
    /*
     * An error occurred while calling the server. Print error message and die.
     */
    clnt_perror(clnt, server);
    exit(1);
  }
  /* Okay, we successfully called the remote procedure. */
  if (*result == -1.0) {
    /*
     * Insufficient funds
     */
    fprintf(stderr, "%s: Insuficcient funds.\n",argv[0]);
    exit(1);
  }
  /* Success */
  printf("Done on: %s\n", server);
  clnt_destroy( clnt );
  exit(0);
}