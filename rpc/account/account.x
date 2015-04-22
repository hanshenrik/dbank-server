/* account.x: Remote account protocol */
program ACCOUNTPROG {
  version ACCOUNTVERS {
    double GETBALANCE(void) = 1;
    double DEPOSIT(double) = 2;
    double WITHDRAW(double) = 3;
  } = 1;
} = 0x20000022;