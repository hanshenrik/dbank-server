#include <iostream>

using namespace std;

class Account
{ public:
    void setID( int id );
    int getID( void );
    Account(int id);  // This is the constructor

  private:
    int id;
};
 
// Member functions definitions including constructor
Account::Account( int idxx)
{ printf("Object is being created, id = %d\n", idxx);
  id = idxx;
}

void Account::setID( int idxx ) { id = idxx; }

int Account::getID( void ) { return id; }

int main(int argc, char **argv)
{ Account account(17);
  printf("Account id: %d\n", account.getID());
  account.setID(21);
  printf("Account id: %d\n", account.getID());

  return 0;
}
