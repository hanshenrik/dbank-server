#include <stdio.h>
#include <string.h>
#include <mpi.h>

#define MSGsize 100

int main(int argc, char **argv)
{ int my_rank, NP;
  int source, dest;
  int tag = 50;
  int length;
  char name[MPI_MAX_PROCESSOR_NAME + 1];
  char message[MSGsize];
  MPI_Status status;
  MPI_Init(&argc, &argv); // argc and argv passed by address to all MPI processes
  MPI_Comm_rank(MPI_COMM_WORLD, &my_rank); // returns taskID=rank of calling process
  MPI_Comm_size(MPI_COMM_WORLD, &NP); // returns total no of MPI processes available
  
  if (my_rank != 0) // slave
  { MPI_Get_processor_name(name, &length);
    sprintf(message, "From process %d on processor %s", my_rank, name);
    dest = 0;
    MPI_Send(message, strlen(message) + 1, MPI_CHAR, dest, tag, MPI_COMM_WORLD);
  }
  else // master
  { for (source = 1; source < NP; source++)
    { MPI_Recv(message, MSGsize, MPI_CHAR, source, tag, MPI_COMM_WORLD, &status);
      printf("%s\n", message);
    }
  }
  
  MPI_Finalize();
  return 0;
}
