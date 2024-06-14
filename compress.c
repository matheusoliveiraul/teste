#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <zconf.h>
#include <zlib.h>
 
#define MASTER_RANK 0
 
int main(int argc, char **argv) {
  int rank, size;
  MPI_Status status;
 
  // Inicializa MPI
  MPI_Init(&argc, &argv);
 
  // Obtém o rank e o tamanho do comunicador MPI
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &size);
 
  // Se for o processo mestre (servidor):
  if (rank == MASTER_RANK) {
    // Abre o arquivo de entrada para leitura
    FILE *fp_in = fopen("dados_entrada.txt", "r");
    if (!fp_in) {
      printf("Erro ao abrir arquivo de entrada!\n");
      MPI_Finalize();
      exit(1);
    }
 
    // Recebe o tamanho dos dados compactados dos trabalhadores
    int data_size;
    for (int i = 1; i < size; i++) {
      MPI_Recv(&data_size, 1, MPI_INT, i, 0, MPI_COMM_WORLD, &status);
    }
 
    // Aloca memória para os dados compactados
    unsigned char *data_compressed = malloc(data_size);
    if (!data_compressed) {
      printf("Erro ao alocar memória!\n");
      MPI_Finalize();
      exit(1);
    }
 
    // Recebe os dados compactados dos trabalhadores
    int offset = 0;
    for (int i = 1; i < size; i++) {
      int chunk_size;
      MPI_Recv(&chunk_size, 1, MPI_INT, i, 0, MPI_COMM_WORLD, &status);
 
      MPI_Recv(data_compressed + offset, chunk_size, MPI_BYTE, i, 0, MPI_COMM_WORLD, &status);
      offset += chunk_size;
    }
 
    // Abre o arquivo de saída para gravação
    FILE *fp_out = fopen("dados_compactados.z", "wb");
    if (!fp_out) {
      printf("Erro ao abrir arquivo de saída!\n");
      MPI_Finalize();
      exit(1);
    }
 
    // Escreve os dados compactados no arquivo de saída
    fwrite(data_compressed, 1, data_size, fp_out);
    fclose(fp_out);
 
    // Libera memória alocada
    free(data_compressed);
 
    // Fecha o arquivo de entrada
    fclose(fp_in);
 
    // Envia mensagem de finalização para os trabalhadores
    for (int i = 1; i < size; i++) {
      MPI_Send(NULL, 0, MPI_BYTE, i, 0, MPI_COMM_WORLD);
    }
 
  } else {
    // Se for um processo trabalhador (VM):
    
    // Abre o arquivo de entrada para leitura
    FILE *fp_in = fopen("dados_entrada.txt", "r");
    if (!fp_in) {
      printf("Erro ao abrir arquivo de entrada!\n");
      MPI_Finalize();
      exit(1);
    }
 
    // Lê os dados do arquivo de entrada
    char *data_buffer = malloc(BUFFER_SIZE);
    if (!data_buffer) {
      printf("Erro ao alocar memória!\n");
      MPI_Finalize();
      exit(1);
    }
 
    int bytes_read = 0;
    while ((bytes_read = fread(data_buffer, 1, BUFFER_SIZE, fp_in)) > 0) {
      // Compacta os dados lidos
      unsigned char *data_compressed = malloc(BUFFER_SIZE * 2);
 