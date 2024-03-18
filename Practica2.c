#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<pthread.h>
#include<unistd.h>
#include<glib.h>
#include <stdbool.h>


#define MAX_LINE_LENGTH 1000

struct usuarios{
    int no_cuenta;
    char nombre[50];
    double saldo;
};

struct transacciones{
    int operacion;
    int cuenta1;
    int cuenta2;
    double monto;
};

typedef struct {
    int hilo_num;
    char *filename;
    int inicio; // Índice de inicio de las líneas a procesar
    int fin;    // Índice final de las líneas a procesar
} ThreadArgs;

GList *lista_usuario = NULL;
GList *lista_transacciones = NULL;

pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER; // Mutex para asegurar acceso exclusivo a la lista
pthread_cond_t cond = PTHREAD_COND_INITIALIZER; // Variable de condición para sincronización
int usuarios_registrados = 0; // Contador de usuarios registrados
int total_hilos = 3; // Número total de hilos que insertarán usuarios
int *registros_por_hilo; // Arreglo para almacenar la cantidad de registros por hilo
int usuarios_por_hilo; // Número de usuarios a ser insertados por cada hilo

//variables para transacciones
int transacciones_realizadas = 0; // Contador de transacciones realizadas
int total_hilos_transacciones = 4; // Número total de hilos
int *registros_transacciones_por_hilo; // Arreglo para almacenar la cantidad de transacciones por hilo
int transacciones_por_hilo; // Número de transacciones a ser insertados por cada hilo

typedef struct{
    int value;
    pthread_mutex_t mutex;
    pthread_cond_t condition;
} semaforo;

semaforo sem;

void semaforo_init(semaforo* sem, int initial_value){
    pthread_mutex_init(&(sem->mutex), NULL);
    pthread_cond_init(&(sem->condition), NULL);
    sem->value = initial_value;
}

void semaforo_wait(semaforo* sem){
    pthread_mutex_lock(&(sem->mutex));
    while (sem->value <= 0){
        pthread_cond_wait(&(sem->condition), &(sem->mutex));
    }
    sem->value--;
    pthread_mutex_unlock(&(sem->mutex));
}

void semaforo_signal(semaforo* sem){
    pthread_mutex_lock(&(sem->mutex));
    sem->value++;
    pthread_cond_signal(&(sem->condition));
    pthread_mutex_unlock(&(sem->mutex));
}

void* insertarUsuarios(void *args) {
    ThreadArgs *threadArgs = (ThreadArgs *)args;
    int hilo_num = threadArgs->hilo_num;
    char *filename = threadArgs->filename;
    int inicio = threadArgs->inicio;
    int fin = threadArgs->fin;

    printf("Hilo %d comenzando...\n", hilo_num);

    FILE *fp;
    char row[MAX_LINE_LENGTH];
    char *token;

    fp = fopen(filename, "r");
    if (fp == NULL) {
        printf("Error al abrir el archivo.\n");
        pthread_exit(NULL);
    }

    // Ir al inicio de la sección correspondiente al hilo actual
    for (int i = 0; i < inicio; i++) {
        if (fgets(row, MAX_LINE_LENGTH, fp) == NULL) {
            printf("Error: No hay suficientes líneas en el archivo.\n");
            fclose(fp);
            pthread_exit(NULL);
        }
    }



    // Procesar las líneas asignadas a este hilo
    while (fgets(row, MAX_LINE_LENGTH, fp) != NULL && inicio <= fin) {
        //si lee estos encabezados saltar a la siguiente linea No. Cuenta: 0, Nombre: nombre, Saldo: 0.00
        if (strstr(row, "no_cuenta") != NULL) {
            inicio++;
            continue;
        }
        struct usuarios *nuevo_usuario = malloc(sizeof(struct usuarios)); // Crear nueva instancia de usuarios

        // Dividir la línea en tokens usando la coma como delimitador
        token = strtok(row, ",");
        nuevo_usuario->no_cuenta = atoi(token);

        token = strtok(NULL, ",");
        sprintf(nuevo_usuario->nombre, "%s", token);

        token = strtok(NULL, ",");
        nuevo_usuario->saldo = atof(token);

        // Bloquear el mutex antes de acceder a la lista compartida
        pthread_mutex_lock(&mutex);
        // Agregar el usuario a la lista
        lista_usuario = g_list_append(lista_usuario, nuevo_usuario);
        usuarios_registrados++;
        // Incrementar el contador de registros para este hilo
        registros_por_hilo[hilo_num - 1]++;
        pthread_mutex_unlock(&mutex);

        inicio++;
    }

    printf("Hilo %d terminado.\n", hilo_num);

    fclose(fp); // Cerrar el archivo después de usarlo
    
    // Señalar al hilo controlador que este hilo ha terminado
    semaforo_signal(&sem);
    
    pthread_exit(NULL);
}

void* controlador(void *args) {
    // Esperar a que todos los hilos terminen de insertar usuarios
    for (int i = 0; i < total_hilos; i++) {
        semaforo_wait(&sem);
    }
    
    printf("Todos los usuarios han sido registrados.\n");
    return NULL;
}

void cargarMasivamenteUsuarios(){
    semaforo_init(&sem, 0); // Inicializar el semáforo con valor 0
    pthread_t threads[total_hilos + 1]; // +1 para el controlador

    char *filename = "prueba_usuarios.csv";
    
    // Calcular el número total de registros en el archivo
    FILE *file = fopen(filename, "r");
    int total_registros = 0;
    char buffer[MAX_LINE_LENGTH];
    while (fgets(buffer, MAX_LINE_LENGTH, file) != NULL) {
        total_registros++;
    }
    fclose(file);
    
    usuarios_por_hilo = total_registros / total_hilos; // Número de usuarios a procesar por hilo
    registros_por_hilo = calloc(total_hilos, sizeof(int)); // Inicializar el arreglo de contadores de registros

    // Crear los hilos para insertar usuarios
    for (int i = 0; i < total_hilos; i++) {
        ThreadArgs *threadArgs = malloc(sizeof(ThreadArgs));
        threadArgs->hilo_num = i + 1;
        threadArgs->filename = filename;
        threadArgs->inicio = i * usuarios_por_hilo;
        threadArgs->fin = (i + 1) * usuarios_por_hilo - 1;
        if (i == total_hilos - 1) {
            // Asegurar que el último hilo procese hasta el final del archivo
            threadArgs->fin = total_registros - 1;
        }

        pthread_create(&threads[i], NULL, insertarUsuarios, threadArgs);
    }

    // Crear el hilo controlador
    pthread_create(&threads[total_hilos], NULL, controlador, NULL);

    // Esperar a que todos los hilos terminen
    for (int i = 0; i <= total_hilos; i++) {
        pthread_join(threads[i], NULL);
    }

    // Imprimir los contadores de registros por hilo
    printf("Contadores de registros por hilo:\n");
    for (int i = 0; i < total_hilos; i++) {
        printf("Hilo %d: %d\n", i + 1, registros_por_hilo[i]);
    }
    //imprimir la lista de usuarios
    printf("Lista de usuarios:\n");
    GList *temp = lista_usuario;
    while (temp != NULL) {
        struct usuarios *usuario = (struct usuarios *)temp->data;
        printf("No. Cuenta: %d, Nombre: %s, Saldo: %.2f\n", usuario->no_cuenta, usuario->nombre, usuario->saldo);
        temp = g_list_next(temp);
    }
}

void* insertarTransacciones(void *args) {
    ThreadArgs *threadArgs = (ThreadArgs *)args;
    int hilo_num = threadArgs->hilo_num;
    char *filename = threadArgs->filename;
    int inicio = threadArgs->inicio;
    int fin = threadArgs->fin;

    printf("Hilo de transacciones %d comenzando...\n", hilo_num);

    FILE *fp;
    char row[MAX_LINE_LENGTH];
    char *token;

    fp = fopen(filename, "r");
    if (fp == NULL) {
        printf("Error al abrir el archivo.\n");
        pthread_exit(NULL);
    }

    // Ir al inicio de la sección correspondiente al hilo actual
    for (int i = 0; i < inicio; i++) {
        if (fgets(row, MAX_LINE_LENGTH, fp) == NULL) {
            printf("Error: No hay suficientes líneas en el archivo.\n");
            fclose(fp);
            pthread_exit(NULL);
        }
    }

    // Procesar las líneas asignadas a este hilo
    while (fgets(row, MAX_LINE_LENGTH, fp) != NULL && inicio <= fin) {
        //si lee estos encabezados saltar a la siguiente linea Operacion: 0, Cuenta1: 0, Cuenta2: 0, Monto: 0.00
        if (strstr(row, "operacion") != NULL) {
            inicio++;
            continue;
        }
        struct transacciones *nueva_transaccion = malloc(sizeof(struct transacciones)); // Crear nueva instancia de transacciones

        // Dividir la línea en tokens usando la coma como delimitador
        token = strtok(row, ",");
        nueva_transaccion->operacion = atoi(token);

        token = strtok(NULL, ",");
        nueva_transaccion->cuenta1 = atoi(token);

        token = strtok(NULL, ",");
        nueva_transaccion->cuenta2 = atoi(token);

        token = strtok(NULL, ",");
        nueva_transaccion->monto = atof(token);

        // Bloquear el mutex antes de acceder a la lista compartida
        pthread_mutex_lock(&mutex);
        // Agregar la transacción a la lista
        lista_transacciones = g_list_append(lista_transacciones, nueva_transaccion);
        transacciones_realizadas++;
        // Incrementar el contador de registros para este hilo
        registros_transacciones_por_hilo[hilo_num - 1]++;
        pthread_mutex_unlock(&mutex);

        inicio++;
    }

    printf("Hilo de transacciones %d terminado.\n", hilo_num);

    fclose(fp); // Cerrar el archivo después de usarlo
    
    // Señalar al hilo controlador que este hilo ha terminado
    semaforo_signal(&sem);
    
    pthread_exit(NULL);
}

void cargarMasivamenteTransacciones(){
    semaforo_init(&sem, 0); // Inicializar el semáforo con valor 0
    pthread_t threads[total_hilos_transacciones + 1]; // +1 para el controlador

    char *filename = "prueba_transacciones.csv";
    
    // Calcular el número total de registros en el archivo
    FILE *file = fopen(filename, "r");
    int total_registros = 0;
    char buffer[MAX_LINE_LENGTH];
    while (fgets(buffer, MAX_LINE_LENGTH, file) != NULL) {
        total_registros++;
    }
    fclose(file);
    
    transacciones_por_hilo = total_registros / total_hilos_transacciones; // Número de transacciones a procesar por hilo
    registros_transacciones_por_hilo = calloc(total_hilos_transacciones, sizeof(int)); // Inicializar el arreglo de contadores de registros

    // Crear los hilos para insertar transacciones
    for (int i = 0; i < total_hilos_transacciones; i++) {
        ThreadArgs *threadArgs = malloc(sizeof(ThreadArgs));
        threadArgs->hilo_num = i + 1;
        threadArgs->filename = filename;
        threadArgs->inicio = i * transacciones_por_hilo;
        threadArgs->fin = (i + 1) * transacciones_por_hilo - 1;
        if (i == total_hilos_transacciones - 1) {
            // Asegurar que el último hilo procese hasta el final del archivo
            threadArgs->fin = total_registros - 1;
        }

        pthread_create(&threads[i], NULL, insertarTransacciones, threadArgs);
    }

    // Crear el hilo controlador
    pthread_create(&threads[total_hilos_transacciones], NULL, controlador, NULL);

    // Esperar a que todos los hilos terminen
    for (int i = 0; i <= total_hilos_transacciones; i++) {
        pthread_join(threads[i], NULL);
    }

    // Imprimir los contadores de registros por hilo
    printf("Contadores de transacciones por hilo:\n");
    for (int i = 0; i < total_hilos_transacciones; i++) {
        printf("Hilo de transacciones %d: %d\n", i + 1, registros_transacciones_por_hilo[i]);
    }
    //imprimir la lista de transacciones
    printf("Lista de transacciones:\n");
    GList *temp = lista_transacciones;
    while (temp != NULL) {
        struct transacciones *transaccion = (struct transacciones *)temp->data;
        printf("Operacion: %d, Cuenta1: %d, Cuenta2: %d, Monto: %.2f\n", transaccion->operacion, transaccion->cuenta1, transaccion->cuenta2, transaccion->monto);
        temp = g_list_next(temp);
    }
}


bool menu(){
    int opcion;
    printf("1. Carga Masiva usuarios\n");
    printf("2. Carga Masiva transacciones\n");
    printf("3. Depositar\n");
    printf("4. Retirar\n");
    printf("5. Salir\n");
    printf("Opcion: ");
    scanf("%d", &opcion);
    switch(opcion){
        case 1:
            printf("Cargando usuarios...\n");

            cargarMasivamenteUsuarios();            
            break;
        case 2:
            cargarMasivamenteTransacciones();
            break;
        case 3:
            //depositar();
            break;
        case 4:
            //retirar();
            break;
        case 5:
            return FALSE;
            break;
        default:
            printf("Opcion no valida\n");
            break;
    }
    return TRUE;
}


int main() {
    
    while(menu());
   

    return 0;
}