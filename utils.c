/***************************************************** 
 * Note: this file is provided for the convenience of
 *       students. Students can choose to edit, delete
 *       or even leave the file as-is. This file will
 *       NOT be replaced during grading.
 ****************************************************/
#include "utils.h"
#include <stdio.h>
#include <stdlib.h>


/* Helper func to choose a partition based on `key`,*/
int partition(char *key, int num_partitions)
{
    unsigned long hash = 5381;
    int c;

    while (c = *key++) {
        hash = ((hash << 5) + hash) + c;
    }

    return hash % num_partitions;
}

char *read_from_file(char const *file_path)
{
    FILE *file = fopen(file_path, "r");
    long file_length;
    char *buffer;
    size_t bytes_read;

    if(!file)
    {
        perror(file_path);
        exit(1);
    }

    fseek(file, 0L, SEEK_END);
    file_length = ftell(file);
    rewind(file);
    buffer = (char *) malloc(file_length + 1); // allocate space for '\0'

    if (!buffer)
    {
        fclose(file);
        perror("malloc failed");
        exit(1);
    }

    bytes_read = fread(buffer, 1, file_length, file);

    if (bytes_read == 0) // might check if lesser elements are read in the future
    {
        fclose(file);
        free(buffer);
        perror("read failed");
        exit(1);
    }

    buffer[file_length] = '\0';
    fclose(file);

    return buffer;
}

void write_to_file(const char* file_path, const char* data) {
    FILE* file = fopen (file_path, "w");
    if (!file) {
        perror(file_path);
        exit(1);
    }

    fputs(data, file);

    fclose(file);
}

void append_to_file(const char* file_path, const char* data) {
    FILE* file = fopen (file_path, "a");
    if (!file) {
        perror(file_path);
        exit(1);
    }

    fputs(data, file);

    fclose(file);
}
