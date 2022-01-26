#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include "tasks.h"
#include "utils.h"


int main(int argc, char** argv)
{
    MPI_Init(&argc, &argv);

    int world_size, rank;
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    /* Create MPI_Datatype for struct KeyValue */
    const int KV_COUNT = 2;
    const int KV_BLOCKLENGTHS[2] = {8, 1};
    const MPI_Aint KV_DISPLACEMENTS[2] = {offsetof(KeyValue, key), offsetof(KeyValue, val)};
    const MPI_Datatype KV_TYPES[2] = {MPI_CHAR, MPI_INT};
    MPI_Datatype MPI_KeyValue;

    MPI_Type_create_struct(KV_COUNT, KV_BLOCKLENGTHS, KV_DISPLACEMENTS, KV_TYPES, &MPI_KeyValue);
    MPI_Type_commit(&MPI_KeyValue);

    /* Get command-line params */
    char *input_files_dir = argv[1];
    int num_files = atoi(argv[2]);
    int num_map_workers = atoi(argv[3]);
    int num_reduce_workers = atoi(argv[4]);
    char *output_file_name = argv[5];
    int map_reduce_task_num = atoi(argv[6]);

    /* Create MPI groups for synchronisation */
	MPI_Group orig_group, new_group;
	MPI_Comm intra_comm, inter_comm;
    MPI_Request master_map_req;
    int ranks1[num_map_workers + 1];
    int ranks2[num_reduce_workers];

    for (int i = 0; i <= num_map_workers + num_reduce_workers; i++)
    {
        if (i <= num_map_workers)
        {
            ranks1[i] = i;
        }
        else
        {
            ranks2[i - num_map_workers - 1] = i;
        }
    }

	MPI_Comm_group(MPI_COMM_WORLD, &orig_group);

    if (rank <= num_map_workers)
    {
        MPI_Group_incl(orig_group, num_map_workers + 1, ranks1, &new_group);
    }
    else
    {
        MPI_Group_incl(orig_group, num_reduce_workers, ranks2, &new_group);
    }

    MPI_Comm_create(MPI_COMM_WORLD, new_group, &intra_comm);

    /* Identify the specific map function to use */
    MapTaskOutput* (*map) (char*);
    switch(map_reduce_task_num){
        case 1:
            map = &map1;
            break;
        case 2:
            map = &map2;
            break;
        case 3:
            map = &map3;
            break;
    }

    /* Distinguish between master, map workers and reduce workers */
    if (rank == 0)
    {
        /* Not using a program buffer for send because files is not re-used. */
        char *file_path = (char *) malloc((strlen(input_files_dir) + 12));
        char *file;
        char **files = (char **) malloc(num_files * sizeof(char *));
        int worker_rank;

        /* read the input files, then send to map workers */
        for (int i = 0; i < num_files; i++)
        {
            sprintf(file_path, "%s/%d.txt", input_files_dir, i);
            file = read_from_file(file_path);
            worker_rank = (i % num_map_workers) + 1;
            int message_length = strlen(file) + 1;
            files[i] = file;
            
            MPI_Send(file, message_length, MPI_CHAR, worker_rank, 0, MPI_COMM_WORLD);
        }

        MPI_Ibarrier(MPI_COMM_WORLD, &master_map_req);

        for (int i = 0; i < num_files; i++)
        {
            free(files[i]);
        }
        free(files);
        free(file_path);

        MPI_Status recv_stat;
        int message_length;
        KeyValue *recv_buffer;

        /* Since its painful to realloc a string all the time, we shall continuously write to the file.*/
        write_to_file(output_file_name, "");

        /* Int is at most 10 chars, key is 8, space and newline are 1 each.*/
        char write_buff[20];
        for (int i = 0; i < num_reduce_workers; i++)
        {
            MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &recv_stat);
            MPI_Recv(&message_length, 1, MPI_INT, recv_stat.MPI_SOURCE, recv_stat.MPI_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            if (message_length)
            {
                recv_buffer = (KeyValue *) malloc(message_length * sizeof(KeyValue));
                MPI_Recv(recv_buffer, message_length, MPI_KeyValue, recv_stat.MPI_SOURCE, recv_stat.MPI_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                for (int j = 0; j < message_length; j++)
                {
                    sprintf(write_buff, "%s %d\n", recv_buffer[j].key, recv_buffer[j].val);
                    append_to_file(output_file_name, write_buff);
                }
                free(recv_buffer);
            }
        }
    }
    else if ((rank >= 1) && (rank <= num_map_workers))
    {
        int num_files_per_worker = (num_files / num_map_workers);
        num_files_per_worker = rank <= num_files % num_map_workers ? num_files_per_worker + 1 : num_files_per_worker;

        MPI_Status status[num_files_per_worker];
        int message_length[num_files_per_worker];

        char **files = (char **) malloc(num_files_per_worker * sizeof(char *));

        for (int i = 0; i < num_files_per_worker; i++)
        {
            /* Probe for messasges coming from master to support receiving dynamically allocated variables. */
            MPI_Probe(0, 0, MPI_COMM_WORLD, &status[i]);
            MPI_Get_count(&status[i], MPI_CHAR, &message_length[i]);

            files[i] = (char *) malloc(message_length[i]);
            if (!files[i])
            {
                exit(1);
            }
            
            MPI_Recv(files[i], message_length[i], MPI_CHAR, 0, 0, MPI_COMM_WORLD, &status[i]);

            MapTaskOutput *output = map(files[i]);

            int pairs_per_worker[num_reduce_workers];
            int partition_arr[output->len];
            memset(pairs_per_worker, 0, num_reduce_workers * sizeof(int)); 

            /* Send output to reduce workers */
            for (int j = 0; j < output->len; j++)
            {
                partition_arr[j] = partition(output->kvs[j].key, num_reduce_workers);
                pairs_per_worker[partition_arr[j]]++;
            }

            for (int j = 0; j < num_reduce_workers; j++)
            {
                MPI_Send(&pairs_per_worker[j], 1, MPI_INT, num_map_workers + j + 1, i, MPI_COMM_WORLD);
            }

            for (int j = 0; j < output->len; j++)
            {
                if (i == num_files_per_worker - 1 && j == output->len - 1)
                {
                    MPI_Ssend(&output->kvs[j], 1, MPI_KeyValue, partition_arr[j] + 1 + num_map_workers, i, MPI_COMM_WORLD);
                }
                else
                {
                    MPI_Send(&output->kvs[j], 1, MPI_KeyValue, partition_arr[j] + 1 + num_map_workers, i, MPI_COMM_WORLD);
                }
            }

            free_map_task_output(output);
        }
        MPI_Ibarrier(MPI_COMM_WORLD, &master_map_req);

        for (int i = 0; i < num_files_per_worker; i++)
        {
            free(files[i]);
        }
        free(files);    
    }
    else
    {
        MPI_Status stat;
        MPI_Request personal_req[num_reduce_workers];

        int mapComplete = 0;
        int hasMessage = 0;
                
        /* initial size and setup for storing key-value pairs. We malloc more at the start since its expensive to realloc */
        int keyval_size = 1000000;
        int key_count = 0;
        KeyValueArr *kvm = (KeyValueArr *) malloc(keyval_size * sizeof(KeyValueArr));

        // change barrier to for loop recv num files
        MPI_Ibarrier(MPI_COMM_WORLD, &personal_req[rank - num_reduce_workers]);

        while (!mapComplete || hasMessage)
        {
            MPI_Request_get_status(personal_req[rank - num_reduce_workers], &mapComplete, &stat);
            MPI_Iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &hasMessage, &stat);
            if (hasMessage)
            {
                int kv_count;

                MPI_Recv(&kv_count, 1, MPI_INT, stat.MPI_SOURCE, stat.MPI_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                KeyValue *kv_buffer = (KeyValue *) malloc(kv_count * sizeof(KeyValue));
                if (!kv_buffer)
                {
                    exit(1);
                }

                for (int i = 0; i < kv_count; i++)
                {
                    MPI_Recv(kv_buffer + i, 1, MPI_KeyValue, stat.MPI_SOURCE, stat.MPI_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                }

                /* Iterate through all key value pairs received. */
                for (int i = 0; i < kv_count; i++)
                {   
                    int inserted = 0;

                    /* Linear probe to determine whether the key exists. */
                    for (int j = 0; j < key_count; j++)
                    {
                        if (!strcmp(kv_buffer[i].key, kvm[j].key))
                        {
                            inserted = 1;
                            if (kvm[j].len < kvm[j].max_size)
                            {
                                kvm[j].val[kvm[j].len] = kv_buffer[i].val;
                                kvm[j].len++;
                            }
                            else
                            {
                                kvm[j].max_size *= 2;
                                kvm[j].val = (int*) realloc(kvm[j].val, kvm[j].max_size * sizeof(int));
                                if (!kvm[j].val)
                                {
                                    exit(1);
                                }
                                kvm[j].val[kvm[j].len] = kv_buffer[i].val;
                                kvm[j].len++;
                            }
                            break;
                        }
                    }
                    if (!inserted)
                    {
                        if (key_count >= keyval_size)
                        {
                            keyval_size *= 2;
                            kvm = (KeyValueArr *) realloc(kvm, keyval_size * sizeof(KeyValueArr));
                            if (!kvm)
                            {
                                exit(1);
                            }
                        }
                        strcpy(kvm[key_count].key, kv_buffer[i].key);
                        kvm[key_count].max_size = 100000;
                        kvm[key_count].val = malloc(kvm[key_count].max_size * sizeof(int));
                        kvm[key_count].len = 1;
                        kvm[key_count].val[0] = kv_buffer[i].val;
                        key_count++;
                    }                    
                }
                free(kv_buffer);
            }
        }

        KeyValue *send_buffer = (KeyValue *) malloc(key_count * sizeof(KeyValue));
        for (int i = 0; i < key_count; i++)
        {
            send_buffer[i] = reduce(kvm[i].key, kvm[i].val, kvm[i].len);
        }

        free(kvm);

        MPI_Send(&key_count, 1, MPI_INT, 0, 11, MPI_COMM_WORLD);
        if (key_count)
        {
            MPI_Send(send_buffer, key_count, MPI_KeyValue, 0, 11, MPI_COMM_WORLD);
        }
        free(send_buffer);
    }
    

    //Clean up
    MPI_Finalize();
    return 0;
}
