/*
 * High-performance LSL consumer for benchmarking
 * Based on the dareplane Python consumer but implemented in C for higher throughput
 */

#include "include/lsl_c.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <math.h>
#include <getopt.h>
#include <signal.h>
#include <unistd.h>

// Configuration
static int n_messages = 1000;
static int msg_size = 1;
static char output_file[256] = "recv_times.csv";

// Timing storage
static double *recv_times = NULL;
static double *process_times = NULL;
static int receive_count = 0;

// Signal handling for clean shutdown
static volatile int keep_running = 1;

void handle_signal(int sig) {
    keep_running = 0;
    printf("\nCaught signal %d, shutting down...\n", sig);
}

// Get high-precision monotonic time in seconds
double get_monotonic_time() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec / 1e9;
}

void save_results(double first_timestamp) {
    if (receive_count < 2) {
        printf("Not enough samples to compute consumer statistics.\n");
        return;
    }

    printf("Total messages processed: %d\n", receive_count);

    // Jitter statistics
    double sum_diff = 0.0;
    double max_diff = 0.0;
    int max_idx = 0;
    double sum_sq_diff = 0.0;
    double *recv_times_diff = (double *)malloc((receive_count - 1) * sizeof(double));

    for (int i = 0; i < receive_count-1; i++) {
        double diff = recv_times[i+1] - recv_times[i];
        recv_times_diff[i] = diff;
        sum_diff += diff;
        sum_sq_diff += diff * diff;
        if (diff > max_diff) {
            max_diff = diff;
            max_idx = i;
        }
    }
    double avg_period_sec = sum_diff / (receive_count - 1);
    double avg_period = avg_period_sec * 1e6;  // us
    double variance = (sum_sq_diff / (receive_count - 1)) - (avg_period_sec * avg_period_sec);  // s²
    double std_period = sqrt(fmax(0.0, variance)) * 1e6;  // us

    printf("Average receive period (us): %.6f\n", avg_period);
    printf("Max receive period (us): %.6f, idx: %d\n", max_diff*1e6, max_idx);
    printf("Std receive period (us): %.6f\n", std_period);

    // Latency statistics
    double sum = 0.0;
    double max = 0.0;
    max_idx = 0;
    double sum_sq = 0.0;

    for (int i=0;i<receive_count;i++) {
        sum += process_times[i];
        sum_sq += process_times[i] * process_times[i];
        if (process_times[i] > max) {
            max = process_times[i];
            max_idx = i;
        }
    }
    double avg_latency_sec = sum / receive_count;
    double avg_latency = avg_latency_sec * 1e6;  // us
    double variance_latency = (sum_sq / receive_count) - (avg_latency_sec * avg_latency_sec);  // s²
    double std_latency = sqrt(fmax(0.0, variance_latency)) * 1e6;  // us

    double throughput = receive_count / (recv_times[receive_count-1] - first_timestamp)/1e3;
    printf("Throughput (msg/ms): %.2f\n", throughput);


    // Save to CSV
    FILE *fp = fopen(output_file, "w");
    if (fp == NULL) {
        fprintf(stderr, "Error opening file %s\n", output_file);
        return;
    }

    fprintf(fp, "Metric,Recv_period,Latency,Throughput\n");
    fprintf(fp, "mean,%.6f,%.6f,%.6f\n", avg_period, avg_latency, throughput);
    fprintf(fp, "std,%.6f,%.6f, \n", std_period, std_latency);
    fprintf(fp, "max,%.6f,%.6f, \n", max_diff*1e6, max*1e6);

    fclose(fp);
    free(recv_times_diff);
}

void parse_args(int argc, char *argv[]) {
    // Parse command-line arguments
    int opt;
    static const struct option long_options[] =
    {
        { "n_messages", required_argument, 0, 'n' },
        { "msg_size", required_argument, 0, 'm' },
        { "output_file", required_argument, 0, 'o' },
        0
    };
    while ((opt = getopt_long(argc, argv, "n:m:o:", long_options, NULL)) != -1) {
        switch (opt) {
            case 'n':
                n_messages = atoi(optarg);
                break;
            case 'm':
                msg_size = atoi(optarg);
                break;
            case 'o':
                strncpy(output_file, optarg, sizeof(output_file) - 1);
                output_file[sizeof(output_file) - 1] = '\0';
                break;
            default:
                fprintf(stderr, "Usage: %s [options]\n", argv[0]);
                fprintf(stderr, "  -n n_messages (default: 1000)\n");
                fprintf(stderr, "  -o output_file (default: recv_times.csv)\n");
                return;
        }
    }
}

int main(int argc, char *argv[]) {
    int exit_code = 0;
    lsl_inlet inlet = NULL;
    double *chunk_buffer = NULL;
    double *timestamps = NULL;
    double first_timestamp = 0.0;

    // Parse command line arguments
    parse_args(argc, argv);
    
    // Set up signal handlers
    signal(SIGINT, handle_signal);
    signal(SIGTERM, handle_signal);
    
    // Allocate timing storage
    recv_times = (double *)malloc(n_messages * sizeof(double));
    process_times = (double *)malloc(n_messages * sizeof(double));

    if (!recv_times || !process_times) {
        fprintf(stderr, "Memory allocation failed\n");
        exit_code = 1;
        goto cleanup;
    }
    
    // Find and connect to LSL stream
    printf("Waiting for LSL stream 'mock_random1'...\n");
    lsl_streaminfo infos[1];
    int n_found = 0;
    while (keep_running && n_found == 0) {
        n_found = lsl_resolve_byprop(infos, 1, "name", "mock_random1", 1, 1.0);
    }

    if (!keep_running) {
        exit_code = 130;
        goto cleanup;
    }

    if (n_found == 0) {
        fprintf(stderr, "Failed to find LSL stream\n");
        exit_code = 1;
        goto cleanup;
    }
    
    printf("Stream found. Creating inlet...\n");
    
    int num_channels = lsl_get_channel_count(infos[0]);
    printf("Nominal sample rate inlet: %f\n", lsl_get_nominal_srate(infos[0]));
    printf("Stream has %d channel(s)\n", num_channels);
    
    inlet = lsl_create_inlet(infos[0], (int)(1000 * msg_size), msg_size, 1);

    if (!inlet) {
        fprintf(stderr, "Failed to create inlet\n");
        exit_code = 1;
        goto cleanup;
    }
    
    // Open the stream
    lsl_open_stream(inlet, LSL_FOREVER, NULL);

    printf("Stream opened. Starting data reception...\n");
    
    // Allocate buffer for receiving chunks of msg_size samples * num_channels values each
    int chunk_size = msg_size * num_channels;
    chunk_buffer = (double *)malloc(chunk_size * sizeof(double));
    timestamps = (double *)malloc(msg_size * sizeof(double));
    if (!chunk_buffer || !timestamps) {
        fprintf(stderr, "Memory allocation failed\n");
        exit_code = 1;
        goto cleanup;
    }
    
    // Main reception loop
    int errcode;
    
    // Warmup - pull 500 chunks
    while (keep_running && receive_count < 500) {
        unsigned long pulled = lsl_pull_chunk_d(inlet, chunk_buffer, timestamps, chunk_size, msg_size, 0.1, &errcode);
        if (errcode != 0) {
            fprintf(stderr, "Error receiving message during warmup\n");
            exit_code = 1;
            goto cleanup;
        }
        if (pulled == 0) {
            continue;
        }
        receive_count++;
    }

    if (!keep_running) {
        exit_code = 130;
        goto cleanup;
    }

    receive_count = 0;  // reset count for actual measurement

    while (keep_running && receive_count < n_messages) {
        unsigned long pulled = lsl_pull_chunk_d(inlet, chunk_buffer, timestamps, chunk_size, msg_size, 0.1, &errcode);
        if (errcode != 0) {
            fprintf(stderr, "Error receiving message\n");
            exit_code = 1;
            goto cleanup;
        }
        if (pulled == 0) {
            continue;
        }

        double now = get_monotonic_time();
        
        // Extract timestamp from first channel of first sample (last sample would be 29)
        double msg_timestamp = chunk_buffer[0];
        if (receive_count == 0) {
            first_timestamp = msg_timestamp;
        }

        recv_times[receive_count] = now;
        process_times[receive_count] = now - msg_timestamp;

        receive_count++;
    }

    if (!keep_running) {
        exit_code = 130;
    }

cleanup:
    // Cleanup
    if (inlet) {
        lsl_close_stream(inlet);
        lsl_destroy_inlet(inlet);
    }
    free(chunk_buffer);
    free(timestamps);

    // Save results
    if (receive_count >= 2) {
        save_results(first_timestamp);
    }
    free(recv_times);
    free(process_times);

    printf("Consumer stopped.\n");
    return exit_code;
}
