/*
 * High-performance LSL producer for benchmarking
 * Based on the dareplane Python producer but implemented in C for higher throughput
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
static int n_channels = 1;
static int msg_size = 1;
static char output_file[256] = "send_times.csv";

// Timing storage
static double *send_times = NULL;

// Signal handling for clean shutdown
static volatile sig_atomic_t keep_running = 1;

void handle_signal(int sig) {
    (void)sig;
    keep_running = 0;
}

// Get high-precision monotonic time in seconds
double get_monotonic_time() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec / 1e9;
}

void save_results() {
    if (n_messages < 2) {
        printf("Not enough samples to compute producer statistics.\n");
        return;
    }

    printf("Total messages sent: %d\n", n_messages);

    // Calculate statistics (skip first measurement)
    double sum_diff = 0.0;
    double max_diff = 0.0;
    int max_idx = 0;
    double sum_sq_diff = 0.0;
    double *send_times_diff = (double *)malloc((n_messages - 1) * sizeof(double));

    for (int i = 0; i < n_messages-1; i++) {
        double diff = send_times[i+1] - send_times[i];
        send_times_diff[i] = diff;
        sum_diff += diff;
        sum_sq_diff += diff * diff;
        if (diff > max_diff) {
            max_diff = diff;
            max_idx = i;
        }
    }

    double avg_period_sec = sum_diff / (n_messages - 1);
    double avg_period = avg_period_sec * 1e6;  // us
    double variance = (sum_sq_diff / (n_messages - 1)) - (avg_period_sec * avg_period_sec);  // s²
    double std_period = sqrt(fmax(0.0, variance)) * 1e6;  // us

    printf("Average send period (us): %.6f\n", avg_period);
    printf("Max send period (us): %.6f, idx: %d\n", max_diff * 1e6, max_idx);
    printf("Std send period (us): %.6f\n", std_period);

    // Save to CSV
    FILE *fp = fopen(output_file, "w");
    if (fp == NULL) {
        fprintf(stderr, "Error opening file %s\n", output_file);
        return;
    }

    fprintf(fp, "Metric,Send_period\n");
    fprintf(fp, "mean,%.6f\n", avg_period);
    fprintf(fp, "std,%.6f\n", std_period);
    fprintf(fp, "max,%.6f\n", max_diff * 1e6);

    fclose(fp);
    free(send_times_diff);
}

void parse_args(int argc, char *argv[]) {
    // Parse command-line arguments
    int opt;
    static const struct option long_options[] = {
        { "n_messages", required_argument, 0, 'n' },
        { "msg_size", required_argument, 0, 'm' },
        { "num_channels", required_argument, 0, 'c' },
        { "output_file", required_argument, 0, 'o' },
        0
    };
    while ((opt = getopt_long(argc, argv, "n:m:c:o:", long_options, NULL)) != -1) {
        switch (opt) {
            case 'n':
                n_messages = atoi(optarg);
                break;
            case 'm':
                msg_size = atoi(optarg);
                break;
            case 'c':
                n_channels = atoi(optarg);
                break;
            case 'o':
                strncpy(output_file, optarg, sizeof(output_file) - 1);
                output_file[sizeof(output_file) - 1] = '\0';
                break;
            default:
                fprintf(stderr, "Usage: %s [options]\n", argv[0]);
                fprintf(stderr, "  -n n_messages (default: 1000)\n");
                fprintf(stderr, "  -m msg_size (default: 1)\n");
                fprintf(stderr, "  -c num_channels (default: 1)\n");
                fprintf(stderr, "  -o output_file (default: send_times.csv)\n");
                return;
        }
    }
}

int main(int argc, char *argv[]) {
    int exit_code = 0;
    lsl_streaminfo info = NULL;
    lsl_outlet outlet = NULL;
    double *chunk_data = NULL;

    // Parse command line arguments
    parse_args(argc, argv);
    
    // Set up signal handlers
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = handle_signal;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);
    
    // Allocate timing storage
    send_times = (double *)malloc(n_messages * sizeof(double));
    if (!send_times) {
        fprintf(stderr, "Memory allocation failed\n");
        return 1;
    }
    
    // Create LSL outlet
    printf("Creating LSL outlet...\n");
    info = lsl_create_streaminfo(
        "mock_random1",           // stream name (matches consumer config)
        "EEG",                    // stream type
        n_channels,               // number of channels
        LSL_IRREGULAR_RATE,       // nominal sample rate
        cft_double64,             // channel format
        "c_producer_uid"          // unique source ID
    );
    if (!info) {
        fprintf(stderr, "Failed to create LSL streaminfo\n");
        exit_code = 1;
        goto cleanup;
    }

    outlet = lsl_create_outlet(info, msg_size, (int)(ceil(500 + n_messages * msg_size)));
    if (!outlet) {
        fprintf(stderr, "Failed to create LSL outlet\n");
        exit_code = 1;
        goto cleanup;
    }
    printf("LSL outlet created. Waiting for consumer...\n");
    
    // Wait for at least one consumer to connect
    while (keep_running && !lsl_wait_for_consumers(outlet, 1.0)) {
        // Poll in short intervals so SIGINT can stop the wait.
        fprintf(stderr, "Waiting for consumer to connect...\n");
    }
    if (!keep_running) {
        exit_code = 130;
        goto cleanup;
    }
    
    printf("Consumer connected. Starting data transmission...\n");
    
    // Calculate total data size for msg_size samples with n_channels each
    int chunk_size = msg_size * n_channels;
    chunk_data = (double *)malloc(chunk_size * sizeof(double));
    double now;
    if (!chunk_data) {
        fprintf(stderr, "Memory allocation failed\n");
        exit_code = 1;
        goto cleanup;
    }
    
    // Warmup
    for (int i = 0; i < 500 && keep_running; i++) {
        now = get_monotonic_time();
        // Fill chunk with current timestamp
        for (int j = 0; j < chunk_size; j++) {
            chunk_data[j] = now;
        }
        lsl_push_chunk_d(outlet, chunk_data, chunk_size);
        // fprintf(stderr, "Warmup message %d sent\n", i+1);
    }
    if (keep_running) {
        usleep(10000);
    }

    // Main transmission loop
    int sent_count = 0;
    for (int i = 0; i < n_messages && keep_running; i++) {
        now = get_monotonic_time();
        // Fill chunk with current timestamp
        for (int j = 0; j < chunk_size; j++) {
            chunk_data[j] = now;
        }
        lsl_push_chunk_d(outlet, chunk_data, chunk_size);
        send_times[i] = now;
        sent_count++;
    }

    n_messages = sent_count;

    while (keep_running && lsl_have_consumers(outlet)) {
        fprintf(stderr,"Waiting to be closed...");
        sleep(1);
    }
    goto cleanup;

cleanup:
    // Cleanup
    if (outlet) {
        lsl_destroy_outlet(outlet);
    }
    free(chunk_data);

    // Save results
    if (n_messages >= 2) {
        save_results();
    }
    free(send_times);

    printf("Producer stopped.\n");
    return exit_code;
}
