/*
 * High-performance LSL consumer for benchmarking
 * Based on the dareplane Python consumer but implemented in C for higher throughput
 */

#include "include/lsl_c.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <signal.h>
#include <unistd.h>

// Configuration
static int n_messages = 1000;
static int n_channels = 1;
static int msg_size = 1;
static char output_file[256] = "recv_times.csv";

// Timing storage
static double *recv_times = NULL;
static double *latencies = NULL;
static int recv_count = 0;

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

void save_results() {
    FILE *fp = fopen(output_file, "w");
    if (!fp) {
        fprintf(stderr, "Error opening output file: %s\n", output_file);
        return;
    }
    
    fprintf(fp, "recv_time,latency,recv_period\n");
    for (int i = 0; i < recv_count; i++) {
        double period = (i < recv_count - 1) ? (recv_times[i+1] - recv_times[i]) : 0.0;
        fprintf(fp, "%.9f,%.9f,%.9f\n", recv_times[i], latencies[i], period);
    }
    fclose(fp);
    
    // Print statistics (excluding first sample)
    if (recv_count > 1) {
        double sum_lat = 0.0, sum_lat_sq = 0.0, max_lat = 0.0;
        double sum_period = 0.0, sum_period_sq = 0.0, max_period = 0.0;
        int max_lat_idx = 0, max_period_idx = 0;
        
        for (int i = 1; i < recv_count; i++) {
            double lat = latencies[i];
            sum_lat += lat;
            sum_lat_sq += lat * lat;
            if (lat > max_lat) {
                max_lat = lat;
                max_lat_idx = i;
            }
        }
        
        for (int i = 1; i < recv_count - 1; i++) {
            double period = recv_times[i+1] - recv_times[i];
            sum_period += period;
            sum_period_sq += period * period;
            if (period > max_period) {
                max_period = period;
                max_period_idx = i;
            }
        }
        
        int n_lat = recv_count - 1;
        int n_period = recv_count - 2;
        
        double mean_lat = sum_lat / n_lat;
        double var_lat = (sum_lat_sq / n_lat) - (mean_lat * mean_lat);
        double std_lat = sqrt(var_lat);
        
        double mean_period = sum_period / n_period;
        double var_period = (sum_period_sq / n_period) - (mean_period * mean_period);
        double std_period = sqrt(var_period);
        
        printf("\n=== Consumer Statistics ===\n");
        printf("Total messages processed: %d\n", recv_count);
        printf("\nLatency:\n");
        printf("  Average (s): %.6f (%.3f ms)\n", mean_lat, mean_lat * 1000);
        printf("  Max (s): %.6f (%.3f ms), idx: %d\n", max_lat, max_lat * 1000, max_lat_idx);
        printf("  Std (s): %.6f (%.3f ms)\n", std_lat, std_lat * 1000);
        printf("\nReceive period:\n");
        printf("  Average (s): %.6f\n", mean_period);
        printf("  Max (s): %.6f, idx: %d\n", max_period, max_period_idx);
        printf("  Std (s): %.6f\n", std_period);
    }
}

void parse_args(int argc, char *argv[]) {
    for (int i = 1; i < argc; i++) {
        if (strncmp(argv[i], "--n_messages=", 13) == 0) {
            n_messages = atoi(argv[i] + 13);
        } else if (strncmp(argv[i], "--n_channels=", 13) == 0) {
            n_channels = atoi(argv[i] + 13);
        } else if (strncmp(argv[i], "--msg_size=", 11) == 0) {
            msg_size = atoi(argv[i] + 11);
        } else if (strncmp(argv[i], "--output_file=", 14) == 0) {
            strncpy(output_file, argv[i] + 14, sizeof(output_file) - 1);
        }
    }
    
    printf("Configuration:\n");
    printf("  n_messages: %d\n", n_messages);
    printf("  n_channels: %d\n", n_channels);
    printf("  msg_size: %d\n", msg_size);
    printf("  output_file: %s\n", output_file);
}

int main(int argc, char *argv[]) {
    // Parse command line arguments
    parse_args(argc, argv);
    
    // Set up signal handlers
    signal(SIGINT, handle_signal);
    signal(SIGTERM, handle_signal);
    
    // Allocate timing storage
    recv_times = (double *)malloc(n_messages * sizeof(double));
    latencies = (double *)malloc(n_messages * sizeof(double));
    if (!recv_times || !latencies) {
        fprintf(stderr, "Failed to allocate memory for timing arrays\n");
        return EXIT_FAILURE;
    }
    
    // Find and connect to LSL stream
    printf("Waiting for LSL stream 'mock_random1'...\n");
    lsl_streaminfo infos[1];
    int n_found = lsl_resolve_byprop(infos, 1, "name", "mock_random1", 1, LSL_FOREVER);
    
    if (n_found == 0) {
        fprintf(stderr, "Failed to find LSL stream\n");
        free(recv_times);
        free(latencies);
        return EXIT_FAILURE;
    }
    
    printf("Stream found. Creating inlet...\n");
    lsl_inlet inlet = lsl_create_inlet(infos[0], 360, LSL_NO_PREFERENCE, 1);
    
    // Open the stream
    lsl_open_stream(inlet, LSL_FOREVER, NULL);
    printf("Stream opened. Starting data reception...\n");
    
    // Allocate data buffer
    double *data = (double *)malloc(n_channels * sizeof(double));
    if (!data) {
        fprintf(stderr, "Failed to allocate memory for data\n");
        lsl_destroy_inlet(inlet);
        free(recv_times);
        free(latencies);
        return EXIT_FAILURE;
    }
    
    // Main reception loop
    double timestamp;
    int errcode;
    
    while (recv_count < n_messages && keep_running) {
        // Pull sample (blocking with 1 second timeout)
        timestamp = lsl_pull_sample_d(inlet, data, n_channels, 1.0, &errcode);
        
        if (errcode == 0 && timestamp > 0) {
            // Get receive time
            double now = get_monotonic_time();
            
            // Calculate latency (send timestamp is in data[0])
            double send_time = data[0];
            double latency = now - send_time;
            
            // Store timing data
            recv_times[recv_count] = now;
            latencies[recv_count] = latency;
            recv_count++;
            
            // Progress indicator
            // if (recv_count % 100 == 0) {
            //     printf("Received %d/%d messages\r", recv_count, n_messages);
            //     fflush(stdout);
            // }
        } else if (errcode != 0) {
            fprintf(stderr, "\nError pulling sample: %d\n", errcode);
        }
    }
    
    printf("\nReception complete.\n");
    
    // Cleanup
    free(data);
    lsl_close_stream(inlet);
    lsl_destroy_inlet(inlet);
    
    // Save results
    save_results();
    free(recv_times);
    free(latencies);
    
    printf("Consumer finished.\n");
    return EXIT_SUCCESS;
}
